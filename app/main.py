from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path
import re
from typing import Dict, Optional

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from app.excel_parsers import PreparedInput, ROLE_GROUP_CHOICES, prepare_input
from app.logging_utils import (
    APP_LOG_FILE,
    ERROR_LOG_DIR,
    setup_logging,
    write_exception_log,
)
from app.scheduler import (
    ScheduleGenerationError,
    ScheduleResult,
    build_preview_rows_t13_aligned,
    export_schedule_to_excel,
    export_t13_to_excel,
    generate_schedule,
)

BASE_DIR = Path(__file__).resolve().parent.parent
STATIC_DIR = BASE_DIR / "static"
OUTPUT_DIR = BASE_DIR / "outputs"
VERSION_FILE = BASE_DIR / "VERSION"
APP_VERSION = VERSION_FILE.read_text(encoding="utf-8").strip() if VERSION_FILE.exists() else "0.1.0"

setup_logging(APP_VERSION)
logger = logging.getLogger("tabel.app")


@dataclass
class RuntimeState:
    prepared: Optional[PreparedInput] = None
    generated: Optional[ScheduleResult] = None
    output_path: Optional[Path] = None
    t13_output_path: Optional[Path] = None


class GenerateRequest(BaseModel):
    role_group_overrides: Optional[Dict[str, str]] = None


app = FastAPI(
    title="Генератор графиков",
    description="FastAPI сервис для генерации рабочих графиков из Excel.",
    version=APP_VERSION,
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.state.runtime = RuntimeState()
PREVIEW_PAGE_SIZE = 150


def _internal_error_detail(context: str, exc: Exception) -> str:
    error_id, error_file = write_exception_log(context=context, exc=exc)
    logger.exception("%s | error_id=%s | file=%s", context, error_id, error_file)
    return f"{context}. Код ошибки: {error_id}. Лог: {error_file}"


def _resolve_t13_template_bytes() -> tuple[Optional[bytes], Optional[str], Optional[str]]:
    """
    Возвращает (bytes, source_path, warning). warning заполнен, если шаблон найден,
    но не может быть использован (например .xls вместо .xlsx).
    """
    candidates = [
        BASE_DIR / "templates" / "t13_template.xlsx",
    ]

    for path in candidates:
        if not path.exists() or not path.is_file():
            continue
        suffix = path.suffix.lower()
        if suffix == ".xlsx":
            try:
                return path.read_bytes(), str(path), None
            except Exception as exc:
                return None, None, f"Не удалось прочитать шаблон Т-13 '{path}': {exc}"
        if suffix == ".xls":
            return (
                None,
                None,
                (
                    f"Шаблон '{path.name}' в формате .xls. "
                    "Для точного сохранения формы нужен .xlsx-шаблон."
                ),
            )

    return None, None, None


@app.get("/version")
def version() -> dict:
    return {"version": APP_VERSION}


@app.get("/", response_class=HTMLResponse)
def root() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.post("/upload")
async def upload_files(
    payroll_file: UploadFile = File(...),
    employees_file: Optional[UploadFile] = File(None),
) -> dict:
    if not payroll_file.filename:
        raise HTTPException(status_code=400, detail="Обязателен файл расчетных листков.")

    try:
        payroll_bytes = await payroll_file.read()
        employees_bytes = await employees_file.read() if employees_file and employees_file.filename else None

        prepared = prepare_input(
            payroll_bytes=payroll_bytes,
            payroll_filename=payroll_file.filename,
            employees_bytes=employees_bytes,
        )
    except ValueError as exc:
        logger.warning("Ошибка пользовательских данных при загрузке: %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=_internal_error_detail("Ошибка разбора Excel", exc)) from exc

    app.state.runtime = RuntimeState(prepared=prepared)

    return {
        "message": "Файлы успешно загружены и обработаны.",
        "files": {
            "payroll": payroll_file.filename,
            "employees": employees_file.filename if employees_file and employees_file.filename else None,
        },
        "summary": prepared.summary,
        "role_group_defaults": prepared.role_group_defaults,
        "warnings": prepared.warnings,
    }


@app.post("/generate")
def generate(payload: Optional[GenerateRequest] = None) -> dict:
    runtime: RuntimeState = app.state.runtime

    if runtime.prepared is None:
        raise HTTPException(status_code=400, detail="Сначала загрузите файлы через /upload.")

    role_group_overrides = payload.role_group_overrides if payload else None
    employees_df = runtime.prepared.employees.copy()
    if role_group_overrides:
        normalized_overrides: Dict[str, str] = {}
        for role_original, group_name in role_group_overrides.items():
            role_key = str(role_original or "").strip().lower()
            group_value = str(group_name or "").strip()
            if group_value == "Зал/Касса":
                group_value = "Касса"
            if not role_key:
                continue
            if group_value not in ROLE_GROUP_CHOICES:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Недопустимая группа '{group_value}' для роли '{role_original}'. "
                        f"Разрешены: {', '.join(ROLE_GROUP_CHOICES)}."
                    ),
                )
            normalized_overrides[role_key] = group_value

        if normalized_overrides:
            employees_df["role_group"] = employees_df.apply(
                lambda row: normalized_overrides.get(str(row["role_original"]).strip().lower(), row["role_group"]),
                axis=1,
            )

    try:
        result = generate_schedule(
            employees_df=employees_df,
            days=runtime.prepared.days,
            weekend_days=set(runtime.prepared.weekend_days),
        )
    except ScheduleGenerationError as exc:
        logger.warning("Ошибка генерации (валидация): %s", exc)
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=_internal_error_detail("Ошибка генерации графика", exc)) from exc

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = OUTPUT_DIR / f"schedule_{timestamp}.xlsx"
    t13_output_path = OUTPUT_DIR / f"timesheet_t13_{timestamp}.xlsx"

    try:
        template_bytes, template_source, template_warning = _resolve_t13_template_bytes()
        export_schedule_to_excel(result, output_path)
        export_t13_to_excel(
            result=result,
            days=runtime.prepared.days,
            output_path=t13_output_path,
            template_bytes=template_bytes,
            weekend_days=set(runtime.prepared.weekend_days),
            period_year=runtime.prepared.period_year,
            period_month=runtime.prepared.period_month,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=_internal_error_detail("Ошибка сохранения результата", exc)) from exc

    app.state.runtime = RuntimeState(
        prepared=runtime.prepared,
        generated=result,
        output_path=output_path,
        t13_output_path=t13_output_path,
    )

    violations = result.employee_summary[(~result.employee_summary["hours_ok"]) | (~result.employee_summary["days_ok"])]
    all_warnings = runtime.prepared.warnings + result.warnings
    if template_source:
        all_warnings.append(f"Т-13 сформирован по шаблону: {template_source}")
    if template_warning:
        all_warnings.append(template_warning)
    deficit_count = int(result.assignments["deficit"].sum()) if "deficit" in result.assignments.columns else 0
    cross_restaurant_count = (
        int(result.assignments["cross_restaurant"].sum())
        if "cross_restaurant" in result.assignments.columns
        else 0
    )

    preview_all_df = build_preview_rows_t13_aligned(
        result=result,
        days=runtime.prepared.days,
        weekend_days=set(runtime.prepared.weekend_days),
    )
    preview_df = preview_all_df.head(PREVIEW_PAGE_SIZE)
    payload = {
        "message": "График успешно сгенерирован.",
        "assignments_count": int(len(result.assignments)),
        "days_count": int(len(runtime.prepared.days)),
        "employees_count": int(len(runtime.prepared.employees)),
        "warnings": all_warnings,
        "violations_count": int(len(violations)),
        "deficit_count": deficit_count,
        "cross_restaurant_count": cross_restaurant_count,
        "download_filename": output_path.name,
        "t13_download_filename": t13_output_path.name,
        "preview": preview_df.to_dict(orient="records"),
        "preview_total": int(len(preview_all_df)),
        "preview_page_size": PREVIEW_PAGE_SIZE,
        "preview_next_offset": int(len(preview_df)),
    }
    return jsonable_encoder(payload)


@app.get("/preview")
def preview(
    offset: int = Query(0, ge=0),
    limit: int = Query(PREVIEW_PAGE_SIZE, ge=1, le=1000),
) -> dict:
    runtime: RuntimeState = app.state.runtime
    if runtime.generated is None:
        raise HTTPException(status_code=400, detail="Сначала выполните /generate.")

    assignments = runtime.generated.assignments
    if runtime.prepared is None:
        raise HTTPException(status_code=400, detail="Сначала выполните /upload и /generate.")
    preview_all_df = build_preview_rows_t13_aligned(
        result=runtime.generated,
        days=runtime.prepared.days,
        weekend_days=set(runtime.prepared.weekend_days),
    )
    total = int(len(preview_all_df))
    if offset >= total:
        return {"rows": [], "total": total, "next_offset": offset}

    chunk = preview_all_df.iloc[offset : offset + limit]
    next_offset = min(offset + len(chunk), total)
    return {
        "rows": chunk.to_dict(orient="records"),
        "total": total,
        "next_offset": int(next_offset),
    }


@app.get("/download")
def download() -> FileResponse:
    runtime: RuntimeState = app.state.runtime

    if runtime.output_path is None or not runtime.output_path.exists():
        raise HTTPException(status_code=404, detail="Файл результата не найден. Сначала выполните /generate.")

    return FileResponse(
        path=runtime.output_path,
        filename=runtime.output_path.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.get("/download_t13")
def download_t13() -> FileResponse:
    runtime: RuntimeState = app.state.runtime

    if runtime.t13_output_path is None or not runtime.t13_output_path.exists():
        raise HTTPException(status_code=404, detail="Файл Т-13 не найден. Сначала выполните /generate.")

    return FileResponse(
        path=runtime.t13_output_path,
        filename=runtime.t13_output_path.name,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.get("/download_log")
def download_log() -> FileResponse:
    if not APP_LOG_FILE.exists():
        raise HTTPException(status_code=404, detail="Файл app.log пока не создан.")

    return FileResponse(
        path=APP_LOG_FILE,
        filename=APP_LOG_FILE.name,
        media_type="text/plain",
    )


@app.get("/download_error_log/{error_id}")
def download_error_log(error_id: str) -> FileResponse:
    if not re.fullmatch(r"[A-Za-z0-9_-]+", error_id):
        raise HTTPException(status_code=400, detail="Некорректный идентификатор ошибки.")

    error_file = ERROR_LOG_DIR / f"{error_id}.log"
    if not error_file.exists():
        raise HTTPException(status_code=404, detail="Лог ошибки не найден.")

    return FileResponse(
        path=error_file,
        filename=error_file.name,
        media_type="text/plain",
    )
