from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from collections import defaultdict
from typing import Any, Optional
import math
import re

import pandas as pd


@dataclass
class PreparedInput:
    employees: pd.DataFrame
    days: list[int]
    weekend_days: list[int]
    warnings: list[str]
    summary: dict[str, Any]


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    text = str(value).strip().lower().replace("ё", "е")
    text = re.sub(r"\s+", " ", text)
    return text


def _normalize_for_search(value: Any) -> str:
    text = _normalize_text(value)
    return re.sub(r"[^a-zа-я0-9]+", "", text)


def _extract_number(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(" ", "").replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group())
    except ValueError:
        return None


def _clean_employee_name(value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    # Удаляем код сотрудника в скобках: "Иванов Иван (00123)" -> "Иванов Иван"
    text = re.sub(r"\s*\(\s*\d+\s*\)\s*$", "", text).strip()
    return text


def _pick_text_or_default(values: pd.Series, default: str) -> str:
    for value in values:
        text = _normalize_text(value)
        if text and text != default:
            return text
    return default


def _find_value_near_label(row_values: list[Any], label_tokens: tuple[str, ...]) -> str | None:
    normalized = [_normalize_text(v) for v in row_values]
    for idx, cell in enumerate(normalized):
        if not cell:
            continue
        if not any(token in cell for token in label_tokens):
            continue

        for j in range(idx + 1, len(row_values)):
            candidate = _normalize_text(row_values[j])
            if not candidate:
                continue
            if any(token in candidate for token in label_tokens):
                continue
            return candidate

    return None


def _map_role_group(value: Any) -> str:
    role = _normalize_text(value)
    if not role:
        return "не указана"

    admin_tokens = (
        "администратор",
        "адмистратор",
        "кассир",
        "менеджер",
        "управляющ",
    )
    kitchen_tokens = (
        "повар",
        "шеф",
        "су-шеф",
        "сушеф",
        "су шеф",
        "шеф-повар",
        "шеф повар",
    )
    bar_tokens = (
        "бармен",
        "бар менеджер",
        "бар-менеджер",
    )
    hall_tokens = (
        "официант",
        "хостес",
        "раннер",
        "ранер",
    )

    if any(token in role for token in admin_tokens):
        return "админ/касса/управление"

    if any(token in role for token in kitchen_tokens):
        return "кухня (повар/шеф/су-шеф)"

    if any(token in role for token in bar_tokens):
        return "бар (бармен/бар-менеджер)"

    if any(token in role for token in hall_tokens):
        return "зал (официант/хостес/раннер)"

    return role


def _find_columns(columns: list[Any], patterns: dict[str, list[str]]) -> dict[str, str]:
    normalized = {str(col): _normalize_for_search(col) for col in columns}
    result: dict[str, str] = {}

    for field, field_patterns in patterns.items():
        best_col = None
        best_score = 0
        for col, normalized_col in normalized.items():
            score = sum(1 for p in field_patterns if p in normalized_col)
            if score > best_score:
                best_score = score
                best_col = col
        if best_col is not None and best_score > 0:
            result[field] = best_col

    return result


def _detect_header_row(raw_df: pd.DataFrame, all_tokens: list[str], max_scan_rows: int = 25) -> int | None:
    max_rows = min(len(raw_df), max_scan_rows)
    best_score = -1
    best_index: int | None = None

    for idx in range(max_rows):
        row_values = raw_df.iloc[idx].tolist()
        normalized_cells = [_normalize_for_search(v) for v in row_values if _normalize_for_search(v)]
        if not normalized_cells:
            continue

        score = 0
        for token in all_tokens:
            if any(token in cell for cell in normalized_cells):
                score += 1

        if score > best_score:
            best_score = score
            best_index = idx

    if best_score < max(2, len(set(all_tokens)) // 3):
        return None

    return best_index


def _find_sheet_with_columns(file_bytes: bytes, patterns: dict[str, list[str]]) -> pd.DataFrame:
    workbook = pd.read_excel(BytesIO(file_bytes), sheet_name=None, dtype=object)

    for _, df in workbook.items():
        df = df.copy()
        df.columns = [str(c).strip() for c in df.columns]
        mapping = _find_columns(list(df.columns), patterns)
        if all(field in mapping for field in patterns):
            return df

    all_tokens = []
    for token_list in patterns.values():
        all_tokens.extend(token_list)

    for _, raw_df in workbook.items():
        raw_df = raw_df.copy()
        header_row = _detect_header_row(raw_df, all_tokens)
        if header_row is None:
            continue

        raw_df.columns = [str(c).strip() for c in raw_df.iloc[header_row].tolist()]
        candidate = raw_df.iloc[header_row + 1 :].copy()
        candidate = candidate.dropna(axis=0, how="all")

        mapping = _find_columns(list(candidate.columns), patterns)
        if all(field in mapping for field in patterns):
            return candidate

    fields = ", ".join(patterns.keys())
    raise ValueError(f"Не удалось найти необходимые колонки: {fields}")


def parse_employee_list(file_bytes: bytes) -> pd.DataFrame:
    patterns = {
        "employee": ["сотрудник", "фио", "работник", "employee", "name"],
        "restaurant": [
            "ресторан",
            "restaurant",
            "точка",
            "филиал",
            "подразделение",
            "названиеподразделения",
        ],
        "role": ["должност", "роль", "позиц", "role", "post"],
    }
    df = _find_sheet_with_columns(file_bytes, patterns)
    mapping = _find_columns(list(df.columns), patterns)

    result = df[[mapping["employee"], mapping["restaurant"], mapping["role"]]].copy()
    result.columns = ["employee", "restaurant", "role"]

    result["employee"] = result["employee"].map(_clean_employee_name)
    result["restaurant"] = result["restaurant"].map(_normalize_text)
    result["role"] = result["role"].map(_normalize_text)

    result = result[result["employee"] != ""]
    result = result.drop_duplicates(subset=["employee"], keep="first")

    result["restaurant"] = result["restaurant"].replace("", "не указан")
    result["role"] = result["role"].replace("", "не указана")

    return result.reset_index(drop=True)


def _parse_payroll_blocks(file_bytes: bytes) -> pd.DataFrame:
    workbook = pd.read_excel(BytesIO(file_bytes), sheet_name=None, header=None, dtype=object)
    rows: list[dict[str, Any]] = []

    for _, df in workbook.items():
        if df.empty:
            continue

        current_employee = ""
        current_restaurant = "не указан"
        current_role = "не указана"
        block_days: list[float] = []
        block_hours: list[float] = []

        def flush_block() -> None:
            nonlocal block_days, block_hours, current_employee, current_restaurant, current_role
            if not current_employee:
                block_days = []
                block_hours = []
                return

            max_days = max(block_days) if block_days else 0.0
            max_hours = max(block_hours) if block_hours else 0.0

            if max_days <= 0 and max_hours > 0:
                max_days = math.ceil(max_hours / 8.0)
            if max_hours <= 0 and max_days > 0:
                max_hours = max_days * 8.0

            if max_days > 0 and max_hours > 0:
                rows.append(
                    {
                        "employee": current_employee,
                        "max_hours": float(max_hours),
                        "max_days": int(round(max_days)),
                        "restaurant": _normalize_text(current_restaurant) or "не указан",
                        "role": _normalize_text(current_role) or "не указана",
                    }
                )

            block_days = []
            block_hours = []

        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            col0 = row.iloc[0] if len(row) > 0 else None
            col0_text = _normalize_text(col0)

            # Начало нового блока сотрудника: "ФИО (00123)"
            if col0_text and re.search(r"\(\s*\d+\s*\)\s*$", col0_text):
                flush_block()
                current_employee = _clean_employee_name(col0_text)
                current_restaurant = "не указан"
                current_role = "не указана"
                continue

            if not current_employee:
                continue

            row_values = row.tolist()
            detected_restaurant = _find_value_near_label(row_values, ("подразделен", "ресторан", "филиал"))
            if detected_restaurant:
                current_restaurant = detected_restaurant

            detected_role = _find_value_near_label(row_values, ("должност",))
            if detected_role:
                current_role = detected_role

            # В структуре расчетного листка дни/часы чаще всего в колонках 10/12
            days_val = _extract_number(row.iloc[10] if len(row) > 10 else None)
            hours_val = _extract_number(row.iloc[12] if len(row) > 12 else None)

            if days_val is None and hours_val is None:
                continue

            is_relevant_line = any(
                token in col0_text
                for token in (
                    "оплата",
                    "оклад",
                    "тариф",
                    "час",
                    "смен",
                    "норма",
                )
            )
            # Иногда col0 может быть пустым — тогда берём любые адекватные численные значения
            if not is_relevant_line and col0_text:
                continue

            if days_val is not None and 0 < days_val <= 31:
                block_days.append(days_val)
            if hours_val is not None and 0 < hours_val <= 400:
                block_hours.append(hours_val)

        flush_block()

    if not rows:
        raise ValueError("Не удалось извлечь часы/дни из расчетных листков.")

    result = pd.DataFrame(rows)
    result = (
        result.groupby("employee", as_index=False)
        .agg(
            max_hours=("max_hours", "max"),
            max_days=("max_days", "max"),
            restaurant=("restaurant", lambda s: _pick_text_or_default(s, "не указан")),
            role=("role", lambda s: _pick_text_or_default(s, "не указана")),
        )
        .reset_index(drop=True)
    )
    result["max_days"] = result["max_days"].round().astype(int)
    return result


def parse_payroll(file_bytes: bytes) -> pd.DataFrame:
    # Для расчетных листков 1С (блочный формат по сотрудникам) сначала пробуем профильный парсер.
    try:
        block_result = _parse_payroll_blocks(file_bytes)
        has_org_data = ((block_result["restaurant"] != "не указан") | (block_result["role"] != "не указана")).any()
        if has_org_data or len(block_result) >= 20:
            return block_result
    except Exception:
        pass

    patterns = {
        "employee": ["сотрудник", "фио", "работник", "employee", "name"],
        "hours": ["час", "hours", "норма", "планчас"],
        "days": ["дн", "дней", "days", "смен"],
    }
    try:
        df = _find_sheet_with_columns(file_bytes, patterns)
        mapping = _find_columns(list(df.columns), patterns)
    except ValueError:
        return _parse_payroll_blocks(file_bytes)

    optional_patterns = {
        "restaurant": [
            "ресторан",
            "restaurant",
            "точка",
            "филиал",
            "подразделение",
            "названиеподразделения",
        ],
        "role": ["должност", "роль", "позиц", "role", "post"],
    }
    optional_mapping = _find_columns(list(df.columns), optional_patterns)

    result = df[[mapping["employee"], mapping["hours"], mapping["days"]]].copy()
    result.columns = ["employee", "max_hours", "max_days"]

    result["employee"] = result["employee"].map(_clean_employee_name)
    result = result[result["employee"] != ""]

    if "restaurant" in optional_mapping:
        result["restaurant"] = df[optional_mapping["restaurant"]].map(_normalize_text)
    else:
        result["restaurant"] = "не указан"

    if "role" in optional_mapping:
        result["role"] = df[optional_mapping["role"]].map(_normalize_text)
    else:
        result["role"] = "не указана"

    result["max_hours"] = result["max_hours"].map(_extract_number)
    result["max_days"] = result["max_days"].map(_extract_number)

    result["max_hours"] = result["max_hours"].fillna(0.0)
    result["max_days"] = result["max_days"].fillna(0.0)

    inferred_days = result["max_hours"].map(lambda h: math.ceil(h / 8.0) if h > 0 else 0)
    inferred_hours = result["max_days"].map(lambda d: d * 8.0 if d > 0 else 0)

    result.loc[result["max_days"] <= 0, "max_days"] = inferred_days
    result.loc[result["max_hours"] <= 0, "max_hours"] = inferred_hours

    result = result[(result["max_hours"] > 0) & (result["max_days"] > 0)]

    result = (
        result.groupby("employee", as_index=False)
        .agg(
            max_hours=("max_hours", "sum"),
            max_days=("max_days", "sum"),
            restaurant=("restaurant", lambda s: _pick_text_or_default(s, "не указан")),
            role=("role", lambda s: _pick_text_or_default(s, "не указана")),
        )
        .reset_index(drop=True)
    )

    result["max_days"] = result["max_days"].round().astype(int)
    result["restaurant"] = result["restaurant"].replace("", "не указан")
    result["role"] = result["role"].replace("", "не указана")

    return result


def _extract_days_from_values(values: list[Any]) -> set[int]:
    days: set[int] = set()
    for value in values:
        if value is None:
            continue

        if isinstance(value, pd.Timestamp):
            days.add(int(value.day))
            continue

        if hasattr(value, "day") and hasattr(value, "month"):
            try:
                day = int(value.day)
                if 1 <= day <= 31:
                    days.add(day)
                    continue
            except Exception:
                pass

        text = _normalize_text(value)
        if not text:
            continue

        for match in re.findall(r"\b([1-9]|[12][0-9]|3[01])\b", text):
            day = int(match)
            if 1 <= day <= 31:
                days.add(day)

    return days


def _extract_day_weekday_pairs(values: list[Any]) -> list[tuple[int, int]]:
    pairs: list[tuple[int, int]] = []

    for value in values:
        if value is None:
            continue

        ts: Optional[pd.Timestamp] = None

        if isinstance(value, pd.Timestamp):
            ts = value
        elif hasattr(value, "year") and hasattr(value, "month") and hasattr(value, "day"):
            try:
                ts = pd.Timestamp(year=int(value.year), month=int(value.month), day=int(value.day))
            except Exception:
                ts = None
        else:
            text = _normalize_text(value)
            if text:
                match = re.search(r"\b(\d{1,2}[./-]\d{1,2}[./-]\d{2,4})\b", text)
                if match:
                    parsed = pd.to_datetime(match.group(1), dayfirst=True, errors="coerce")
                    if pd.notna(parsed):
                        ts = parsed

        if ts is None or pd.isna(ts):
            continue

        day = int(ts.day)
        if 1 <= day <= 31:
            pairs.append((day, int(ts.weekday())))

    return pairs


def parse_calendar_from_timesheet(file_bytes: bytes) -> tuple[list[int], list[int]]:
    workbook = pd.read_excel(BytesIO(file_bytes), sheet_name=None, dtype=object)
    day_candidates: set[int] = set()
    weekend_votes: dict[int, dict[bool, int]] = defaultdict(lambda: {True: 0, False: 0})

    for _, df in workbook.items():
        if df.empty:
            continue

        header_values = list(df.columns)
        day_candidates.update(_extract_days_from_values(header_values))
        for day, weekday in _extract_day_weekday_pairs(header_values):
            weekend_votes[day][weekday >= 5] += 1

        sample_rows = min(6, len(df))
        for row_idx in range(sample_rows):
            row_values = df.iloc[row_idx].tolist()
            day_candidates.update(_extract_days_from_values(row_values))
            for day, weekday in _extract_day_weekday_pairs(row_values):
                weekend_votes[day][weekday >= 5] += 1

    if not day_candidates:
        fallback_days = list(range(1, 32))
        fallback_weekends = [day for day in fallback_days if day % 7 in (6, 0)]
        return fallback_days, fallback_weekends

    weekend_days = [
        day
        for day in sorted(day_candidates)
        if weekend_votes.get(day, {True: 0, False: 0})[True]
        > weekend_votes.get(day, {True: 0, False: 0})[False]
    ]

    if not weekend_days:
        # Fallback: если в файле нет явных дат с годом/месяцем, считаем по шаблону недели.
        weekend_days = [day for day in sorted(day_candidates) if day % 7 in (6, 0)]

    return sorted(day_candidates), sorted(weekend_days)


def prepare_input(
    payroll_bytes: bytes,
    timesheet_bytes: bytes,
    employees_bytes: Optional[bytes] = None,
) -> PreparedInput:
    payroll_df = parse_payroll(payroll_bytes)
    days, weekend_days = parse_calendar_from_timesheet(timesheet_bytes)

    merged = payroll_df.copy()
    warnings: list[str] = []

    if employees_bytes:
        employees_df = parse_employee_list(employees_bytes)
        merged = merged.merge(
            employees_df.rename(columns={"restaurant": "restaurant_file", "role": "role_file"}),
            on="employee",
            how="left",
        )
        merged["restaurant"] = merged["restaurant_file"].fillna(merged["restaurant"])
        merged["role"] = merged["role_file"].fillna(merged["role"])
        merged = merged.drop(columns=["restaurant_file", "role_file"])

    merged["restaurant"] = merged["restaurant"].fillna("не указан").map(_normalize_text)
    merged["role"] = merged["role"].fillna("не указана").map(_normalize_text)

    missing_restaurant = (merged["restaurant"] == "").sum()
    missing_role = (merged["role"] == "").sum()

    if missing_restaurant:
        warnings.append(
            f"Для {missing_restaurant} сотрудников не найдено подразделение в расчетных листках. "
            "Им присвоен 'не указан'."
        )
    if missing_role:
        warnings.append(
            f"Для {missing_role} сотрудников не найдена должность в расчетных листках. "
            "Им присвоена 'не указана'."
        )

    merged["restaurant"] = merged["restaurant"].replace("", "не указан")
    merged["role"] = merged["role"].replace("", "не указана")
    merged["role"] = merged["role"].map(_map_role_group)

    merged = merged[["employee", "restaurant", "role", "max_hours", "max_days"]].copy()
    merged["max_hours"] = merged["max_hours"].astype(float)
    merged["max_days"] = merged["max_days"].astype(int)

    merged = merged.sort_values(["restaurant", "role", "employee"]).reset_index(drop=True)

    summary = {
        "employee_count": int(len(merged)),
        "restaurants": int(merged["restaurant"].nunique()),
        "roles": int(merged["role"].nunique()),
        "days_in_template": int(len(days)),
        "weekend_days_in_template": int(len(weekend_days)),
    }

    return PreparedInput(
        employees=merged,
        days=days,
        weekend_days=weekend_days,
        warnings=warnings,
        summary=summary,
    )
