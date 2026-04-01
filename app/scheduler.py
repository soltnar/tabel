from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import re
from typing import Any, Optional

import pandas as pd
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter


@dataclass(frozen=True)
class Shift:
    key: str
    label: str
    start: str
    end: str
    hours: float


SHIFTS: tuple[Shift, ...] = (
    Shift("lunch", "бизнес-ланч", "12:00", "16:00", 4.0),
    Shift("evening", "вечерняя посадка", "18:00", "22:00", 4.0),
)

MANDATORY_SHIFT_KEYS = ("lunch", "evening")
OPTIONAL_SHIFT_KEYS: tuple[str, ...] = ()
KITCHEN_ROLE = "кухня (повар/шеф/су-шеф)"
DEFICIT_EMPLOYEE_LABEL = "НЕ ЗАКРЫТО"
MAX_CONSECUTIVE_WORK_DAYS = 6
NON_MANDATORY_ROLE_TOKENS = (
    "подсобный рабочий",
    "бухгалтер",
    "директор",
    "маркетолог",
    "аниматор",
    "гардеробщик",
)
WEEKEND_FOCUSED_OPTIONAL_ROLE_TOKENS = (
    "аниматор",
    "гардеробщик",
)
WEEKDAY_FOCUSED_OPTIONAL_ROLE_TOKENS = (
    "бухгалтер",
    "подсобный рабочий",
    "маркетолог",
    "директор",
)


class ScheduleGenerationError(RuntimeError):
    pass


@dataclass
class ScheduleResult:
    assignments: pd.DataFrame
    employee_summary: pd.DataFrame
    matrix: pd.DataFrame
    warnings: list[str]


class _EmployeeState:
    def __init__(self, row: pd.Series) -> None:
        self.employee = str(row["employee"])
        self.restaurant = str(row["restaurant"])
        self.role_original = str(row["role_original"])
        self.role_group = str(row["role_group"])
        self.max_hours = float(row["max_hours"])
        self.max_days = int(row["max_days"])

        self.used_hours = 0.0
        self.used_days = 0
        self.daily_hours: dict[int, float] = defaultdict(float)
        self.assigned_days: set[int] = set()
        self.daily_shift_keys: dict[int, set[str]] = defaultdict(set)

    @property
    def id(self) -> tuple[str, str, str]:
        return (self.employee, self.restaurant, self.role_group)


def _shift_by_key(key: str) -> Shift:
    for shift in SHIFTS:
        if shift.key == key:
            return shift
    raise KeyError(key)


def _is_mandatory_coverage_role(role: str) -> bool:
    role_norm = str(role).strip().lower()
    return not any(token in role_norm for token in NON_MANDATORY_ROLE_TOKENS)


def _required_slots_for_shift(day: int, shift_key: str, weekend_days: set[int], role: str) -> int:
    role_norm = str(role).strip().lower()
    is_mandatory_role = _is_mandatory_coverage_role(role_norm)
    is_weekend = day in weekend_days

    if not is_mandatory_role:
        if any(token in role_norm for token in WEEKEND_FOCUSED_OPTIONAL_ROLE_TOKENS):
            return 2 if is_weekend else 0
        if any(token in role_norm for token in WEEKDAY_FOCUSED_OPTIONAL_ROLE_TOKENS):
            return 1 if not is_weekend else 0
        return 1 if is_weekend else 0

    base = 1
    if shift_key in MANDATORY_SHIFT_KEYS and is_weekend:
        return base + 1
    return base


def _consecutive_span_with_day(assigned_days: set[int], day: int) -> int:
    days = set(assigned_days)
    days.add(day)

    left = day
    while (left - 1) in days:
        left -= 1

    right = day
    while (right + 1) in days:
        right += 1

    return right - left + 1


def _can_take_shift(emp: _EmployeeState, day: int, shift: Shift) -> bool:
    new_day = day not in emp.assigned_days

    if shift.key in emp.daily_shift_keys[day]:
        return False

    if emp.used_hours + shift.hours > emp.max_hours + 1e-9:
        return False

    if new_day and emp.used_days + 1 > emp.max_days:
        return False

    if emp.daily_hours[day] + shift.hours > 13.0 + 1e-9:
        return False

    if new_day:
        streak = _consecutive_span_with_day(emp.assigned_days, day)
        if streak > MAX_CONSECUTIVE_WORK_DAYS:
            return False

    return True


def _candidate_score(
    emp: _EmployeeState,
    day: int,
    prefer_existing_day: bool,
    deterministic_tiebreak: float,
    day_rank: int,
    total_days: int,
) -> float:
    hours_ratio = emp.used_hours / max(emp.max_hours, 1.0)
    days_ratio = emp.used_days / max(float(emp.max_days), 1.0)

    day_presence_penalty = 0.0
    if prefer_existing_day:
        day_presence_penalty = 0.16 if day not in emp.assigned_days else 0.0
    else:
        day_presence_penalty = 0.12 if day in emp.assigned_days else 0.0

    day_load_penalty = (emp.daily_hours[day] / 13.0) * 0.2

    projected_days = emp.used_days + (1 if day not in emp.assigned_days else 0)
    expected_days = (day_rank / max(total_days, 1)) * float(emp.max_days)
    pace_ahead = max(0.0, projected_days - expected_days)
    pace_behind = max(0.0, expected_days - projected_days)
    # Сильно штрафуем "раннее выгорание" (слишком много смен в первой половине месяца).
    pace_penalty = (0.22 * pace_ahead) + (0.05 * pace_behind)

    streak = _consecutive_span_with_day(emp.assigned_days, day)
    # После 3-4 дней подряд рейтинг ухудшается, чтобы график был распределенным.
    streak_penalty = max(0, streak - 3) * 0.12

    return (
        (0.55 * hours_ratio)
        + (0.35 * days_ratio)
        + day_presence_penalty
        + day_load_penalty
        + pace_penalty
        + streak_penalty
        + deterministic_tiebreak
    )


def _pick_employee(
    employees: list[_EmployeeState],
    day: int,
    shift: Shift,
    prefer_existing_day: bool,
    day_rank: int,
    total_days: int,
) -> Optional[_EmployeeState]:
    candidates = [emp for emp in employees if _can_take_shift(emp, day, shift)]
    if not candidates:
        return None

    scored = []
    for emp in candidates:
        tie = (abs(hash((emp.id, day, shift.key))) % 1000) / 10_000_000
        score = _candidate_score(
            emp,
            day,
            prefer_existing_day,
            tie,
            day_rank=day_rank,
            total_days=total_days,
        )
        scored.append((score, emp))

    scored.sort(key=lambda x: x[0])
    return scored[0][1]


def _pick_from_primary_or_fallback(
    primary_pool: list[_EmployeeState],
    fallback_pool: list[_EmployeeState],
    day: int,
    shift: Shift,
    prefer_existing_day: bool,
    day_rank: int,
    total_days: int,
) -> tuple[Optional[_EmployeeState], bool]:
    picked = _pick_employee(
        primary_pool,
        day,
        shift,
        prefer_existing_day,
        day_rank=day_rank,
        total_days=total_days,
    )
    if picked is not None:
        return picked, False

    if fallback_pool:
        picked = _pick_employee(
            fallback_pool,
            day,
            shift,
            prefer_existing_day,
            day_rank=day_rank,
            total_days=total_days,
        )
        if picked is not None:
            return picked, True

    return None, False


def _assign_shift(emp: _EmployeeState, day: int, shift: Shift) -> None:
    if day not in emp.assigned_days:
        emp.assigned_days.add(day)
        emp.used_days += 1

    emp.used_hours += shift.hours
    emp.daily_hours[day] += shift.hours
    emp.daily_shift_keys[day].add(shift.key)


def _build_warnings(
    mandatory_deficits: list[dict[str, Any]],
    optional_missed_count: int,
    cross_restaurant_count: int,
    cross_restaurant_employees: set[str],
) -> list[str]:
    warnings: list[str] = []

    if mandatory_deficits:
        warnings.append(
            f"Обязательные смены с дефицитом: {len(mandatory_deficits)}."
        )

        deficit_df = pd.DataFrame(mandatory_deficits)
        grouped = (
            deficit_df.groupby(["restaurant", "role"], as_index=False)
            .size()
            .sort_values("size", ascending=False)
        )
        for _, row in grouped.head(10).iterrows():
            warnings.append(
                f"Дефицит {int(row['size'])} смен: {row['restaurant']} / {row['role']}."
            )

    if optional_missed_count:
        warnings.append(
            f"Дополнительные (необязательные) слоты без назначения: {optional_missed_count}."
        )

    if cross_restaurant_count:
        warnings.append(
            f"Межресторанные подмены кухни: {cross_restaurant_count}."
        )
        names = sorted(cross_restaurant_employees)
        if names:
            sample = ", ".join(names[:20])
            suffix = " ..." if len(names) > 20 else ""
            warnings.append(f"Сотрудники с межресторанными сменами: {sample}{suffix}")

    return warnings


def generate_schedule(
    employees_df: pd.DataFrame,
    days: list[int],
    weekend_days: Optional[set[int]] = None,
) -> ScheduleResult:
    if employees_df.empty:
        raise ScheduleGenerationError("Нет сотрудников для планирования.")

    if not days:
        raise ScheduleGenerationError("В шаблоне табеля не найдены дни месяца.")

    states = [_EmployeeState(row) for _, row in employees_df.iterrows()]

    by_group: dict[tuple[str, str], list[_EmployeeState]] = defaultdict(list)
    for state in states:
        by_group[(state.restaurant, state.role_group)].append(state)

    target_groups = sorted(by_group.keys(), key=lambda x: (x[0], x[1]))
    kitchen_pool = [state for state in states if state.role_group == KITCHEN_ROLE]

    assignments: list[dict[str, Any]] = []
    mandatory_deficits: list[dict[str, Any]] = []
    optional_missed_count = 0
    cross_restaurant_count = 0
    cross_restaurant_employees: set[str] = set()

    sorted_days = sorted(int(day) for day in days)
    weekend_days_set = set(int(day) for day in (weekend_days or set()))

    total_days = len(sorted_days)

    for day_rank, day in enumerate(sorted_days, start=1):
        for restaurant, role_group in target_groups:
            primary_pool = by_group[(restaurant, role_group)]
            fallback_pool: list[_EmployeeState] = []

            if role_group == KITCHEN_ROLE:
                fallback_pool = [emp for emp in kitchen_pool if emp.restaurant != restaurant]

            role_is_mandatory = _is_mandatory_coverage_role(role_group)

            for shift_key in MANDATORY_SHIFT_KEYS:
                shift = _shift_by_key(shift_key)
                required_slots = _required_slots_for_shift(
                    day=day,
                    shift_key=shift_key,
                    weekend_days=weekend_days_set,
                    role=role_group,
                )
                prefer_existing_day = not role_is_mandatory

                for _slot in range(required_slots):
                    picked, _ = _pick_from_primary_or_fallback(
                        primary_pool=primary_pool,
                        fallback_pool=fallback_pool,
                        day=day,
                        shift=shift,
                        prefer_existing_day=prefer_existing_day,
                        day_rank=day_rank,
                        total_days=total_days,
                    )

                    if picked is None:
                        if role_is_mandatory:
                            assignments.append(
                                {
                                    "day": day,
                                    "restaurant": restaurant,
                                    "role": role_group,
                                    "role_group": role_group,
                                    "role_original": "",
                                    "shift": shift.key,
                                    "shift_label": shift.label,
                                    "start": shift.start,
                                    "end": shift.end,
                                    "hours": shift.hours,
                                    "employee": DEFICIT_EMPLOYEE_LABEL,
                                    "employee_home_restaurant": "",
                                    "cross_restaurant": False,
                                    "deficit": True,
                                    "status": "дефицит",
                                }
                            )
                            mandatory_deficits.append(
                                {
                                    "day": day,
                                    "restaurant": restaurant,
                                    "role": role_group,
                                    "shift": shift.label,
                                }
                            )
                        else:
                            optional_missed_count += 1
                        continue

                    _assign_shift(picked, day, shift)
                    cross_restaurant = picked.restaurant != restaurant
                    if cross_restaurant:
                        cross_restaurant_count += 1
                        cross_restaurant_employees.add(picked.employee)

                    assignments.append(
                        {
                            "day": day,
                            "restaurant": restaurant,
                            "role": picked.role_original,
                            "role_group": role_group,
                            "role_original": picked.role_original,
                            "shift": shift.key,
                            "shift_label": shift.label,
                            "start": shift.start,
                            "end": shift.end,
                            "hours": shift.hours,
                            "employee": picked.employee,
                            "employee_home_restaurant": picked.restaurant,
                            "cross_restaurant": cross_restaurant,
                            "deficit": False,
                            "status": "межресторанная замена" if cross_restaurant else "ok",
                        }
                    )

            for shift_key in OPTIONAL_SHIFT_KEYS:
                shift = _shift_by_key(shift_key)
                picked, _ = _pick_from_primary_or_fallback(
                    primary_pool=primary_pool,
                    fallback_pool=fallback_pool,
                    day=day,
                    shift=shift,
                    prefer_existing_day=True,
                    day_rank=day_rank,
                    total_days=total_days,
                )

                if picked is None:
                    optional_missed_count += 1
                    continue

                _assign_shift(picked, day, shift)
                cross_restaurant = picked.restaurant != restaurant
                if cross_restaurant:
                    cross_restaurant_count += 1
                    cross_restaurant_employees.add(picked.employee)

                assignments.append(
                    {
                            "day": day,
                            "restaurant": restaurant,
                            "role": picked.role_original,
                            "role_group": role_group,
                            "role_original": picked.role_original,
                            "shift": shift.key,
                            "shift_label": shift.label,
                            "start": shift.start,
                            "end": shift.end,
                        "hours": shift.hours,
                        "employee": picked.employee,
                        "employee_home_restaurant": picked.restaurant,
                        "cross_restaurant": cross_restaurant,
                        "deficit": False,
                        "status": "межресторанная замена" if cross_restaurant else "ok",
                    }
                )

    if assignments:
        assignments_df = pd.DataFrame(assignments).sort_values(
            ["day", "restaurant", "role_group", "role", "start", "employee"]
        )
    else:
        assignments_df = pd.DataFrame(
            columns=[
                "day",
                "restaurant",
                "role",
                "role_group",
                "role_original",
                "shift",
                "shift_label",
                "start",
                "end",
                "hours",
                "employee",
                "employee_home_restaurant",
                "cross_restaurant",
                "deficit",
                "status",
            ]
        )

    cross_by_employee: dict[str, int] = {}
    if not assignments_df.empty:
        cross_rows = assignments_df[(~assignments_df["deficit"]) & (assignments_df["cross_restaurant"])]
        cross_by_employee = cross_rows.groupby("employee").size().to_dict()

    summary_rows = []
    for emp in states:
        summary_rows.append(
            {
                "employee": emp.employee,
                "restaurant": emp.restaurant,
                "role": emp.role_original,
                "role_group": emp.role_group,
                "planned_hours": round(emp.used_hours, 2),
                "planned_days": emp.used_days,
                "max_hours": round(emp.max_hours, 2),
                "max_days": emp.max_days,
                "hours_ok": emp.used_hours <= emp.max_hours + 1e-9,
                "days_ok": emp.used_days <= emp.max_days,
                "cross_restaurant_shifts": int(cross_by_employee.get(emp.employee, 0)),
            }
        )

    summary_df = pd.DataFrame(summary_rows).sort_values(
        ["restaurant", "role", "employee"]
    )

    code_map = {"lunch": "БЛ", "evening": "В", "morning": "У", "day": "Д"}
    code_order = {"У": 0, "Д": 1, "БЛ": 2, "В": 3}

    matrix_index_df = pd.DataFrame(
        [{"employee": emp.employee, "restaurant": emp.restaurant, "role": emp.role_original} for emp in states]
    ).drop_duplicates()

    matrix_base = assignments_df[
        (~assignments_df.get("deficit", False))
        & (assignments_df.get("employee", "") != DEFICIT_EMPLOYEE_LABEL)
    ].copy()

    if matrix_base.empty:
        matrix_df = matrix_index_df.copy()
    else:
        matrix_base["shift_code"] = matrix_base["shift"].map(code_map)
        pivot_df = (
            matrix_base.groupby(["employee", "restaurant", "role", "day"], as_index=False)["shift_code"]
            .agg(lambda values: "+".join(sorted(set(values), key=lambda x: code_order.get(x, 99))))
            .pivot(index=["employee", "restaurant", "role"], columns="day", values="shift_code")
            .reset_index()
        )
        matrix_df = matrix_index_df.merge(pivot_df, on=["employee", "restaurant", "role"], how="left")

    for day in sorted_days:
        if day not in matrix_df.columns:
            matrix_df[day] = ""

    matrix_df = matrix_df.fillna("")
    numeric_columns = [col for col in matrix_df.columns if isinstance(col, int)]
    ordered_cols = ["employee", "restaurant", "role"] + sorted(numeric_columns)
    matrix_df = matrix_df[ordered_cols]

    warnings = _build_warnings(
        mandatory_deficits=mandatory_deficits,
        optional_missed_count=optional_missed_count,
        cross_restaurant_count=cross_restaurant_count,
        cross_restaurant_employees=cross_restaurant_employees,
    )

    return ScheduleResult(
        assignments=assignments_df,
        employee_summary=summary_df,
        matrix=matrix_df,
        warnings=warnings,
    )


def export_schedule_to_excel(result: ScheduleResult, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        result.assignments.to_excel(writer, index=False, sheet_name="График")
        result.employee_summary.to_excel(writer, index=False, sheet_name="Сводка")
        result.matrix.to_excel(writer, index=False, sheet_name="Матрица")


def _build_t13_dataframe(result: ScheduleResult, days: list[int]) -> tuple[pd.DataFrame, dict[str, str]]:
    sorted_days = sorted({int(day) for day in days})
    day_columns = [str(day) for day in sorted_days]

    assignments = result.assignments.copy()
    if assignments.empty:
        assignments = pd.DataFrame(
            columns=[
                "employee",
                "day",
                "hours",
                "deficit",
                "cross_restaurant",
                "restaurant",
                "shift",
            ]
        )

    if "day" in assignments.columns:
        assignments["day"] = pd.to_numeric(assignments["day"], errors="coerce").fillna(0).astype(int)
    else:
        assignments["day"] = 0

    if "deficit" not in assignments.columns:
        assignments["deficit"] = False
    else:
        assignments["deficit"] = assignments["deficit"].fillna(False)

    filtered = assignments[
        (~assignments["deficit"]) & (assignments["employee"] != DEFICIT_EMPLOYEE_LABEL)
    ].copy()

    employee_base = (
        result.employee_summary[["employee", "restaurant", "role"]]
        .drop_duplicates()
        .sort_values(["restaurant", "role", "employee"])
        .reset_index(drop=True)
    )

    all_restaurants = sorted(
        set(employee_base["restaurant"].astype(str).tolist())
        | set(filtered.get("restaurant", pd.Series(dtype=str)).astype(str).tolist())
    )
    restaurant_codes = {name: f"R{idx:02d}" for idx, name in enumerate(all_restaurants, start=1)}

    day_hours_map: dict[tuple[str, int], float] = {}
    day_details_map: dict[tuple[str, int], list[tuple[str, str]]] = defaultdict(list)

    if not filtered.empty:
        filtered["hours"] = pd.to_numeric(filtered["hours"], errors="coerce").fillna(0.0)
        filtered["restaurant"] = filtered["restaurant"].astype(str)

        shift_to_code = {"lunch": "БЛ", "evening": "В", "morning": "У", "day": "Д"}
        shift_order = {"У": 0, "Д": 1, "БЛ": 2, "В": 3}
        filtered["shift_code"] = filtered["shift"].map(shift_to_code).fillna("Я")

        day_hours_map = (
            filtered.groupby(["employee", "day"], as_index=False)["hours"]
            .sum()
            .set_index(["employee", "day"])["hours"]
            .to_dict()
        )

        grouped = (
            filtered.groupby(["employee", "day", "restaurant"], as_index=False)["shift_code"]
            .agg(lambda vals: "+".join(sorted(set(vals), key=lambda x: shift_order.get(x, 99))))
        )
        for _, row in grouped.iterrows():
            key = (str(row["employee"]), int(row["day"]))
            day_details_map[key].append((str(row["restaurant"]), str(row["shift_code"])))

        for key in day_details_map:
            day_details_map[key] = sorted(day_details_map[key], key=lambda x: x[0])

    rows: list[dict[str, Any]] = []
    total_hours_all = 0.0
    total_worked_days_all = 0
    total_hours_by_day: dict[int, float] = defaultdict(float)
    worked_day_pairs: set[tuple[str, int]] = set()

    for idx, row in employee_base.iterrows():
        employee = str(row["employee"])
        restaurant = str(row["restaurant"])
        role = str(row["role"])
        base_rest_code = restaurant_codes.get(restaurant, "R00")

        row_codes: dict[str, Any] = {
            "№ п/п": idx + 1,
            "Подразделение": f"{restaurant} ({base_rest_code})",
            "Сотрудник": employee,
            "Должность": role,
        }
        row_hours: dict[str, Any] = {
            "№ п/п": "",
            "Подразделение": "",
            "Сотрудник": "",
            "Должность": "часы",
        }

        worked_days = 0
        total_hours = 0.0
        cross_days: list[int] = []

        for day in sorted_days:
            day_col = str(day)
            key = (employee, day)
            hours = float(day_hours_map.get(key, 0.0))
            details = day_details_map.get(key, [])

            if hours <= 0:
                row_codes[day_col] = ""
                row_hours[day_col] = ""
                continue

            worked_days += 1
            total_hours += hours
            total_hours_all += hours
            total_hours_by_day[day] += hours
            worked_day_pairs.add(key)

            code_parts = []
            day_is_cross = False
            for rest_name, shift_code in details:
                rest_code = restaurant_codes.get(rest_name, "R00")
                code_parts.append(f"{shift_code}@{rest_code}")
                if rest_name != restaurant:
                    day_is_cross = True

            row_codes[day_col] = " | ".join(code_parts) if code_parts else f"Я@{base_rest_code}"
            row_hours[day_col] = round(hours, 2)
            if day_is_cross:
                cross_days.append(day)

        row_codes["Итого дней"] = worked_days
        row_codes["Итого часов"] = round(total_hours, 2)
        row_codes["Примечание"] = (
            f"межресторанные дни: {', '.join(str(d) for d in cross_days)}" if cross_days else ""
        )

        row_hours["Итого дней"] = ""
        row_hours["Итого часов"] = round(total_hours, 2) if total_hours > 0 else ""
        row_hours["Примечание"] = ""

        rows.append(row_codes)
        rows.append(row_hours)

    total_worked_days_all = len(worked_day_pairs)

    total_row: dict[str, Any] = {
        "№ п/п": "ИТОГО",
        "Подразделение": "",
        "Сотрудник": "",
        "Должность": "",
    }
    for day in sorted_days:
        day_hours = round(total_hours_by_day.get(day, 0.0), 2)
        total_row[str(day)] = day_hours if day_hours > 0 else ""
    total_row["Итого дней"] = total_worked_days_all
    total_row["Итого часов"] = round(total_hours_all, 2)
    total_row["Примечание"] = ""
    rows.append(total_row)

    ordered_columns = (
        ["№ п/п", "Подразделение", "Сотрудник", "Должность"]
        + day_columns
        + ["Итого дней", "Итого часов", "Примечание"]
    )

    df = pd.DataFrame(rows)
    for col in ordered_columns:
        if col not in df.columns:
            df[col] = ""
    df = df[ordered_columns].fillna("")
    return df, restaurant_codes


def export_t13_to_excel(result: ScheduleResult, days: list[int], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    t13_df, restaurant_codes = _build_t13_dataframe(result=result, days=days)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        sheet_name = "Т-13"
        start_row = 4
        t13_df.to_excel(writer, index=False, sheet_name=sheet_name, startrow=start_row)

        ws = writer.sheets[sheet_name]
        max_col = ws.max_column
        max_row = ws.max_row

        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=max_col)
        ws.cell(
            row=1,
            column=1,
            value="Табель учета рабочего времени (Унифицированная форма № Т-13)",
        )
        ws.cell(row=2, column=1, value=f"Сформировано: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
        if restaurant_codes:
            legend = "; ".join(f"{code}={name}" for name, code in restaurant_codes.items())
            ws.merge_cells(start_row=3, start_column=1, end_row=3, end_column=max_col)
            ws.cell(row=3, column=1, value=f"Коды подразделений: {legend}")

        title_cell = ws.cell(row=1, column=1)
        title_cell.font = Font(bold=True, size=12)
        title_cell.alignment = Alignment(horizontal="center")
        ws.cell(row=2, column=1).font = Font(italic=True, size=10)
        ws.cell(row=3, column=1).font = Font(size=9, color="404040")
        ws.cell(row=3, column=1).alignment = Alignment(horizontal="left")

        header_row = start_row + 1
        header_fill = PatternFill(start_color="DCEBFA", end_color="DCEBFA", fill_type="solid")
        for col_idx in range(1, max_col + 1):
            cell = ws.cell(row=header_row, column=col_idx)
            cell.font = Font(bold=True)
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.fill = header_fill

        ws.freeze_panes = "E6"

        width_map = {
            1: 8,   # №
            2: 28,  # Подразделение
            3: 30,  # Сотрудник
            4: 24,  # Должность
        }
        for col_idx in range(1, max_col + 1):
            width = width_map.get(col_idx, 11)
            if col_idx > max_col - 3:
                width = 14
            ws.column_dimensions[get_column_letter(col_idx)].width = width

        cross_fill = PatternFill(start_color="FFF7DC", end_color="FFF7DC", fill_type="solid")

        for row_idx in range(header_row + 1, max_row + 1):
            role_value = str(ws.cell(row=row_idx, column=4).value or "").strip().lower()
            if role_value == "часы":
                for col_idx in range(1, max_col + 1):
                    ws.cell(row=row_idx, column=col_idx).font = Font(size=9, color="404040")
                    ws.cell(row=row_idx, column=col_idx).alignment = Alignment(
                        horizontal="center", vertical="center"
                    )
                continue

            if str(ws.cell(row=row_idx, column=1).value).strip().upper() == "ИТОГО":
                for col_idx in range(1, max_col + 1):
                    ws.cell(row=row_idx, column=col_idx).font = Font(bold=True)
                    ws.cell(row=row_idx, column=col_idx).alignment = Alignment(
                        horizontal="center", vertical="center"
                    )
                continue

            base_rest = str(ws.cell(row=row_idx, column=2).value or "")
            base_match = re.search(r"\((R\d{2})\)\s*$", base_rest)
            base_code = base_match.group(1) if base_match else ""
            for col_idx in range(1, max_col + 1):
                cell = ws.cell(row=row_idx, column=col_idx)
                if col_idx <= 4:
                    cell.alignment = Alignment(horizontal="left", vertical="center")
                else:
                    cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

            first_day_col = 5
            last_day_col = max_col - 3
            for col_idx in range(first_day_col, last_day_col + 1):
                code_cell = ws.cell(row=row_idx, column=col_idx)
                code_text = str(code_cell.value or "").strip()
                if not code_text:
                    continue

                day_codes = re.findall(r"@([A-Z]\d{2})", code_text)
                if day_codes and any(code != base_code for code in day_codes):
                    code_cell.fill = cross_fill
                    next_role = str(ws.cell(row=row_idx + 1, column=4).value or "").strip().lower()
                    if next_role == "часы":
                        ws.cell(row=row_idx + 1, column=col_idx).fill = cross_fill
