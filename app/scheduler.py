from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
import re
from typing import Any, Optional

import pandas as pd
from openpyxl.comments import Comment
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
KITCHEN_ROLE = "Кухня"
BAR_ROLE = "Бар"
HALL_ROLE = "Зал"
CASH_ROLE = "Касса"
CORE_MANDATORY_GROUPS: tuple[str, ...] = (KITCHEN_ROLE, HALL_ROLE, CASH_ROLE, BAR_ROLE)
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
        self.daily_shift_groups: dict[int, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))

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


def _is_core_group(role_group: str) -> bool:
    return str(role_group) in CORE_MANDATORY_GROUPS


def _required_slots_for_shift(
    day: int,
    shift_key: str,
    weekend_days: set[int],
    role: str,
    is_mandatory_role: bool,
) -> int:
    role_norm = str(role).strip().lower()
    role_group = str(role).strip()
    _is_weekend = day in weekend_days

    # Для core-групп (кухня/зал/касса/бар) нужен один сотрудник на день:
    # не дублируем обязательный набор по каждой смене.
    # Это снижает переизбыток назначений и лучше соответствует факту,
    # что итоговая выработка по часам берется из расчетных листков.
    if _is_core_group(role_group):
        return 1 if shift_key == MANDATORY_SHIFT_KEYS[0] else 0

    if not is_mandatory_role:
        if any(token in role_norm for token in WEEKEND_FOCUSED_OPTIONAL_ROLE_TOKENS):
            return 2 if _is_weekend else 0
        if any(token in role_norm for token in WEEKDAY_FOCUSED_OPTIONAL_ROLE_TOKENS):
            return 1 if not _is_weekend else 0
        return 1 if _is_weekend else 0

    base = 1
    if shift_key in MANDATORY_SHIFT_KEYS and _is_weekend:
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
    return _can_take_shift_for_group(emp=emp, day=day, shift=shift, role_group="")


def _is_manager_role(emp: _EmployeeState) -> bool:
    role = str(emp.role_original).strip().lower()
    return ("менеджер" in role) or ("администратор" in role) or ("адмистратор" in role)


def _is_front_multirole(emp: _EmployeeState) -> bool:
    # Универсальный фронт-персонал: менеджеры и администраторы.
    # Могут закрывать Бар/Зал/Кассу, но не Кухню.
    return _is_manager_role(emp)


def _can_share_same_shift(emp: _EmployeeState, day: int, shift_key: str, role_group: str) -> bool:
    role_group = str(role_group or "").strip()
    if role_group not in {BAR_ROLE, CASH_ROLE}:
        return False
    if not _is_manager_role(emp):
        return False

    existing_groups = emp.daily_shift_groups[day].get(shift_key, set())
    if not existing_groups:
        return False
    if role_group in existing_groups:
        return False
    return all(group in {BAR_ROLE, CASH_ROLE} for group in existing_groups)


def _can_take_shift_for_group(emp: _EmployeeState, day: int, shift: Shift, role_group: str) -> bool:
    new_day = day not in emp.assigned_days

    same_shift_exists = shift.key in emp.daily_shift_keys[day]
    shared_same_shift = same_shift_exists and _can_share_same_shift(
        emp=emp,
        day=day,
        shift_key=shift.key,
        role_group=role_group,
    )
    if same_shift_exists and not shared_same_shift:
        return False

    if not shared_same_shift:
        if emp.used_hours + shift.hours > emp.max_hours + 1e-9:
            return False

    if new_day and emp.used_days + 1 > emp.max_days:
        return False

    if not shared_same_shift:
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
    role_group: str,
    prefer_existing_day: bool,
    day_rank: int,
    total_days: int,
) -> Optional[_EmployeeState]:
    candidates = [emp for emp in employees if _can_take_shift_for_group(emp, day, shift, role_group)]
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
    role_group: str,
    prefer_existing_day: bool,
    day_rank: int,
    total_days: int,
) -> tuple[Optional[_EmployeeState], bool]:
    picked = _pick_employee(
        primary_pool,
        day,
        shift,
        role_group,
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
            role_group,
            prefer_existing_day,
            day_rank=day_rank,
            total_days=total_days,
        )
        if picked is not None:
            return picked, True

    return None, False


def _dedupe_states(states: list[_EmployeeState]) -> list[_EmployeeState]:
    seen: set[tuple[str, str, str]] = set()
    result: list[_EmployeeState] = []
    for emp in states:
        if emp.id in seen:
            continue
        seen.add(emp.id)
        result.append(emp)
    return result


def _assign_shift(emp: _EmployeeState, day: int, shift: Shift, role_group: str) -> None:
    if day not in emp.assigned_days:
        emp.assigned_days.add(day)
        emp.used_days += 1

    shared_same_shift = _can_share_same_shift(
        emp=emp,
        day=day,
        shift_key=shift.key,
        role_group=role_group,
    )
    if not shared_same_shift:
        emp.used_hours += shift.hours
        emp.daily_hours[day] += shift.hours
    emp.daily_shift_keys[day].add(shift.key)
    emp.daily_shift_groups[day][shift.key].add(role_group)


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
            f"Межресторанные подмены: {cross_restaurant_count}."
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

    restaurants = sorted({state.restaurant for state in states})
    target_groups_set: set[tuple[str, str]] = set(by_group.keys())
    for restaurant in restaurants:
        for group in CORE_MANDATORY_GROUPS:
            target_groups_set.add((restaurant, group))
    group_priority = {
        CASH_ROLE: 0,
        HALL_ROLE: 1,
        KITCHEN_ROLE: 2,
        BAR_ROLE: 3,
    }
    target_groups = sorted(
        target_groups_set,
        key=lambda x: (x[0], group_priority.get(x[1], 99), x[1]),
    )
    kitchen_pool = [state for state in states if state.role_group == KITCHEN_ROLE]
    bar_pool = [state for state in states if state.role_group == BAR_ROLE]
    cash_pool = [state for state in states if state.role_group == CASH_ROLE]
    front_multirole_pool = [state for state in states if _is_front_multirole(state)]

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
            elif role_group == BAR_ROLE:
                # Для бара разрешаем:
                # 1) межресторанные барные подмены;
                # 2) подмену универсальным фронт-персоналом (менеджер/администратор),
                #    сначала локально, затем межресторанно.
                fallback_pool = [emp for emp in bar_pool if emp.restaurant != restaurant]
                fallback_pool.extend([emp for emp in front_multirole_pool if emp.restaurant == restaurant])
                fallback_pool.extend([emp for emp in front_multirole_pool if emp.restaurant != restaurant])
                fallback_pool = _dedupe_states(fallback_pool)
            elif role_group == HALL_ROLE:
                # Для зала: при дефиците подменяет универсальный фронт-персонал.
                fallback_pool = [emp for emp in front_multirole_pool if emp.restaurant == restaurant]
                fallback_pool.extend([emp for emp in front_multirole_pool if emp.restaurant != restaurant])
                fallback_pool = _dedupe_states(fallback_pool)
            elif role_group == CASH_ROLE:
                # Для кассы: разрешаем межресторанную подмену кассой
                # и универсальным фронт-персоналом.
                fallback_pool = [emp for emp in cash_pool if emp.restaurant != restaurant]
                fallback_pool.extend([emp for emp in front_multirole_pool if emp.restaurant == restaurant])
                fallback_pool.extend([emp for emp in front_multirole_pool if emp.restaurant != restaurant])
                fallback_pool = _dedupe_states(fallback_pool)

            role_is_mandatory = _is_core_group(role_group) or any(
                _is_mandatory_coverage_role(emp.role_original) for emp in primary_pool
            )

            for shift_key in MANDATORY_SHIFT_KEYS:
                shift = _shift_by_key(shift_key)
                required_slots = _required_slots_for_shift(
                    day=day,
                    shift_key=shift_key,
                    weekend_days=weekend_days_set,
                    role=role_group,
                    is_mandatory_role=role_is_mandatory,
                )
                prefer_existing_day = not role_is_mandatory

                for _slot in range(required_slots):
                    picked, _ = _pick_from_primary_or_fallback(
                        primary_pool=primary_pool,
                        fallback_pool=fallback_pool,
                        day=day,
                        shift=shift,
                        role_group=role_group,
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

                    _assign_shift(picked, day, shift, role_group)
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
                    role_group=role_group,
                    prefer_existing_day=True,
                    day_rank=day_rank,
                    total_days=total_days,
                )

                if picked is None:
                    optional_missed_count += 1
                    continue

                _assign_shift(picked, day, shift, role_group)
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


def build_preview_rows_t13_aligned(
    result: ScheduleResult,
    days: list[int],
    weekend_days: Optional[set[int]] = None,
) -> pd.DataFrame:
    sorted_days = sorted({int(day) for day in days})
    weekend_days_set = set(int(day) for day in (weekend_days or set()))

    assignments = result.assignments.copy()
    if assignments.empty:
        assignments = pd.DataFrame(columns=["employee", "day", "restaurant", "deficit", "cross_restaurant"])
    if "deficit" not in assignments.columns:
        assignments["deficit"] = False
    assignments = assignments[(~assignments["deficit"]) & (assignments["employee"] != DEFICIT_EMPLOYEE_LABEL)].copy()
    assignments["day"] = pd.to_numeric(assignments.get("day"), errors="coerce").fillna(0).astype(int)
    assignments["restaurant"] = assignments.get("restaurant", "").astype(str)

    day_restaurants_map: dict[tuple[str, int], set[str]] = defaultdict(set)
    for _, row in assignments.iterrows():
        key = (str(row["employee"]), int(row["day"]))
        day_restaurants_map[key].add(str(row["restaurant"]))

    employee_base = (
        result.employee_summary[["employee", "restaurant", "role", "role_group", "max_days", "max_hours"]]
        .drop_duplicates()
        .sort_values(["restaurant", "role", "employee"])
        .reset_index(drop=True)
    )

    rows: list[dict[str, Any]] = []
    for _, rec in employee_base.iterrows():
        employee = str(rec["employee"])
        home_restaurant = str(rec["restaurant"])
        role = str(rec["role"])
        role_group = str(rec["role_group"])
        payroll_days_target = int(pd.to_numeric(rec["max_days"], errors="coerce") or 0)
        payroll_hours_target = float(pd.to_numeric(rec["max_hours"], errors="coerce") or 0.0)

        factual_days = sorted({day for (emp_key, day) in day_restaurants_map.keys() if emp_key == employee})
        selected_days = _select_employee_days(
            factual_days=factual_days,
            all_days=sorted_days,
            target_count=payroll_days_target,
            prefer_weekends=_is_core_group(role_group),
            weekend_days=weekend_days_set,
        )

        day_hours = _distribute_hours(payroll_hours_target, len(selected_days))
        hours_map = {day: day_hours[pos] for pos, day in enumerate(selected_days)}

        for day in selected_days:
            worked_restaurants = sorted(day_restaurants_map.get((employee, day), set()))
            factual = bool(worked_restaurants)
            cross = any(rest != home_restaurant for rest in worked_restaurants)
            display_restaurant = worked_restaurants[0] if worked_restaurants else home_restaurant

            status = "ОК"
            if not factual:
                status = "План из расчетного листка"
            elif cross:
                status = "Межресторанная замена"

            rows.append(
                {
                    "day": int(day),
                    "restaurant": display_restaurant,
                    "role": role,
                    "shift_label": "Я" if factual else "ПЛ",
                    "start": "-",
                    "end": "-",
                    "hours": round(float(hours_map.get(day, 0.0)), 2),
                    "employee": employee,
                    "employee_home_restaurant": home_restaurant,
                    "cross_restaurant": cross,
                    "deficit": False,
                    "status": status,
                }
            )

    # Добавляем отдельные строки дефицита, чтобы их можно было быстро фильтровать в preview.
    deficit_rows = result.assignments[result.assignments.get("deficit", False)].copy()
    if not deficit_rows.empty:
        deficit_rows["day"] = pd.to_numeric(deficit_rows.get("day"), errors="coerce").fillna(0).astype(int)
        for _, row in deficit_rows.iterrows():
            role_value = str(row.get("role", "") or "").strip()
            role_group_value = str(row.get("role_group", "") or "").strip()
            role_display = role_value if role_value else role_group_value
            rows.append(
                {
                    "day": int(row.get("day", 0) or 0),
                    "restaurant": str(row.get("restaurant", "") or ""),
                    "role": role_display,
                    "shift_label": "ДЕФ",
                    "start": "-",
                    "end": "-",
                    "hours": round(float(pd.to_numeric(row.get("hours"), errors="coerce") or 0.0), 2),
                    "employee": DEFICIT_EMPLOYEE_LABEL,
                    "employee_home_restaurant": "-",
                    "cross_restaurant": False,
                    "deficit": True,
                    "status": "ДЕФИЦИТ",
                }
            )

    if not rows:
        return pd.DataFrame(
            columns=[
                "day",
                "restaurant",
                "role",
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

    preview_df = pd.DataFrame(rows).sort_values(["day", "restaurant", "role", "employee"]).reset_index(drop=True)
    return preview_df


def _distribute_hours(total_hours: float, day_count: int) -> list[float]:
    if day_count <= 0:
        return []
    if total_hours <= 0:
        return [0.0] * day_count

    units = int(round(total_hours * 2))
    base = units // day_count
    rem = units % day_count

    distributed = [base for _ in range(day_count)]
    for idx in range(rem):
        distributed[idx] += 1

    # Ограничение в 13 часов в день (26 получасов).
    cap = 26
    overflow = 0
    for idx, val in enumerate(distributed):
        if val > cap:
            overflow += val - cap
            distributed[idx] = cap

    idx = 0
    while overflow > 0 and day_count > 0:
        slot = idx % day_count
        if distributed[slot] < cap:
            distributed[slot] += 1
            overflow -= 1
        idx += 1
        if idx > day_count * 100:
            break

    return [val / 2.0 for val in distributed]


def _pick_extra_days(days: list[int], occupied: set[int], count: int) -> list[int]:
    if count <= 0:
        return []

    candidates = [day for day in days if day not in occupied]
    if not candidates:
        return []

    picked: list[int] = []
    step = max(1, len(candidates) // max(1, count))
    idx = 0
    while len(picked) < count and candidates:
        pick = candidates[idx % len(candidates)]
        if pick not in picked:
            picked.append(pick)
        idx += step
        if idx > len(candidates) * 10:
            for day in candidates:
                if len(picked) >= count:
                    break
                if day not in picked:
                    picked.append(day)
            break
    return sorted(picked[:count])


def _pick_evenly_from_days(candidate_days: list[int], count: int) -> list[int]:
    unique_days = sorted(set(int(day) for day in candidate_days))
    if count <= 0 or not unique_days:
        return []
    if count >= len(unique_days):
        return unique_days

    if count == 1:
        return [unique_days[len(unique_days) // 2]]

    n = len(unique_days)
    picked_indices: set[int] = set()
    for i in range(count):
        idx = round(i * (n - 1) / (count - 1))
        picked_indices.add(int(idx))

    # Если из-за округления взяли меньше count, добираем промежуточными индексами.
    cursor = 0
    while len(picked_indices) < count and cursor < n:
        picked_indices.add(cursor)
        cursor += 1

    picked = [unique_days[idx] for idx in sorted(picked_indices)]
    return sorted(picked[:count])


def _select_employee_days(
    factual_days: list[int],
    all_days: list[int],
    target_count: int,
    prefer_weekends: bool,
    weekend_days: set[int],
) -> list[int]:
    if target_count <= 0:
        return []

    factual_unique = sorted(set(int(day) for day in factual_days))
    selected = _pick_evenly_from_days(factual_unique, min(target_count, len(factual_unique)))

    if len(selected) < target_count:
        remaining = [day for day in sorted(set(all_days)) if day not in selected]
        if prefer_weekends:
            remaining.sort(key=lambda d: (d not in weekend_days, d))
        extras = _pick_evenly_from_days(remaining, target_count - len(selected))
        selected = sorted(set(selected + extras))

    if len(selected) > target_count:
        selected = _pick_evenly_from_days(selected, target_count)

    # Для core-групп стараемся, чтобы выходных было ощутимо больше, если есть доступные.
    if prefer_weekends and weekend_days and selected:
        weekend_selected = [day for day in selected if day in weekend_days]
        weekend_available = [day for day in all_days if day in weekend_days]
        min_weekend = min(len(weekend_available), max(1, round(target_count * 0.4)))

        if len(weekend_selected) < min_weekend:
            need = min_weekend - len(weekend_selected)
            replace_from = [day for day in selected if day not in weekend_days]
            replace_to = [day for day in weekend_available if day not in selected]
            replace_to = _pick_evenly_from_days(replace_to, need)
            for idx, day in enumerate(replace_to):
                if idx >= len(replace_from):
                    break
                selected.remove(replace_from[idx])
                selected.append(day)
            selected = sorted(set(selected))
            if len(selected) > target_count:
                selected = _pick_evenly_from_days(selected, target_count)

    return sorted(selected)


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
        summary_row = result.employee_summary[result.employee_summary["employee"] == employee]
        payroll_days_target = 0
        payroll_hours_target = 0.0
        if not summary_row.empty:
            payroll_days_target = int(pd.to_numeric(summary_row.iloc[0]["max_days"], errors="coerce") or 0)
            payroll_hours_target = float(pd.to_numeric(summary_row.iloc[0]["max_hours"], errors="coerce") or 0.0)

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

        factual_days = sorted({day for (emp_key, day) in day_details_map.keys() if emp_key == employee})
        role_group = str(row.get("role_group", ""))
        selected_days = _select_employee_days(
            factual_days=factual_days,
            all_days=sorted_days,
            target_count=payroll_days_target,
            prefer_weekends=_is_core_group(role_group),
            weekend_days=set(),
        )

        day_hours = _distribute_hours(payroll_hours_target, len(selected_days))
        hours_map = {day: day_hours[idx] for idx, day in enumerate(selected_days)}

        cross_days: list[int] = []

        for day in sorted_days:
            day_col = str(day)
            key = (employee, day)
            hours = float(hours_map.get(day, 0.0))
            details = day_details_map.get(key, [])

            if hours <= 0:
                row_codes[day_col] = ""
                row_hours[day_col] = ""
                continue

            total_hours_all += hours
            total_hours_by_day[day] += hours
            worked_day_pairs.add(key)

            code_parts = []
            day_is_cross = False
            for rest_name, shift_code in details:
                # В унифицированной Т-13 оставляем классический код "Я" в ячейке.
                code_parts.append("Я")
                if rest_name != restaurant:
                    day_is_cross = True

            row_codes[day_col] = "Я" if code_parts else "ПЛ"
            row_hours[day_col] = round(hours, 2)
            if day_is_cross:
                cross_days.append(day)

        row_codes["Итого дней"] = payroll_days_target
        row_codes["Итого часов"] = round(payroll_hours_target, 2)
        row_codes["Примечание"] = (
            (
                f"межресторанные дни: {', '.join(str(d) for d in cross_days)}; "
                "ПЛ — день из расчетного листка без назначенной смены"
            )
            if cross_days
            else "ПЛ — день из расчетного листка без назначенной смены"
        )

        row_hours["Итого дней"] = ""
        row_hours["Итого часов"] = round(payroll_hours_target, 2) if payroll_hours_target > 0 else ""
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


def _find_t13_day_columns(ws) -> tuple[dict[int, int], dict[int, int]]:
    def _strict_half_map(row_idx: int, day_from: int, day_to: int) -> Optional[dict[int, int]]:
        day_to_cols: dict[int, list[int]] = defaultdict(list)
        for col in range(1, ws.max_column + 1):
            val = ws.cell(row=row_idx, column=col).value
            day: Optional[int] = None
            if isinstance(val, (int, float)):
                day = int(val)
            elif isinstance(val, str):
                text = val.strip()
                if text.isdigit():
                    day = int(text)
            if day is None:
                continue
            if day_from <= day <= day_to:
                day_to_cols[day].append(col)

        expected_days = list(range(day_from, day_to + 1))
        if not all(day in day_to_cols for day in expected_days):
            return None

        # Выбираем возрастающую по колонкам последовательность для полного диапазона дней.
        chosen: dict[int, int] = {}
        prev_col = -1
        for day in expected_days:
            candidates = sorted(c for c in day_to_cols[day] if c > prev_col)
            if not candidates:
                return None
            chosen_col = candidates[0]
            chosen[day] = chosen_col
            prev_col = chosen_col

        # Отсекаем ложные попадания из правого служебного блока (там обычно большие разрывы).
        cols = [chosen[d] for d in expected_days]
        gaps = [cols[i + 1] - cols[i] for i in range(len(cols) - 1)]
        if not gaps or max(gaps) > 4:
            return None

        return chosen

    best_first: dict[int, int] = {}
    best_second: dict[int, int] = {}
    for row_idx in range(1, min(80, ws.max_row) + 1):
        first_candidate = _strict_half_map(row_idx, 1, 15)
        second_candidate = _strict_half_map(row_idx, 16, 31)
        if first_candidate and (not best_first or min(first_candidate.values()) < min(best_first.values())):
            best_first = first_candidate
        if second_candidate and (not best_second or min(second_candidate.values()) < min(best_second.values())):
            best_second = second_candidate

    if best_first and best_second:
        return best_first, best_second

    # Fallback на старую эвристику (если шаблон нестандартный).
    def _day_map_from_row(row_idx: int) -> dict[int, int]:
        mapped: dict[int, int] = {}
        for col in range(1, ws.max_column + 1):
            val = ws.cell(row=row_idx, column=col).value
            day: Optional[int] = None
            if isinstance(val, (int, float)):
                day = int(val)
            elif isinstance(val, str):
                text = val.strip()
                if text.isdigit():
                    day = int(text)
            if day is not None and 1 <= day <= 31 and day not in mapped:
                mapped[day] = col
        return mapped

    first_map: dict[int, int] = {}
    second_map: dict[int, int] = {}
    for row_idx in range(1, min(60, ws.max_row) + 1):
        mapped = _day_map_from_row(row_idx)
        if len(mapped) < 8:
            continue
        if any(day <= 15 for day in mapped):
            first_map = mapped if len(mapped) > len(first_map) else first_map
        if any(day >= 16 for day in mapped):
            second_map = mapped if len(mapped) > len(second_map) else second_map
    return first_map, second_map


def _find_t13_first_employee_row(ws) -> int:
    for row in range(1, min(120, ws.max_row) + 1):
        number = str(ws.cell(row=row, column=2).value or "").strip()
        fio = str(ws.cell(row=row, column=3).value or "").strip()
        tab_num = str(ws.cell(row=row, column=5).value or "").strip()
        if number.isdigit() and "(" in fio and tab_num:
            return row
    return 22


def _find_t13_footer_row(ws, start_scan_row: int) -> Optional[int]:
    """
    Ищет начало служебной части бланка Т-13 (подписи/реквизиты организации),
    ниже которой нельзя размещать сотрудников.
    """
    markers = (
        "ответствен",
        "наименование организац",
        "утверждена",
        "постановлен",
        "общество с ограниченной ответственностью",
    )

    max_scan_row = ws.max_row
    max_scan_col = min(ws.max_column, 30)
    for row in range(max(start_scan_row, 1), max_scan_row + 1):
        row_text_parts: list[str] = []
        for col in range(1, max_scan_col + 1):
            value = ws.cell(row=row, column=col).value
            if value is None:
                continue
            text = str(value).strip().lower()
            text = re.sub(r"\s+", " ", text)
            if text:
                row_text_parts.append(text)
        if not row_text_parts:
            continue
        row_text = " ".join(row_text_parts)
        if any(marker in row_text for marker in markers):
            return row
    return None


def _fill_t13_template_sheet(
    ws,
    result: ScheduleResult,
    days: list[int],
    restaurant_codes: dict[str, str],
    weekend_days: Optional[set[int]] = None,
) -> None:
    sorted_days = sorted({int(day) for day in days})
    weekend_days_set = set(int(day) for day in (weekend_days or set()))
    first_half_map, second_half_map = _find_t13_day_columns(ws)
    start_row = _find_t13_first_employee_row(ws)
    block_height = 4

    def _writable(row: int, col: int) -> bool:
        return ws.cell(row=row, column=col).__class__.__name__ != "MergedCell"

    footer_row = _find_t13_footer_row(ws, start_scan_row=start_row)
    max_data_row = (footer_row - 1) if footer_row else ws.max_row

    def _block_has_valid_day_grid(block_start: int) -> bool:
        # Для корректного заполнения блок сотрудника должен иметь
        # полностью доступные ячейки сетки дней в двух половинах месяца.
        for _day, col in first_half_map.items():
            if not (_writable(block_start, col) and _writable(block_start + 1, col)):
                return False
        for _day, col in second_half_map.items():
            if not (_writable(block_start + 2, col) and _writable(block_start + 3, col)):
                return False
        return True

    block_rows: list[int] = []
    probe = start_row
    while probe <= max_data_row - 3:
        if (
            _writable(probe, 2)
            and _writable(probe, 3)
            and _writable(probe, 5)
            and _block_has_valid_day_grid(probe)
        ):
            block_rows.append(probe)
            probe += block_height
        else:
            probe += 1

    assignments = result.assignments.copy()
    if assignments.empty:
        assignments = pd.DataFrame(columns=["employee", "day", "restaurant", "deficit"])
    if "deficit" not in assignments.columns:
        assignments["deficit"] = False
    assignments = assignments[(~assignments["deficit"]) & (assignments["employee"] != DEFICIT_EMPLOYEE_LABEL)].copy()
    assignments["day"] = pd.to_numeric(assignments.get("day"), errors="coerce").fillna(0).astype(int)
    assignments["restaurant"] = assignments.get("restaurant", "").astype(str)

    details_map: dict[tuple[str, int], list[str]] = defaultdict(list)
    for _, row in assignments.iterrows():
        key = (str(row["employee"]), int(row["day"]))
        rest_name = str(row["restaurant"])
        details_map[key].append(rest_name)

    employee_base = (
        result.employee_summary[["employee", "restaurant", "role", "role_group", "max_days", "max_hours"]]
        .drop_duplicates()
        .sort_values(["restaurant", "role", "employee"])
        .reset_index(drop=True)
    )

    max_blocks = len(block_rows)
    fill_count = min(len(employee_base), max_blocks)

    for r in block_rows:
        for col in [2, 3, 5]:
            if _writable(r, col):
                ws.cell(row=r, column=col, value=None)
        mapped_cols = list({**first_half_map, **second_half_map}.values())
        if mapped_cols:
            day_grid_start = max(1, min(mapped_cols))
            day_grid_end = min(ws.max_column, max(mapped_cols))
        else:
            day_grid_start, day_grid_end = 9, ws.max_column

        for row_idx in (r, r + 1, r + 2, r + 3):
            for col in range(day_grid_start, day_grid_end + 1):
                if _writable(row_idx, col):
                    cell = ws.cell(row=row_idx, column=col)
                    cell.value = None
                    cell.comment = None

    for idx in range(fill_count):
        rec = employee_base.iloc[idx]
        employee = str(rec["employee"])
        restaurant = str(rec["restaurant"])
        role = str(rec["role"])
        role_group = str(rec["role_group"])
        payroll_days_target = int(pd.to_numeric(rec["max_days"], errors="coerce") or 0)
        payroll_hours_target = float(pd.to_numeric(rec["max_hours"], errors="coerce") or 0.0)

        r = block_rows[idx]
        if _writable(r, 2):
            ws.cell(row=r, column=2, value=str(idx + 1))
        if _writable(r, 3):
            ws.cell(row=r, column=3, value=f"{employee}\n({role})")
        if _writable(r, 5):
            ws.cell(row=r, column=5, value="")

        factual_days = sorted({day for (emp_key, day) in details_map.keys() if emp_key == employee})
        selected_days = _select_employee_days(
            factual_days=factual_days,
            all_days=sorted_days,
            target_count=payroll_days_target,
            prefer_weekends=_is_core_group(role_group),
            weekend_days=weekend_days_set,
        )

        day_hours = _distribute_hours(payroll_hours_target, len(selected_days))
        hours_map = {day: day_hours[pos] for pos, day in enumerate(selected_days)}

        for day in sorted_days:
            if day <= 15:
                col = first_half_map.get(day)
                code_row, hours_row = r, r + 1
            else:
                col = second_half_map.get(day)
                code_row, hours_row = r + 2, r + 3
            if col is None:
                continue
            code_col = col if _writable(code_row, col) else None
            hours_col = col if _writable(hours_row, col) else None

            if day not in hours_map:
                if code_col is not None:
                    code_cell = ws.cell(row=code_row, column=code_col)
                    code_cell.value = "В"
                    code_cell.comment = None
                if hours_col is not None:
                    ws.cell(row=hours_row, column=hours_col, value=None)
                continue

            key = (employee, day)
            worked_restaurants = sorted(set(details_map.get(key, [])))
            if code_col is not None:
                code_cell = ws.cell(row=code_row, column=code_col)
                if worked_restaurants:
                    code_cell.value = "Я"
                    if any(rest != restaurant for rest in worked_restaurants):
                        note = "Межресторанная подмена: " + ", ".join(worked_restaurants)
                        code_cell.comment = Comment(note, "tabel")
                    else:
                        code_cell.comment = None
                else:
                    code_cell.value = "ПЛ"
                    code_cell.comment = None
            if hours_col is not None:
                ws.cell(row=hours_row, column=hours_col, value=round(float(hours_map[day]), 2))

    ws.cell(row=2, column=1, value=f"Сформировано: {datetime.now().strftime('%d.%m.%Y %H:%M')}")


def export_t13_to_excel(
    result: ScheduleResult,
    days: list[int],
    output_path: Path,
    template_bytes: Optional[bytes] = None,
    weekend_days: Optional[set[int]] = None,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if template_bytes:
        from openpyxl import load_workbook

        template_wb = load_workbook(BytesIO(template_bytes))
        ws = template_wb[template_wb.sheetnames[0]]

        all_restaurants = sorted(result.employee_summary["restaurant"].astype(str).unique().tolist())
        restaurant_codes = {name: f"R{idx:02d}" for idx, name in enumerate(all_restaurants, start=1)}
        _fill_t13_template_sheet(
            ws=ws,
            result=result,
            days=days,
            restaurant_codes=restaurant_codes,
            weekend_days=weekend_days,
        )
        template_wb.save(output_path)
        return

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
