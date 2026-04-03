from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from collections import defaultdict
from typing import Any, Optional
from datetime import date
import calendar
import math
import re

import pandas as pd

ROLE_GROUP_KITCHEN = "Кухня"
ROLE_GROUP_HALL = "Зал"
ROLE_GROUP_CASH = "Касса"
ROLE_GROUP_BAR = "Бар"
ROLE_GROUP_SERVICE = "Обслуживание"
ROLE_GROUP_CHOICES = [
    ROLE_GROUP_KITCHEN,
    ROLE_GROUP_HALL,
    ROLE_GROUP_CASH,
    ROLE_GROUP_BAR,
    ROLE_GROUP_SERVICE,
]


@dataclass
class PreparedInput:
    employees: pd.DataFrame
    days: list[int]
    weekend_days: list[int]
    period_year: Optional[int]
    period_month: Optional[int]
    role_group_defaults: dict[str, str]
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


def _normalize_restaurant(value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""

    # Унификация адресов: одно и то же место с разными "пом./помещение" считаем одним рестораном.
    # Пример: "пр-т циолковского, 19 а пом.1" и "пр-т циолковского, 19 а пом.3".
    text = re.sub(r"[, ]+\bпом(?:\.|ещение)?\s*[\w-]+\b", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text).strip(" ,.-")
    return text


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


def _extract_tab_number(value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    match = re.search(r"\(\s*(\d+)\s*\)\s*$", text)
    if not match:
        return ""
    return str(match.group(1)).strip()


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

    # Явные пользовательские дефолты (приоритет над общими правилами).
    if "бар-менеджер" in role or "бар менеджер" in role:
        return ROLE_GROUP_BAR
    if "директор" in role:
        return ROLE_GROUP_SERVICE
    if "управляющий по производству и учету" in role:
        return ROLE_GROUP_KITCHEN
    if "подсобный рабочий" in role:
        return ROLE_GROUP_KITCHEN

    admin_tokens = (
        "администратор",
        "адмистратор",
        "кассир",
        "менеджер",
    )
    kitchen_tokens = (
        "повар",
        "шеф",
        "су-шеф",
        "сушеф",
        "су шеф",
        "шеф-повар",
        "шеф повар",
        "подсоб",
        "управляющ по производству",
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
        return ROLE_GROUP_CASH

    if any(token in role for token in kitchen_tokens):
        return ROLE_GROUP_KITCHEN

    if any(token in role for token in bar_tokens):
        return ROLE_GROUP_BAR

    if any(token in role for token in hall_tokens):
        return ROLE_GROUP_HALL

    return ROLE_GROUP_SERVICE


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
    result["restaurant"] = result["restaurant"].map(_normalize_restaurant)
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
        current_tab_number = ""
        current_restaurant = "не указан"
        current_role = "не указана"
        block_days: list[float] = []
        block_hours: list[float] = []
        # Приоритетные значения: фактически оплаченные/отработанные за месяц.
        block_days_priority: list[float] = []
        block_hours_priority: list[float] = []
        first_half_pay: float = 0.0
        second_half_pay: float = 0.0
        detected_days_col: Optional[int] = None
        detected_hours_col: Optional[int] = None

        def _extract_last_money_from_row(values: list[Any]) -> Optional[float]:
            # Сначала берем реальные числовые ячейки (обычно колонка "Сумма").
            typed_values: list[float] = []
            for cell in values:
                if cell is None or pd.isna(cell):
                    continue
                if isinstance(cell, (int, float)):
                    value = float(cell)
                    if value >= 100:
                        typed_values.append(value)
            if typed_values:
                return typed_values[-1]

            # Фолбэк: парсим строковые значения, игнорируя номера ведомостей "№...".
            parsed_values: list[float] = []
            for cell in values:
                if cell is None or pd.isna(cell):
                    continue
                raw = str(cell)
                if "№" in raw:
                    continue
                text = raw.replace("\u00a0", " ").replace(" ", "").replace(",", ".")
                for token in re.findall(r"-?\d+(?:\.\d+)?", text):
                    try:
                        value = float(token)
                    except ValueError:
                        continue
                    if value >= 100:
                        parsed_values.append(value)
            if not parsed_values:
                return None
            return parsed_values[-1]

        def flush_block() -> None:
            nonlocal block_days
            nonlocal block_hours
            nonlocal block_days_priority
            nonlocal block_hours_priority
            nonlocal current_employee
            nonlocal current_tab_number
            nonlocal current_restaurant
            nonlocal current_role
            nonlocal first_half_pay
            nonlocal second_half_pay
            if not current_employee:
                block_days = []
                block_hours = []
                block_days_priority = []
                block_hours_priority = []
                first_half_pay = 0.0
                second_half_pay = 0.0
                return

            source_days = block_days_priority if block_days_priority else block_days
            source_hours = block_hours_priority if block_hours_priority else block_hours
            max_days = max(source_days) if source_days else 0.0
            max_hours = max(source_hours) if source_hours else 0.0

            half_preference = "neutral"
            if first_half_pay > 0 and second_half_pay > 0:
                if first_half_pay > second_half_pay * 1.05:
                    half_preference = "first"
                elif second_half_pay > first_half_pay * 1.05:
                    half_preference = "second"
            elif first_half_pay > 0 and second_half_pay <= 0:
                half_preference = "first"
            elif second_half_pay > 0 and first_half_pay <= 0:
                half_preference = "second"

            if max_days <= 0 and max_hours > 0:
                max_days = math.ceil(max_hours / 8.0)
            if max_hours <= 0 and max_days > 0:
                max_hours = max_days * 8.0

            if max_days > 0 and max_hours > 0:
                rows.append(
                    {
                        "employee": current_employee,
                        "payroll_hours": float(max_hours),
                        "payroll_days": float(max_days),
                        "max_hours": float(max_hours),
                        "max_days": int(round(max_days)),
                        "restaurant": _normalize_restaurant(current_restaurant) or "не указан",
                        "role": _normalize_text(current_role) or "не указана",
                        "tab_number": str(current_tab_number or ""),
                        "first_half_pay": float(first_half_pay),
                        "second_half_pay": float(second_half_pay),
                        "half_preference": half_preference,
                    }
                )

            block_days = []
            block_hours = []
            block_days_priority = []
            block_hours_priority = []
            first_half_pay = 0.0
            second_half_pay = 0.0

        for row_idx in range(len(df)):
            row = df.iloc[row_idx]
            col0 = row.iloc[0] if len(row) > 0 else None
            col0_text = _normalize_text(col0)

            # Начало нового блока сотрудника: "ФИО (00123)"
            if col0_text and re.search(r"\(\s*\d+\s*\)\s*$", col0_text):
                flush_block()
                current_employee = _clean_employee_name(col0_text)
                current_tab_number = _extract_tab_number(col0_text)
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

            # Определяем реальные колонки "Дни/Часы" по заголовку строки.
            if detected_days_col is None or detected_hours_col is None:
                normalized_row = [_normalize_text(v) for v in row_values]
                day_candidate: Optional[int] = None
                hour_candidate: Optional[int] = None
                for col_idx, token in enumerate(normalized_row):
                    if not token:
                        continue
                    if day_candidate is None and ("дни" in token or token == "дн"):
                        day_candidate = col_idx
                    if hour_candidate is None and ("часы" in token or "час" in token):
                        hour_candidate = col_idx
                if day_candidate is not None and hour_candidate is not None:
                    detected_days_col = day_candidate
                    detected_hours_col = hour_candidate

            # Fallback для нестандартной верстки расчетного листка.
            days_col = detected_days_col if detected_days_col is not None else 10
            hours_col = detected_hours_col if detected_hours_col is not None else 12
            days_val = _extract_number(row.iloc[days_col] if len(row) > days_col else None)
            hours_val = _extract_number(row.iloc[hours_col] if len(row) > hours_col else None)
            normalized_row = [_normalize_text(v) for v in row_values]
            row_text = " ".join(token for token in normalized_row if token)

            if "за первую половину" in row_text:
                amount = _extract_last_money_from_row(row_values)
                if amount:
                    first_half_pay = max(first_half_pay, amount)
            elif ("за вторую половину" in row_text) or ("зарплата (банк" in row_text):
                amount = _extract_last_money_from_row(row_values)
                if amount:
                    second_half_pay = max(second_half_pay, amount)

            if days_val is None and hours_val is None:
                continue

            if "оклад (тариф)" in row_text or "оклад(тариф)" in row_text:
                continue

            is_relevant_line = any(
                token in row_text
                for token in (
                    "оплата",
                    "оклад",
                    "тариф",
                    "час",
                    "смен",
                    "рабочие",
                )
            )
            # Иногда col0 может быть пустым — тогда берём любые адекватные численные значения
            if not is_relevant_line and col0_text:
                continue

            is_priority_line = any(
                token in row_text
                for token in (
                    "оплата по окладу",
                    "оплата",
                    "начислено",
                    "отработано",
                    "рабочие",
                )
            ) and ("норма" not in row_text)
            is_norm_only_line = ("норма" in row_text) and (not is_priority_line)
            if is_norm_only_line:
                continue

            if days_val is not None and 0 < days_val <= 31:
                block_days.append(days_val)
                if is_priority_line:
                    block_days_priority.append(days_val)
            if hours_val is not None and 0 < hours_val <= 400:
                block_hours.append(hours_val)
                if is_priority_line:
                    block_hours_priority.append(hours_val)

        flush_block()

    if not rows:
        raise ValueError("Не удалось извлечь часы/дни из расчетных листков.")

    result = pd.DataFrame(rows)
    result = (
        result.groupby("employee", as_index=False)
        .agg(
            payroll_hours=("payroll_hours", "max"),
            payroll_days=("payroll_days", "max"),
            max_hours=("max_hours", "max"),
            max_days=("max_days", "max"),
            restaurant=("restaurant", lambda s: _pick_text_or_default(s, "не указан")),
            role=("role", lambda s: _pick_text_or_default(s, "не указана")),
            tab_number=("tab_number", lambda s: _pick_text_or_default(s, "")),
            first_half_pay=("first_half_pay", "max"),
            second_half_pay=("second_half_pay", "max"),
            half_preference=(
                "half_preference",
                lambda s: "first"
                if "first" in set(s)
                else ("second" if "second" in set(s) else "neutral"),
            ),
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
        result["restaurant"] = df[optional_mapping["restaurant"]].map(_normalize_restaurant)
    else:
        result["restaurant"] = "не указан"

    if "role" in optional_mapping:
        result["role"] = df[optional_mapping["role"]].map(_normalize_text)
    else:
        result["role"] = "не указана"
    result["tab_number"] = ""

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
            tab_number=("tab_number", lambda s: _pick_text_or_default(s, "")),
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


MONTH_NAME_TO_NUM = {
    "январ": 1,
    "феврал": 2,
    "март": 3,
    "апрел": 4,
    "ма": 5,  # май/мая
    "июн": 6,
    "июл": 7,
    "август": 8,
    "сентябр": 9,
    "октябр": 10,
    "ноябр": 11,
    "декабр": 12,
}


def _extract_month_year_from_text(text: str) -> Optional[tuple[int, int]]:
    normalized = _normalize_text(text)
    if not normalized:
        return None

    # Пример: "01.2025" / "1-2025"
    month_year = re.search(r"\b(0?[1-9]|1[0-2])[./-](20\d{2})\b", normalized)
    if month_year:
        return int(month_year.group(2)), int(month_year.group(1))

    # Пример: "за январь 2025"
    year_match = re.search(r"\b(20\d{2})(?:\s*г(?:\.|ода)?)?\b", normalized)
    if year_match:
        year = int(year_match.group(1))
        for token, month in MONTH_NAME_TO_NUM.items():
            if token in normalized:
                return year, month

    return None


def _detect_payroll_period(file_bytes: bytes, filename: Optional[str] = None) -> Optional[tuple[int, int]]:
    votes: dict[tuple[int, int], int] = defaultdict(int)

    if filename:
        pair = _extract_month_year_from_text(filename)
        if pair:
            votes[pair] += 3

    workbook = pd.read_excel(BytesIO(file_bytes), sheet_name=None, header=None, dtype=object)
    for _, df in workbook.items():
        if df.empty:
            continue

        # Смотрим первые строки, где обычно есть шапка периода.
        sample_df = df.head(40)
        for value in sample_df.to_numpy().flatten():
            if value is None or pd.isna(value):
                continue

            if isinstance(value, pd.Timestamp):
                votes[(int(value.year), int(value.month))] += 2
                continue

            if hasattr(value, "year") and hasattr(value, "month"):
                try:
                    votes[(int(value.year), int(value.month))] += 2
                    continue
                except Exception:
                    pass

            text = str(value).strip()
            if not text:
                continue

            # Полная дата: 31.01.2025
            full_date = re.search(r"\b([0-3]?\d)[./-](0?[1-9]|1[0-2])[./-](20\d{2})\b", text)
            if full_date:
                votes[(int(full_date.group(3)), int(full_date.group(2)))] += 2
                continue

            pair = _extract_month_year_from_text(text)
            if pair:
                votes[pair] += 1

    if not votes:
        return None
    return max(votes.items(), key=lambda item: item[1])[0]


def _russian_fixed_holidays(year: int) -> set[date]:
    holidays: set[date] = set()

    # Федеральные праздничные дни (фиксированные даты).
    for day in range(1, 9):
        holidays.add(date(year, 1, day))
    holidays.add(date(year, 2, 23))
    holidays.add(date(year, 3, 8))
    holidays.add(date(year, 5, 1))
    holidays.add(date(year, 5, 9))
    holidays.add(date(year, 6, 12))
    holidays.add(date(year, 11, 4))

    return holidays


def _russian_holidays_with_observed(year: int) -> set[date]:
    """
    Возвращает праздничные/нерабочие дни РФ с учетом переносов выходных
    (observed), если доступна библиотека holidays.
    """
    try:
        import holidays  # type: ignore
    except Exception:
        return _russian_fixed_holidays(year)

    result: set[date] = set()
    try:
        ru_holidays = holidays.country_holidays("RU", years=[year], observed=True)
        for dt in ru_holidays.keys():
            if isinstance(dt, date):
                result.add(dt)
    except Exception:
        return _russian_fixed_holidays(year)

    if not result:
        return _russian_fixed_holidays(year)
    return result


def parse_calendar_from_payroll(
    payroll_df: pd.DataFrame,
    payroll_bytes: Optional[bytes] = None,
    payroll_filename: Optional[str] = None,
) -> tuple[list[int], list[int]]:
    del payroll_df  # календарь читается из шапки файла и периода, а не из агрегированных строк.

    detected_period = _detect_payroll_period(payroll_bytes, payroll_filename) if payroll_bytes else None
    if detected_period is None:
        # Fallback: если период не нашли, оставляем прежнее поведение.
        days = list(range(1, 32))
        weekend_days = [day for day in days if day % 7 in (6, 0)]
        return days, weekend_days

    year, month = detected_period
    month_days = calendar.monthrange(year, month)[1]
    days = list(range(1, month_days + 1))

    holidays_with_observed = _russian_holidays_with_observed(year)
    weekend_days = []
    for day in days:
        current = date(year, month, day)
        is_weekend = current.weekday() >= 5
        is_holiday = current in holidays_with_observed
        if is_weekend or is_holiday:
            weekend_days.append(day)

    return days, sorted(set(weekend_days))


def prepare_input(
    payroll_bytes: bytes,
    payroll_filename: Optional[str] = None,
    employees_bytes: Optional[bytes] = None,
) -> PreparedInput:
    payroll_df = parse_payroll(payroll_bytes)
    detected_period = _detect_payroll_period(payroll_bytes, payroll_filename)
    days, weekend_days = parse_calendar_from_payroll(
        payroll_df=payroll_df,
        payroll_bytes=payroll_bytes,
        payroll_filename=payroll_filename,
    )

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

    merged["restaurant"] = merged["restaurant"].fillna("не указан").map(_normalize_restaurant)
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
    if "tab_number" not in merged.columns:
        merged["tab_number"] = ""
    merged["tab_number"] = merged["tab_number"].fillna("").astype(str).str.strip()
    merged["role_original"] = merged["role"]
    merged["role_group"] = merged["role_original"].map(_map_role_group)

    if "first_half_pay" not in merged.columns:
        merged["first_half_pay"] = 0.0
    if "second_half_pay" not in merged.columns:
        merged["second_half_pay"] = 0.0
    if "half_preference" not in merged.columns:
        merged["half_preference"] = "neutral"

    merged = merged[
        [
            "employee",
            "restaurant",
            "tab_number",
            "role_original",
            "role_group",
            "max_hours",
            "max_days",
            "first_half_pay",
            "second_half_pay",
            "half_preference",
        ]
    ].copy()
    merged["max_hours"] = merged["max_hours"].astype(float)
    merged["max_days"] = merged["max_days"].astype(int)
    merged["first_half_pay"] = pd.to_numeric(merged["first_half_pay"], errors="coerce").fillna(0.0).astype(float)
    merged["second_half_pay"] = pd.to_numeric(merged["second_half_pay"], errors="coerce").fillna(0.0).astype(float)
    merged["half_preference"] = merged["half_preference"].astype(str)

    merged = merged.sort_values(["restaurant", "role_group", "employee"]).reset_index(drop=True)

    role_group_defaults = (
        merged[["role_original", "role_group"]]
        .drop_duplicates()
        .sort_values(["role_original"])
        .set_index("role_original")["role_group"]
        .to_dict()
    )

    summary = {
        "employee_count": int(len(merged)),
        "restaurants": int(merged["restaurant"].nunique()),
        "roles": int(merged["role_original"].nunique()),
        "role_groups": int(merged["role_group"].nunique()),
        "available_role_groups": ROLE_GROUP_CHOICES,
        "days_in_payroll": int(len(days)),
        "weekend_days_in_payroll": int(len(weekend_days)),
        # Backward compatibility for old frontend keys.
        "days_in_template": int(len(days)),
        "weekend_days_in_template": int(len(weekend_days)),
    }

    return PreparedInput(
        employees=merged,
        days=days,
        weekend_days=weekend_days,
        period_year=detected_period[0] if detected_period else None,
        period_month=detected_period[1] if detected_period else None,
        role_group_defaults=role_group_defaults,
        warnings=warnings,
        summary=summary,
    )
