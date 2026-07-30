import unittest
from datetime import date

import pandas as pd

from app.scheduler import _EmployeeState, _select_employee_days
from app.work_rules import (
    employment_allowed_days,
    find_unique_name_match,
    names_compatible,
)


class EmploymentConstraintTests(unittest.TestCase):
    def test_full_name_matches_payroll_initials(self):
        self.assertTrue(
            names_compatible("Сотов И. В.", "Сотов Иван Вячеславович")
        )
        self.assertFalse(
            names_compatible("Сотов П. В.", "Сотов Иван Вячеславович")
        )

    def test_initials_match_must_be_unique(self):
        personnel = {
            "сотов иван вячеславович": [(date(2024, 11, 5), None)],
            "сотов петр викторович": [(date(2024, 1, 1), None)],
        }

        self.assertEqual(
            find_unique_name_match(personnel, "Сотов И. В."),
            [(date(2024, 11, 5), None)],
        )
        self.assertIsNone(find_unique_name_match(personnel, "Сотов В. В."))

    def test_hire_date_limits_allowed_days(self):
        allowed = employment_allowed_days(
            [(date(2024, 11, 5), None)],
            year=2024,
            month=11,
        )

        self.assertNotIn(1, allowed)
        self.assertNotIn(4, allowed)
        self.assertIn(5, allowed)
        self.assertIn(30, allowed)

    def test_manual_timesheet_cannot_restore_day_before_hire(self):
        selected = _select_employee_days(
            factual_days=[1, 6, 10],
            all_days=list(range(1, 31)),
            target_count=5,
            prefer_weekends=False,
            weekend_days=set(),
            allowed_days=set(range(5, 31)),
            fixed_days={1, 6},
        )

        self.assertEqual(len(selected), 5)
        self.assertIn(6, selected)
        self.assertNotIn(1, selected)
        self.assertTrue(all(day >= 5 for day in selected))

    def test_employee_state_filters_fixed_hours_outside_employment(self):
        state = _EmployeeState(
            pd.Series(
                {
                    "employee": "Сотов Иван Вячеславович",
                    "restaurant": "Тестовый ресторан",
                    "tab_number": "001",
                    "role_original": "Официант",
                    "role_group": "Зал",
                    "max_hours": 16,
                    "max_days": 2,
                    "allowed_days": [5, 6],
                    "fixed_work_hours": {1: 8, 5: 8},
                }
            )
        )

        self.assertEqual(state.fixed_work_hours, {5: 8.0})


if __name__ == "__main__":
    unittest.main()
