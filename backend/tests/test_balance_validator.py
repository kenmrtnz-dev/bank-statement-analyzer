import logging
from decimal import Decimal

from app.parser.validation import balance_validator


def test_validate_balances_uses_balance_only_rows_as_anchors_and_logs_details(caplog):
    caplog.set_level(logging.DEBUG, logger="app.parser.validation.balance_validator")
    validation = balance_validator.validate_balances(
        [
            {
                "page_number": 1,
                "row_number": 1,
                "balance": Decimal("100.00"),
                "debit": None,
                "credit": None,
            },
            {
                "page_number": 1,
                "row_number": 2,
                "balance": Decimal("90.00"),
                "debit": Decimal("10.00"),
                "credit": None,
            },
            {
                "page_number": 1,
                "row_number": 3,
                "balance": Decimal("90.01"),
                "debit": Decimal("0.00"),
                "credit": None,
            },
        ]
    )

    assert validation == {
        "checked_rows": 2,
        "mismatch_rows": [],
        "is_valid": True,
    }
    assert any(
        "page_number=1" in record.message
        and "row_index=2" in record.message
        and "previous_balance=100.00" in record.message
        and "debit=10.00" in record.message
        and "credit=None" in record.message
        and "expected_balance=90.00" in record.message
        and "actual_balance=90.00" in record.message
        and "difference=0.00" in record.message
        for record in caplog.records
    )


def test_validate_balances_flags_rows_beyond_tolerance():
    validation = balance_validator.validate_balances(
        [
            {
                "page_number": 1,
                "row_number": 1,
                "balance": Decimal("100.00"),
                "debit": None,
                "credit": None,
            },
            {
                "page_number": 1,
                "row_number": 2,
                "balance": Decimal("89.98"),
                "debit": Decimal("10.00"),
                "credit": None,
            },
        ]
    )

    assert validation == {
        "checked_rows": 1,
        "mismatch_rows": [2],
        "is_valid": False,
    }
