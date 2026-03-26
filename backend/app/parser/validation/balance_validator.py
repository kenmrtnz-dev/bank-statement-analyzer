"""Balance validation helpers."""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Sequence

logger = logging.getLogger(__name__)
BALANCE_TOLERANCE = Decimal("0.01")


def _to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    text = str(value).strip()
    if not text:
        return None
    cleaned = text.replace(",", "")
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def evaluate_ordered_balance_rows(
    items: Sequence[dict[str, Any]],
    *,
    balance_logger: logging.Logger | None = None,
) -> list[dict[str, Any]]:
    """Evaluate disbalance fields for rows that are already in statement order."""
    active_logger = balance_logger or logger
    results: list[dict[str, Any]] = []
    previous_balance: Decimal | None = None

    for fallback_index, item in enumerate(items, start=1):
        current_balance = _to_decimal(item.get("balance"))
        debit = _to_decimal(item.get("debit"))
        credit = _to_decimal(item.get("credit"))
        has_flow = debit is not None or credit is not None
        result = {
            "checked": False,
            "is_disbalanced": False,
            "expected_balance": None,
            "difference": None,
        }

        if previous_balance is not None and current_balance is not None and has_flow:
            debit_value = abs(debit) if debit is not None else Decimal("0")
            credit_value = abs(credit) if credit is not None else Decimal("0")
            expected_balance = previous_balance - debit_value + credit_value
            difference = current_balance - expected_balance
            row_index = item.get("row_index")
            if row_index is None:
                row_index = item.get("row_number")
            if row_index is None:
                row_index = fallback_index
            active_logger.debug(
                "Balance check page_number=%s row_index=%s previous_balance=%s debit=%s credit=%s "
                "expected_balance=%s actual_balance=%s difference=%s",
                item.get("page_number"),
                row_index,
                previous_balance,
                debit,
                credit,
                expected_balance,
                current_balance,
                difference,
            )
            result = {
                "checked": True,
                "is_disbalanced": abs(difference) > BALANCE_TOLERANCE,
                "expected_balance": expected_balance,
                "difference": difference,
            }

        results.append(result)

        # Balance-only rows still act as anchors for the next transaction.
        if current_balance is not None:
            previous_balance = current_balance

    return results


def validate_balances(items: list[dict]) -> dict:
    """Run a running-balance check across parsed transactions in statement order."""
    checked_rows = 0
    mismatches: list[int] = []
    results = evaluate_ordered_balance_rows(items, balance_logger=logger)

    for fallback_index, (item, result) in enumerate(zip(items, results), start=1):
        if result["checked"]:
            checked_rows += 1
        if not result["is_disbalanced"]:
            continue
        row_number = item.get("row_number")
        if row_number is None:
            row_number = item.get("row_index")
        if row_number is None:
            row_number = fallback_index
        mismatches.append(int(row_number))

    return {
        "checked_rows": checked_rows,
        "mismatch_rows": mismatches,
        "is_valid": not mismatches,
    }
