"""Balance validation helpers."""

from decimal import Decimal


def validate_balances(items: list[dict]) -> dict:
    """Run a simple running-balance check across parsed transactions."""
    mismatches: list[int] = []

    for index in range(1, len(items)):
        previous = items[index - 1]
        current = items[index]

        if previous.get("balance") is None or current.get("balance") is None:
            continue

        debit = current.get("debit") or Decimal("0")
        credit = current.get("credit") or Decimal("0")
        expected = previous["balance"] - debit + credit

        if expected.quantize(Decimal("0.01")) != current["balance"].quantize(Decimal("0.01")):
            mismatches.append(current["row_number"])

    return {
        "checked_rows": max(0, len(items) - 1),
        "mismatch_rows": mismatches,
        "is_valid": not mismatches,
    }
