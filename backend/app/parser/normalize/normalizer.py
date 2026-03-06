"""Normalization helpers for parsed transactions."""

from datetime import datetime
from decimal import Decimal, InvalidOperation

DATE_FORMATS = (
    "%m/%d/%Y",
    "%m/%d/%y",
    "%m-%d-%Y",
    "%m-%d-%y",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%d-%m-%y",
    "%Y-%m-%d",
    "%b %d %Y",
    "%b %d, %Y",
)


def _parse_date(value: str | None):
    """Parse a transaction date using common formats."""
    if not value:
        return None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value.strip(), fmt).date()
        except ValueError:
            continue
    return None


def _parse_decimal(value: str | None):
    """Parse a decimal amount from a bank statement string."""
    if not value:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None
    cleaned = value.replace(",", "").replace("$", "").replace("PHP", "").strip()
    if not cleaned:
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def normalize_transactions(items: list[dict]) -> list[dict]:
    """Normalize parsed transaction fields into database-ready values."""
    normalized: list[dict] = []
    for item in items:
        normalized.append(
            {
                "row_number": item["row_number"],
                "date": _parse_date(item.get("date")),
                "description": (item.get("description") or "").strip(),
                "debit": _parse_decimal(item.get("debit")),
                "credit": _parse_decimal(item.get("credit")),
                "balance": _parse_decimal(item.get("balance")),
            }
        )
    return normalized
