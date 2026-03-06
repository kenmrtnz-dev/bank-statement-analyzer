"""Transaction extraction helpers."""


def _is_header_row(row: dict) -> bool:
    """Return True when a row looks like a table header."""
    date_cell = (row.get("date") or "").lower()
    description_cell = (row.get("description") or "").lower()
    return "date" in date_cell and "description" in description_cell


def extract_transactions(rows: list[dict]) -> list[dict]:
    """Convert raw table rows into canonical transaction dictionaries."""
    transactions: list[dict] = []

    for row in rows:
        if _is_header_row(row):
            continue

        if not any((row.get(key) or "").strip() for key in ("date", "description", "debit", "credit", "balance")):
            continue

        transactions.append(
            {
                "row_number": len(transactions) + 1,
                "date": row.get("date") or None,
                "description": (row.get("description") or "").strip(),
                "debit": row.get("debit") or None,
                "credit": row.get("credit") or None,
                "balance": row.get("balance") or None,
            }
        )

    # TODO: Add bank-specific parsing rules for multi-line dates and split amount columns.
    return transactions
