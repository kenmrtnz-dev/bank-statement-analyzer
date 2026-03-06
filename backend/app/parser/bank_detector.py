"""Simple bank detection heuristics."""


def detect_bank(text: str) -> str:
    """Return the most likely bank template name for the extracted text."""
    normalized = text.lower()
    if "sterling bank of asia" in normalized:
        return "sterling_bank_of_asia"
    if all(keyword in normalized for keyword in ("ln", "date", "withdrawal", "bal", "tc")):
        return "sterling_bank_of_asia"

    keyword_map = {
        "bdo": "bdo",
        "banco de oro": "bdo",
        "metrobank": "metrobank",
        "metropolitan bank": "metrobank",
        "bpi": "bpi",
        "bank of the philippine islands": "bpi",
    }
    for keyword, bank in keyword_map.items():
        if keyword in normalized:
            return bank
    return "generic"
