"""Helpers for combining wrapped descriptions."""


def merge_descriptions(rows: list[dict]) -> list[dict]:
    """Merge continuation rows into the previous transaction description."""
    merged: list[dict] = []
    for row in rows:
        has_date = bool((row.get("date") or "").strip())
        description = (row.get("description") or "").strip()

        # Continuation rows often omit the date and only carry extra description
        # text, so fold them into the previous row before extraction.
        if not has_date and description and merged:
            previous = merged[-1]
            previous_description = previous.get("description") or ""
            previous["description"] = f"{previous_description} {description}".strip()
            continue

        merged.append(dict(row))
    return merged
