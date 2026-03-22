"""Table-building helpers."""


def build_table(rows: list[str], column_map: dict[str, tuple[int, int]]) -> list[dict]:
    """Slice detected rows into column-based dictionaries."""
    max_end = max((end for _, end in column_map.values()), default=0)
    table: list[dict] = []
    for raw_row in rows:
        padded = raw_row.ljust(max_end)
        table.append(
            {
                key: padded[start:end].strip()
                for key, (start, end) in column_map.items()
            }
        )
    return table
