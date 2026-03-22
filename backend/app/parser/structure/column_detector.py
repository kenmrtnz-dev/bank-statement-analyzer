"""Column slicing helpers."""


def detect_columns(template: dict, normalized_width: int = 750, output_width: int = 100) -> dict[str, tuple[int, int]]:
    """Convert template coordinate ranges into approximate text slices."""
    columns = template.get("columns", {})
    # Templates are defined in a normalized coordinate system so they are easy
    # to tweak manually without tying them to a specific OCR engine output size.
    scale = output_width / normalized_width
    detected: dict[str, tuple[int, int]] = {}
    for key, bounds in columns.items():
        start = max(0, int(bounds[0] * scale))
        end = max(start + 1, int(bounds[1] * scale))
        detected[key] = (start, end)
    return detected
