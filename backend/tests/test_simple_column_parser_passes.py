from app.parser.simple_column_parser import parse_transactions_from_ocr_raw


def _annotation(text: str, x1: int, y1: int, x2: int, y2: int) -> dict:
    return {
        "description": text,
        "boundingPoly": {
            "vertices": [
                {"x": x1, "y": y1},
                {"x": x2, "y": y1},
                {"x": x2, "y": y2},
                {"x": x1, "y": y2},
            ]
        },
    }


def test_generic_parser_three_passes_layout_math_and_cleanup():
    annotations = [
        _annotation("FULL PAGE TEXT", 0, 0, 1, 1),
        _annotation("STATEMENT", 60, 16, 160, 30),
        _annotation("2025", 170, 16, 220, 30),
        _annotation("Date", 30, 60, 70, 74),
        _annotation("Description", 140, 60, 250, 74),
        _annotation("Debit", 430, 60, 490, 74),
        _annotation("Credit", 540, 60, 610, 74),
        _annotation("Balance", 670, 60, 760, 74),
        _annotation("01/05", 35, 100, 95, 114),
        _annotation("ATM", 140, 100, 180, 114),
        _annotation("Withdrawal", 190, 100, 290, 114),
        _annotation("500.00", 430, 100, 500, 114),
        _annotation("9500.00", 680, 100, 770, 114),
        _annotation("via", 300, 120, 335, 134),
        _annotation("Terminal", 342, 120, 430, 134),
        _annotation("01/06", 35, 160, 95, 174),
        _annotation("Deposit", 140, 160, 215, 174),
        _annotation("11500.00", 680, 160, 785, 174),
        _annotation("SERVICE", 140, 200, 220, 214),
        _annotation("CHARGE", 228, 200, 312, 214),
        _annotation("PAGE", 140, 240, 190, 254),
        _annotation("TOTAL", 198, 240, 260, 254),
        _annotation("11500.00", 680, 240, 785, 254),
    ]
    payload = {
        "provider": "google_vision",
        "pages": [{"page_number": 1, "response": {"textAnnotations": annotations}}],
    }

    rows = parse_transactions_from_ocr_raw(payload)

    assert len(rows) == 2
    assert rows[0]["date"] == "2025-01-05"
    assert rows[0]["description"] == "ATM Withdrawal via Terminal"
    assert rows[0]["debit"] == 500.0
    assert rows[0]["credit"] == 0.0
    assert rows[0]["balance"] == 9500.0

    assert rows[1]["date"] == "2025-01-06"
    assert rows[1]["description"] == "Deposit"
    assert rows[1]["debit"] == 0.0
    assert rows[1]["credit"] == 2000.0
    assert rows[1]["balance"] == 11500.0


def test_generic_parser_keeps_numeric_rows_without_date():
    annotations = [
        _annotation("FULL PAGE TEXT", 0, 0, 1, 1),
        _annotation("2025", 170, 16, 220, 30),
        _annotation("Date", 30, 60, 70, 74),
        _annotation("Description", 140, 60, 250, 74),
        _annotation("Balance", 670, 60, 760, 74),
        _annotation("no", 140, 100, 170, 114),
        _annotation("date", 175, 100, 225, 114),
        _annotation("here", 230, 100, 275, 114),
        _annotation("999.00", 680, 100, 745, 114),
    ]
    payload = {
        "provider": "google_vision",
        "pages": [{"page_number": 1, "response": {"textAnnotations": annotations}}],
    }

    rows = parse_transactions_from_ocr_raw(payload)
    assert len(rows) == 1
    assert rows[0]["date"] == ""
    assert rows[0]["description"] == "no here"
    assert rows[0]["balance"] is None


def test_generic_parser_extracts_embedded_textual_dates():
    annotations = [
        _annotation("FULL PAGE TEXT", 0, 0, 1, 1),
        _annotation("2025", 170, 16, 220, 30),
        _annotation("Date", 30, 60, 70, 74),
        _annotation("Description", 140, 60, 250, 74),
        _annotation("Balance", 670, 60, 760, 74),
        _annotation("Jan", 140, 100, 180, 114),
        _annotation("05", 185, 100, 215, 114),
        _annotation("POS", 220, 100, 260, 114),
        _annotation("DEBIT", 268, 100, 330, 114),
        _annotation("100.00", 430, 100, 500, 114),
        _annotation("900.00", 680, 100, 760, 114),
    ]
    payload = {
        "provider": "google_vision",
        "pages": [{"page_number": 1, "response": {"textAnnotations": annotations}}],
    }

    rows = parse_transactions_from_ocr_raw(payload)

    assert len(rows) == 1
    assert rows[0]["date"] == "2025-01-05"
    assert rows[0]["description"] == "Jan 05 POS"
