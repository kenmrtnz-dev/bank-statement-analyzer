from app.bank_profiles import PROFILES
from app.statement_parser import parse_page_with_profile_fallback


def _word(text: str, x: float, y: float, w: float = 48.0, h: float = 10.0) -> dict:
    return {
        "text": text,
        "x1": x,
        "y1": y,
        "x2": x + w,
        "y2": y + h,
    }


def test_header_hint_reuses_amount_columns_for_headerless_page():
    profile = PROFILES["GENERIC"]
    page_w = 800.0
    page_h = 1000.0

    page_with_header_words = [
        _word("Date", 40, 20),
        _word("Description", 170, 20, 90),
        _word("Debit", 470, 20),
        _word("Credit", 560, 20),
        _word("Running", 660, 20),
        _word("Balance", 718, 20),
        _word("08/27/2025", 40, 60, 72),
        _word("BIZLINK", 190, 60, 70),
        _word("1000.00", 470, 60),
        _word("9000.00", 680, 60),
    ]

    _, _, first_diag = parse_page_with_profile_fallback(
        page_with_header_words,
        page_w,
        page_h,
        profile,
    )
    anchors = first_diag.get("header_anchors")
    assert isinstance(anchors, dict) and anchors

    headerless_words = [
        _word("08/26/2025", 40, 60, 72),
        _word("PAYMENT", 200, 60, 70),
        _word("500.00", 470, 60),
        _word("8500.00", 680, 60),
        _word("08/26/2025", 40, 90, 72),
        _word("TRANSFER", 200, 90, 70),
        _word("1000.00", 560, 90),
        _word("9500.00", 680, 90),
    ]

    rows_without_hint, _, _ = parse_page_with_profile_fallback(
        headerless_words,
        page_w,
        page_h,
        profile,
    )
    assert rows_without_hint[0]["debit"] == "500.00"
    assert rows_without_hint[1]["debit"] == "1000.00"
    assert rows_without_hint[1]["credit"] is None

    rows_with_hint, _, second_diag = parse_page_with_profile_fallback(
        headerless_words,
        page_w,
        page_h,
        profile,
        header_hint=anchors,
    )
    assert second_diag.get("header_hint_used") is True
    assert rows_with_hint[0]["debit"] == "500.00"
    assert rows_with_hint[0]["credit"] is None
    assert rows_with_hint[1]["debit"] is None
    assert rows_with_hint[1]["credit"] == "1000.00"
