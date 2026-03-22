from app.statement_parser import normalize_date


def test_normalize_date_corrects_common_ocr_century_misread():
    parsed = normalize_date("10/10/1925", ["mdy", "dmy", "ymd"])
    assert parsed == "2025-10-10"


def test_normalize_date_keeps_two_digit_year_for_slash_dates():
    parsed = normalize_date("11/27/25", ["mdy", "dmy", "ymd"])
    assert parsed == "2025-11-27"


def test_normalize_date_supports_dot_separated_day_month_year():
    parsed = normalize_date("30.04.25", ["mdy", "dmy", "ymd"])
    assert parsed == "2025-04-30"


def test_normalize_date_supports_compact_numeric_dates():
    parsed = normalize_date("050725", ["mdy", "dmy", "ymd"])
    assert parsed == "2025-05-07"
