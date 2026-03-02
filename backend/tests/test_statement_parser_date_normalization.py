from app.statement_parser import normalize_date


def test_normalize_date_corrects_common_ocr_century_misread():
    parsed = normalize_date("10/10/1925", ["mdy", "dmy", "ymd"])
    assert parsed == "2025-10-10"

