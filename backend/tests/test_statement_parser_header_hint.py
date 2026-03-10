from app.bank_profiles import PROFILES, detect_bank_profile
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


def test_header_column_ranges_prevent_description_digits_from_becoming_debit():
    profile = PROFILES["GENERIC"]
    page_w = 800.0
    page_h = 1000.0

    words = [
        _word("Date", 40, 20),
        _word("Description", 170, 20, 90),
        _word("Debit", 470, 20),
        _word("Credit", 560, 20),
        _word("Balance", 680, 20, 70),
        _word("08/27/2025", 40, 60, 72),
        _word("REF", 190, 60, 36),
        _word("9", 355, 60, 10),
        _word("PAYMENT", 320, 60, 70),
        _word("9000.00", 690, 60, 68),
    ]

    rows, _, diag = parse_page_with_profile_fallback(
        words,
        page_w,
        page_h,
        profile,
    )

    assert diag.get("header_detected") is True
    assert len(rows) == 1
    assert rows[0]["description"] == "REF PAYMENT 9"
    assert rows[0]["debit"] is None
    assert rows[0]["credit"] is None
    assert rows[0]["balance"] == "9000.00"


def test_bdo_digital_layout_ignores_short_description_digits_and_keeps_credit():
    page_w = 800.0
    page_h = 1000.0
    profile = detect_bank_profile(
        "BUSINESS BANKING Transaction History BRANCH DESCRIPTION DEBIT CREDIT RUNNING BALANCE CHECK NUMBER"
    )

    assert profile.name == "AUTO_BUSINESS_BANKING_GROWIDE"

    words = [
        _word("POSTING", 35, 20, 36),
        _word("DATE", 73, 20, 22),
        _word("BRANCH", 103, 20, 35),
        _word("DESCRIPTION", 223, 20, 55),
        _word("DEBIT", 343, 20, 24),
        _word("CREDIT", 480, 20, 30),
        _word("RUNNING", 619, 20, 37),
        _word("BALANCE", 658.6, 20, 39),
        _word("CHECK", 727, 20, 28),
        _word("NUMBER", 757.7, 20, 35),
        _word("08/12/2025", 32, 60, 50),
        _word("OTHER", 100, 60, 35),
        _word("BANKS", 137.8, 60, 34),
        _word("0000007590950725", 220, 60, 89),
        _word("IBTD", 220, 60, 22.8),
        _word("052804052804", 245.6, 60, 66.7),
        _word("9", 311.7, 60, 5.6),
        _word("50,000.00", 570.5, 60, 44.5),
        _word("1,199,587.46", 664.6, 60, 58.4),
        _word("000000000", 727, 60, 50),
    ]

    rows, _, diag = parse_page_with_profile_fallback(
        words,
        page_w,
        page_h,
        profile,
    )

    assert diag.get("header_detected") is True
    assert len(rows) == 1
    assert rows[0]["debit"] is None
    assert rows[0]["credit"] == "50000.00"
    assert rows[0]["balance"] == "1199587.46"


def test_header_synonyms_and_extra_columns_flow_into_description():
    profile = PROFILES["GENERIC"]
    page_w = 900.0
    page_h = 1200.0

    words = [
        _word("LN", 20, 20, 25),
        _word("Value", 70, 20, 38),
        _word("Date", 112, 20, 30),
        _word("Transaction", 210, 20, 86),
        _word("TC", 360, 20, 24),
        _word("Withdrawal", 470, 20, 78),
        _word("Deposit", 590, 20, 56),
        _word("Bal", 710, 20, 28),
        _word("1", 22, 60, 10),
        _word("08/27/2025", 70, 60, 74),
        _word("ATM", 210, 60, 28),
        _word("PAYMENT", 244, 60, 70),
        _word("A45", 360, 60, 26),
        _word("1000.00", 595, 60, 58),
        _word("9000.00", 715, 60, 60),
    ]

    rows, _, diag = parse_page_with_profile_fallback(
        words,
        page_w,
        page_h,
        profile,
    )

    assert diag.get("header_detected") is True
    assert len(rows) == 1
    assert rows[0]["date"] == "2025-08-27"
    assert rows[0]["description"] == "ATM PAYMENT A45"
    assert rows[0]["debit"] is None
    assert rows[0]["credit"] == "1000.00"
    assert rows[0]["balance"] == "9000.00"


def test_multiline_header_detection_merges_adjacent_header_rows():
    profile = PROFILES["RCBC"]
    page_w = 900.0
    page_h = 1200.0

    words = [
        _word("POSTED", 40, 20, 50),
        _word("DESCRIPTION", 180, 20, 90),
        _word("BRANCH", 300, 20, 55),
        _word("DEBIT", 470, 20, 48),
        _word("CREDIT", 560, 20, 54),
        _word("RUNNING", 680, 20, 66),
        _word("DATE", 48, 34, 34),
        _word("BALANCE", 758, 34, 72),
        _word("11/27/25", 48, 72, 62),
        _word("CHECK", 180, 72, 50),
        _word("ENCASHED", 236, 72, 76),
        _word("10000.00", 474, 72, 68),
        _word("12858713.49", 760, 72, 90),
    ]

    rows, _, diag = parse_page_with_profile_fallback(
        words,
        page_w,
        page_h,
        profile,
    )

    assert diag.get("header_detected") is True
    assert len(rows) == 1
    assert rows[0]["date"] == "2025-11-27"
    assert rows[0]["description"] == "CHECK ENCASHED"
    assert rows[0]["debit"] == "10000.00"
    assert rows[0]["balance"] == "12858713.49"


def test_extra_column_placeholder_tokens_are_dropped_from_description():
    profile = PROFILES["GENERIC"]
    page_w = 900.0
    page_h = 1200.0

    words = [
        _word("Date", 40, 20),
        _word("Description", 180, 20, 90),
        _word("Debit", 470, 20),
        _word("Credit", 560, 20),
        _word("Balance", 680, 20, 70),
        _word("Check", 790, 20, 42),
        _word("No.", 836, 20, 28),
        _word("08/27/2025", 40, 60, 74),
        _word("PAYMENT", 180, 60, 70),
        _word("—", 808, 60, 10),
        _word("9000.00", 690, 60, 58),
    ]

    rows, _, diag = parse_page_with_profile_fallback(
        words,
        page_w,
        page_h,
        profile,
    )

    assert diag.get("header_detected") is True
    assert len(rows) == 1
    assert rows[0]["description"] == "PAYMENT"


def test_missing_date_reuses_last_extracted_date_for_valid_transaction_row():
    profile = PROFILES["GENERIC"]
    page_w = 900.0
    page_h = 1200.0

    words = [
        _word("Date", 40, 20),
        _word("Description", 180, 20, 90),
        _word("Debit", 470, 20),
        _word("Credit", 560, 20),
        _word("Balance", 680, 20, 70),
        _word("08/27/2025", 40, 60, 74),
        _word("PAYMENT", 180, 60, 70),
        _word("1000.00", 470, 60, 58),
        _word("9000.00", 690, 60, 58),
        _word("TRANSFER", 180, 90, 74),
        _word("500.00", 470, 90, 52),
        _word("8500.00", 690, 90, 58),
    ]

    rows, _, diag = parse_page_with_profile_fallback(
        words,
        page_w,
        page_h,
        profile,
    )

    assert diag.get("header_detected") is True
    assert len(rows) == 2
    assert rows[0]["date"] == "2025-08-27"
    assert rows[1]["date"] == "2025-08-27"
    assert rows[1]["description"] == "TRANSFER"
    assert rows[1]["debit"] == "500.00"
    assert rows[1]["balance"] == "8500.00"


def test_rcbc_transaction_reference_number_is_not_promoted_to_debit():
    profile = PROFILES["RCBC"]
    page_w = 900.0
    page_h = 1200.0

    words = [
        _word("Date", 20, 20, 28),
        _word("Instrmnt", 80, 20, 58),
        _word("Particulars", 165, 20, 70),
        _word("Transaction", 396, 20, 72),
        _word("Transaction", 508, 20, 72),
        _word("Balance", 638, 20, 56),
        _word("Number", 80, 34, 46),
        _word("Debit", 392, 34, 42),
        _word("Amount", 422, 34, 56),
        _word("Credit", 498, 34, 48),
        _word("Amount", 532, 34, 56),
        _word("07-24-2025", 22, 72, 62),
        _word("Fund", 166, 72, 34),
        _word("Transfer", 204, 72, 56),
        _word("Instapay", 264, 72, 60),
        _word("191065", 378, 72, 28),
        _word("50,000.00", 526, 72, 68),
        _word("3,418,916.58Cr", 613, 72, 90),
    ]

    rows, _, diag = parse_page_with_profile_fallback(
        words,
        page_w,
        page_h,
        profile,
    )

    assert diag.get("header_detected") is True
    assert len(rows) == 1
    assert rows[0]["description"] == "Fund Transfer Instapay"
    assert rows[0]["debit"] is None
    assert rows[0]["credit"] == "50000.00"
    assert rows[0]["balance"] == "3418916.58"


def test_header_parser_does_not_promote_description_numbers_into_amount_columns():
    profile = PROFILES["GENERIC"]
    page_w = 900.0
    page_h = 1200.0

    words = [
        _word("Date", 40, 20),
        _word("Description", 180, 20, 90),
        _word("Debit", 470, 20),
        _word("Credit", 560, 20),
        _word("Balance", 680, 20, 70),
        _word("08/27/2025", 40, 60, 74),
        _word("REF", 180, 60, 26),
        _word("1234.56", 220, 60, 50),
        _word("TC", 280, 60, 18),
        _word("7890.12", 310, 60, 50),
    ]

    rows, _, diag = parse_page_with_profile_fallback(
        words,
        page_w,
        page_h,
        profile,
    )

    assert diag.get("header_detected") is True
    assert rows == []


def test_ewb_wide_closing_balance_header_keeps_first_page_credit_row():
    profile = PROFILES["EWB"]
    page_w = 3600.0
    page_h = 4800.0

    words = [
        _word("Book", 158, 20, 52),
        _word("Date", 216, 20, 54),
        _word("Reference", 572, 20, 260),
        _word("Descript", 1050, 20, 270),
        _word("Value", 1450, 20, 62),
        _word("Date", 1518, 20, 112),
        _word("Cheque", 1988, 20, 90),
        _word("Number", 2086, 20, 76),
        _word("Debit", 2228, 20, 112),
        _word("Credit", 2642, 20, 130),
        _word("Closing", 3056, 20, 150),
        _word("Balance", 3215, 20, 191),
        _word("02", 158, 60, 34),
        _word("MAY", 202, 60, 62),
        _word("24", 272, 60, 32),
        _word("TT2412333BRM\\F", 572, 60, 380),
        _word("Cash", 1075, 60, 90),
        _word("Deposit", 1172, 60, 116),
        _word("02", 1518, 60, 34),
        _word("MAY", 1562, 60, 62),
        _word("24", 1632, 60, 32),
        _word("14,000.00", 2670, 60, 144),
        _word("952,374.45", 3140, 60, 164),
    ]

    rows, _, diag = parse_page_with_profile_fallback(
        words,
        page_w,
        page_h,
        profile,
    )

    assert diag.get("header_detected") is True
    assert len(rows) == 1
    assert rows[0]["date"] == "2024-05-02"
    assert rows[0]["debit"] is None
    assert rows[0]["credit"] == "14000.00"
    assert rows[0]["balance"] == "952374.45"


def test_header_parser_drops_beginning_balance_variants():
    profile = PROFILES["GENERIC"]
    page_w = 900.0
    page_h = 1200.0

    words = [
        _word("Date", 40, 20),
        _word("Description", 180, 20, 90),
        _word("Debit", 470, 20),
        _word("Credit", 560, 20),
        _word("Balance", 680, 20, 70),
        _word("08/27/2025", 40, 60, 74),
        _word("Beggining", 180, 60, 72),
        _word("Balance", 260, 60, 62),
        _word("9000.00", 690, 60, 58),
        _word("08/28/2025", 40, 90, 74),
        _word("PAYMENT", 180, 90, 70),
        _word("1000.00", 560, 90, 58),
        _word("10000.00", 690, 90, 68),
    ]

    rows, _, diag = parse_page_with_profile_fallback(
        words,
        page_w,
        page_h,
        profile,
    )

    assert diag.get("header_detected") is True
    assert len(rows) == 1
    assert rows[0]["description"] == "PAYMENT"
    assert rows[0]["credit"] == "1000.00"
    assert rows[0]["balance"] == "10000.00"


def test_headerless_parser_drops_balance_forwarded_rows():
    profile = PROFILES["GENERIC"]
    page_w = 900.0
    page_h = 1200.0

    words = [
        _word("08/27/2025", 40, 60, 74),
        _word("Balance", 180, 60, 62),
        _word("forwarded", 246, 60, 72),
        _word("9000.00", 690, 60, 58),
        _word("08/28/2025", 40, 90, 74),
        _word("TRANSFER", 180, 90, 70),
        _word("500.00", 470, 90, 52),
        _word("8500.00", 690, 90, 58),
    ]

    rows, _, diag = parse_page_with_profile_fallback(
        words,
        page_w,
        page_h,
        profile,
    )

    assert diag.get("header_detected") is False
    assert len(rows) == 1
    assert rows[0]["description"] == "TRANSFER"
    assert rows[0]["debit"] == "500.00"
    assert rows[0]["balance"] == "8500.00"
