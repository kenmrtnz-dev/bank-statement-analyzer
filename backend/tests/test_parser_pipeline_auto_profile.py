from app.parser import pipeline


def test_google_vision_auto_parser_prefers_non_generic_with_valid_rows(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "_parse_google_vision_transactions",
        lambda _raw, parser: (
            [{"date": "2026-03-01", "balance": "100.00", "debit": "", "credit": "100.00"}]
            if parser == "bdo"
            else ([] if parser == "sterling_bank_of_asia" else [{"description": "noise"}])
        ),
    )

    rows, parser_used = pipeline.parse_google_vision_raw_payload(
        {"pages": []},
        parser_profile="auto",
        detected_bank="generic",
    )

    assert parser_used == "bdo"
    assert len(rows) == 1


def test_google_vision_auto_parser_falls_back_to_generic_when_no_valid_parser(monkeypatch):
    monkeypatch.setattr(
        pipeline,
        "_parse_google_vision_transactions",
        lambda _raw, parser: [{"description": "noise"}] if parser == "generic" else [],
    )

    rows, parser_used = pipeline.parse_google_vision_raw_payload(
        {"pages": []},
        parser_profile="auto",
        detected_bank="generic",
    )

    assert parser_used == "generic"
    assert rows == [{"description": "noise"}]
