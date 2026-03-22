from app.jobs import service as jobs_service
from app.pdf_text_extract import layout_page_to_json_payload


def test_layout_page_to_json_payload_builds_jsonb_safe_text_result():
    payload = layout_page_to_json_payload(
        {
            "width": 900,
            "height": 1200,
            "text": "Opening Balance",
            "words": [{"text": "Opening", "x1": 10, "y1": 20, "x2": 100, "y2": 40}],
        },
        page_number=2,
    )

    assert payload["provider"] == "pdftotext"
    assert payload["source_type"] == "text"
    assert payload["page_number"] == 2
    assert payload["is_digital"] is True
    assert payload["words"][0]["text"] == "Opening"


def test_list_cleaned_pages_falls_back_to_input_pdf_pages_when_cleaned_is_missing(monkeypatch, tmp_path):
    job_id = "job-cleaned-pages"
    repo = jobs_service.JobsRepository(tmp_path)
    repo.ensure_job_layout(job_id)
    repo.write_bytes(repo.path(job_id, "input", "document.pdf"), b"%PDF-1.4\n")
    repo.write_json(
        repo.path(job_id, "result", "parsed_rows.json"),
        {
            "page_002": [
                {
                    "row_id": "001",
                    "date": "2024-05-14",
                    "description": "stale parsed output",
                    "debit": None,
                    "credit": "200.00",
                    "balance": "107668.45",
                }
            ]
        },
    )

    class _FakeReader:
        def __init__(self, _path: str):
            self.pages = [object(), object(), object()]

    monkeypatch.setattr(jobs_service, "DATA_DIR", str(tmp_path))
    monkeypatch.setattr(jobs_service, "PdfReader", _FakeReader)

    assert jobs_service.list_cleaned_pages(job_id) == [
        "page_001.png",
        "page_002.png",
        "page_003.png",
    ]


def test_rebuild_ocr_outputs_from_saved_artifacts_reuses_previous_page_header_hint(monkeypatch, tmp_path):
    job_id = "job-rebuild-ocr"
    repo = jobs_service.JobsRepository(tmp_path)
    repo.ensure_job_layout(job_id)

    cleaned_dir = repo.path(job_id, "cleaned")
    ocr_dir = repo.path(job_id, "ocr")
    for page_name in ("page_001", "page_002"):
        cleaned_dir.mkdir(parents=True, exist_ok=True)
        ocr_dir.mkdir(parents=True, exist_ok=True)
        repo.write_bytes(cleaned_dir / f"{page_name}.png", b"png")

    repo.write_json(ocr_dir / "page_001.json", [{"text": "CHINABANK HEADER"}])
    repo.write_json(ocr_dir / "page_002.json", [{"text": "CONTINUATION PAGE"}])

    monkeypatch.setattr(jobs_service, "_image_size", lambda _path: (1200, 900))
    monkeypatch.setattr(jobs_service, "_ocr_items_to_words", lambda items: items)
    monkeypatch.setattr(jobs_service, "_filter_rows_and_bounds", lambda rows, bounds, _profile: (rows, bounds))

    def _detect_profile(text: str):
        return type("Profile", (), {"name": "CHINABANK" if "HEADER" in text else "GENERIC"})()

    monkeypatch.setattr(jobs_service, "detect_bank_profile", _detect_profile)

    seen_header_hints = []

    def _fake_parse(words, _page_width, _page_height, profile, header_hint=None, last_date_hint=None):
        seen_header_hints.append(header_hint)
        if profile.name == "CHINABANK":
            return (
                [
                    {
                        "row_id": "001",
                        "date": "2025-06-04",
                        "description": "Cash Deposit",
                        "debit": None,
                        "credit": "127001.00",
                        "balance": "258224.86",
                        "row_type": "transaction",
                    }
                ],
                [{"row_id": "001", "x1": 0.1, "y1": 0.1, "x2": 0.9, "y2": 0.2}],
                {"profile_detected": "CHINABANK", "profile_selected": "CHINABANK", "header_detected": True, "header_hint_used": False, "header_anchors": {"date": 10.0, "debit": 20.0, "credit": 30.0, "balance": 40.0}},
            )
        assert header_hint == {"date": 10.0, "debit": 20.0, "credit": 30.0, "balance": 40.0}
        return (
            [
                {
                    "row_id": "001",
                    "date": "2025-06-04",
                    "description": "PM TRANSFER FR PNB PGW",
                    "debit": None,
                    "credit": "50000.00",
                    "balance": "308224.86",
                    "row_type": "transaction",
                }
            ],
            [{"row_id": "001", "x1": 0.1, "y1": 0.2, "x2": 0.9, "y2": 0.3}],
            {"profile_detected": "GENERIC", "profile_selected": "GENERIC", "header_detected": False, "header_hint_used": True},
        )

    monkeypatch.setattr(jobs_service, "parse_page_with_profile_fallback", _fake_parse)

    parsed_output, bounds_output, page_diags = jobs_service._rebuild_ocr_outputs_from_saved_artifacts(
        repo=repo,
        job_id=job_id,
        page_files=["page_001.png", "page_002.png"],
        parsed_output={},
        bounds_output={},
        page_diagnostics={},
    )

    assert seen_header_hints == [None, {"date": 10.0, "debit": 20.0, "credit": 30.0, "balance": 40.0}]
    assert parsed_output["page_002"][0]["credit"] == "50000.00"
    assert bounds_output["page_002"][0]["row_id"] == "001"
    assert page_diags["page_002"]["header_hint_used"] is True
