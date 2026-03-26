from __future__ import annotations

from io import BytesIO
from types import SimpleNamespace

from pypdf import PdfReader, PdfWriter

from app.jobs import service as jobs_service


def _escape_pdf_text(text: str) -> str:
    return (
        str(text)
        .replace("\\", "\\\\")
        .replace("(", "\\(")
        .replace(")", "\\)")
    )


def _build_text_pdf_bytes(page_texts: list[str]) -> bytes:
    page_count = len(page_texts)
    objects: list[str] = []

    kids = " ".join(f"{4 + idx} 0 R" for idx in range(page_count))
    objects.append("<< /Type /Catalog /Pages 2 0 R >>")
    objects.append(f"<< /Type /Pages /Kids [{kids}] /Count {page_count} >>")
    objects.append("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    content_object_numbers = []
    for idx, raw_text in enumerate(page_texts, start=1):
        content_object_numbers.append(3 + page_count + idx)
        escaped_text = _escape_pdf_text(raw_text)
        stream = f"BT /F1 12 Tf 50 100 Td ({escaped_text}) Tj ET"
        objects.append(
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200] "
            f"/Resources << /Font << /F1 3 0 R >> >> /Contents {3 + page_count + idx} 0 R >>"
        )

    for raw_text in page_texts:
        escaped_text = _escape_pdf_text(raw_text)
        stream = f"BT /F1 12 Tf 50 100 Td ({escaped_text}) Tj ET"
        encoded = stream.encode("latin1")
        objects.append(f"<< /Length {len(encoded)} >>\nstream\n{stream}\nendstream")

    out: list[str] = ["%PDF-1.4\n"]
    offsets = [0]
    for idx, obj in enumerate(objects, start=1):
        offsets.append(sum(len(part.encode("latin1")) for part in out))
        out.append(f"{idx} 0 obj\n{obj}\nendobj\n")
    xref_start = sum(len(part.encode("latin1")) for part in out)
    xref = ["xref\n", f"0 {len(objects) + 1}\n", "0000000000 65535 f \n"]
    for offset in offsets[1:]:
        xref.append(f"{offset:010d} 00000 n \n")
    out.extend(xref)
    out.append(
        f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_start}\n%%EOF\n"
    )
    return "".join(out).encode("latin1")


def test_split_preserves_embedded_text_after_writing_single_pages():
    source_pdf = BytesIO(_build_text_pdf_bytes(["Opening Balance", "Monthly Statement"]))
    reader = PdfReader(source_pdf)

    split_texts: list[str] = []
    for page_index, expected_text in enumerate(["Opening Balance", "Monthly Statement"]):
        writer = PdfWriter()
        writer.add_page(reader.pages[page_index])
        output = BytesIO()
        writer.write(output)

        split_reader = PdfReader(BytesIO(output.getvalue()))
        extracted_text = (split_reader.pages[0].extract_text() or "").strip()
        split_texts.append(extracted_text)
        assert extracted_text == expected_text

    assert split_texts == ["Opening Balance", "Monthly Statement"]


def test_mixed_document_routes_digital_pages_without_ocr_and_scanned_pages_with_ocr(client, monkeypatch):
    job_id = client.post(
        "/jobs",
        files={"file": ("statement.pdf", _build_text_pdf_bytes(["Digital one", "", "Digital three"]), "application/pdf")},
        data={"mode": "auto", "auto_start": "false"},
    ).json()["job_id"]

    repo = jobs_service.JobsRepository(jobs_service.DATA_DIR)
    page_layouts = [
        {
            "width": 612.0,
            "height": 792.0,
            "text": "Digital one",
            "words": [{"text": "Digital one", "x1": 10.0, "y1": 20.0, "x2": 110.0, "y2": 40.0}],
        },
        {"width": 612.0, "height": 792.0, "text": "", "words": []},
        {
            "width": 612.0,
            "height": 792.0,
            "text": "Digital three",
            "words": [{"text": "Digital three", "x1": 10.0, "y1": 20.0, "x2": 130.0, "y2": 40.0}],
        },
    ]

    monkeypatch.setattr(jobs_service, "prepare_ocr_pages", lambda **_kwargs: (_ for _ in ()).throw(AssertionError("full-PDF rasterization must not run for mixed documents")))
    monkeypatch.setattr(jobs_service, "_filter_rows_and_bounds", lambda rows, bounds, _profile: (rows, bounds))
    monkeypatch.setattr(jobs_service, "_repair_page_flow_columns", lambda rows, previous_balance_hint=None: rows)
    monkeypatch.setattr(jobs_service, "_upsert_page_intake_record", lambda **_kwargs: None, raising=False)

    ocr_calls: list[str] = []

    def _fake_detect_bank_profile(_text: str):
        return SimpleNamespace(name="GENERIC")

    def _fake_parse_page_with_profile_fallback(words, _page_w, _page_h, _profile, header_hint=None, last_date_hint=None):
        del header_hint, last_date_hint
        description = " ".join(str(item.get("text") or "").strip() for item in words if isinstance(item, dict)).strip()
        return (
            [
                {
                    "row_id": "001",
                    "date": "2026-03-01",
                    "description": description or "Digital page",
                    "debit": None,
                    "credit": "1.00",
                    "balance": "1.00",
                    "row_type": "transaction",
                }
            ],
            [{"row_id": "001", "x1": 0.1, "y1": 0.2, "x2": 0.9, "y2": 0.25}],
            {"profile_detected": "GENERIC", "profile_selected": "GENERIC", "rows_parsed": 1},
        )

    def _fake_extract_text_page_raw_result(*, page_pdf_path, page_number):
        del page_pdf_path
        if page_number == 2:
            return {
                "provider": "pdftotext",
                "source_type": "ocr",
                "page_number": page_number,
                "width": 612.0,
                "height": 792.0,
                "text": "",
                "words": [],
                "is_digital": False,
            }
        label = "Digital one" if page_number == 1 else "Digital three"
        return {
            "provider": "pdftotext",
            "source_type": "text",
            "page_number": page_number,
            "width": 612.0,
            "height": 792.0,
            "text": label,
            "words": [{"text": label, "x1": 10.0, "y1": 20.0, "x2": 110.0, "y2": 40.0}],
            "is_digital": True,
        }

    def _fake_process_ocr_page(*, page_file, pages_dir, cleaned_dir, ocr_dir, rate_limit_heartbeat=None, raw_result_callback=None, **_kwargs):
        del pages_dir, cleaned_dir, ocr_dir, rate_limit_heartbeat
        ocr_calls.append(page_file)
        if callable(raw_result_callback):
            raw_result_callback(
                {
                    "provider": "google_vision",
                    "source_type": "ocr",
                    "page_number": 2,
                    "width": 612.0,
                    "height": 792.0,
                    "text": "Scanned page",
                    "words": [{"text": "Scanned page", "x1": 10.0, "y1": 20.0, "x2": 130.0, "y2": 40.0}],
                    "is_digital": False,
                }
            )
        return (
            "page_002",
            [
                {
                    "row_id": "001",
                    "date": "2026-03-02",
                    "description": "Scanned page",
                    "debit": None,
                    "credit": "2.00",
                    "balance": "2.00",
                    "row_type": "transaction",
                }
            ],
            [{"row_id": "001", "x1": 0.2, "y1": 0.3, "x2": 0.8, "y2": 0.35}],
                {"source_type": "ocr", "ocr_backend": "fake_ocr", "rows_parsed": 1},
            )

    def _split_pdf_into_page_pdfs(*, repo, job_id, input_pdf):
        del input_pdf
        split_dir = repo.path(job_id, "split")
        pages_dir = repo.path(job_id, "pages")
        ocr_dir = repo.path(job_id, "ocr")
        split_dir.mkdir(parents=True, exist_ok=True)
        pages_dir.mkdir(parents=True, exist_ok=True)
        ocr_dir.mkdir(parents=True, exist_ok=True)
        for idx, page_name in enumerate(["page_001", "page_002", "page_003"], start=1):
            pdf_payload = _build_text_pdf_bytes([page_layouts[idx - 1]["text"] if idx != 2 else ""])
            (split_dir / f"{page_name}.pdf").write_bytes(pdf_payload)
            if idx == 2:
                (pages_dir / f"{page_name}.png").write_bytes(b"png")
                continue
            repo.write_json(
                repo.path(job_id, "ocr", f"{page_name}.raw.json"),
                jobs_service.layout_page_to_json_payload(page_layouts[idx - 1], page_number=idx),
            )
        return ["page_001", "page_002", "page_003"]

    def _enqueue_page_job(job_id, parse_mode, page_name, page_index, page_count):
        task_id = f"page-task-{page_name}"
        jobs_service.process_job_page(
            job_id=job_id,
            parse_mode=parse_mode,
            page_name=page_name,
            page_index=page_index,
            page_count=page_count,
            task_id=task_id,
        )
        return task_id

    def _enqueue_finalize_job(job_id, parse_mode):
        jobs_service.finalize_job_processing(job_id=job_id, parse_mode=parse_mode, task_id="finalize-task")
        return "finalize-task"

    monkeypatch.setattr(jobs_service, "detect_bank_profile", _fake_detect_bank_profile)
    monkeypatch.setattr(jobs_service, "parse_page_with_profile_fallback", _fake_parse_page_with_profile_fallback)
    monkeypatch.setattr(jobs_service, "process_ocr_page", _fake_process_ocr_page)
    monkeypatch.setattr(jobs_service, "_extract_text_page_raw_result", _fake_extract_text_page_raw_result)
    monkeypatch.setattr(jobs_service, "_split_pdf_into_page_pdfs", _split_pdf_into_page_pdfs)
    monkeypatch.setattr(jobs_service, "_update_page_runtime_status", lambda **_kwargs: None, raising=False)
    monkeypatch.setattr(jobs_service, "_enqueue_page_job", _enqueue_page_job)
    monkeypatch.setattr(jobs_service, "_enqueue_finalize_job", _enqueue_finalize_job)

    payload = jobs_service.process_job(job_id=job_id, parse_mode="auto", task_id="root-task")

    assert repo.path(job_id, "split", "page_001.pdf").exists()
    assert repo.path(job_id, "split", "page_002.pdf").exists()
    assert repo.path(job_id, "split", "page_003.pdf").exists()
    assert ocr_calls == ["page_002.png"]
    assert repo.read_json(repo.path(job_id, "ocr", "page_001.raw.json"), default={})["source_type"] == "text"
    assert repo.read_json(repo.path(job_id, "ocr", "page_002.raw.json"), default={})["source_type"] == "ocr"
    assert repo.read_json(repo.path(job_id, "result", "page_fragments", "page_001.json"), default={})["rows"][0]["description"] == "Digital one"
    assert repo.read_json(repo.path(job_id, "result", "page_fragments", "page_002.json"), default={})["rows"][0]["description"] == "Scanned page"
    assert repo.read_json(repo.path(job_id, "result", "page_fragments", "page_003.json"), default={})["rows"][0]["description"] == "Digital three"
    assert payload["status"] in {"processing", "parsing", "completed"}


def test_pages_are_processed_independently_per_page_job(client, monkeypatch):
    job_id = client.post(
        "/jobs",
        files={"file": ("statement.pdf", _build_text_pdf_bytes(["Page one", "Page two"]), "application/pdf")},
        data={"mode": "auto", "auto_start": "false"},
    ).json()["job_id"]

    repo = jobs_service.JobsRepository(jobs_service.DATA_DIR)
    repo.write_json(repo.path(job_id, "result", "pages_manifest.json"), {"pages": ["page_001.png", "page_002.png"]})
    split_dir = repo.path(job_id, "split")
    split_dir.mkdir(parents=True, exist_ok=True)
    (split_dir / "page_001.pdf").write_bytes(_build_text_pdf_bytes(["Page one"]))
    (split_dir / "page_002.pdf").write_bytes(_build_text_pdf_bytes([""]))
    repo.write_json(
        repo.path(job_id, "ocr", "page_001.raw.json"),
        {
            "provider": "pdftotext",
            "source_type": "text",
            "page_number": 1,
            "width": 1000.0,
            "height": 1400.0,
            "text": "Page one",
            "words": [{"text": "Page one", "x1": 10.0, "y1": 20.0, "x2": 120.0, "y2": 40.0}],
            "is_digital": True,
        },
    )
    (repo.path(job_id, "pages")).mkdir(parents=True, exist_ok=True)
    (repo.path(job_id, "pages", "page_002.png")).write_bytes(b"png")

    page_calls: list[tuple[str, str]] = []

    monkeypatch.setattr(jobs_service, "detect_bank_profile", lambda _text: SimpleNamespace(name="GENERIC"))
    monkeypatch.setattr(jobs_service, "_filter_rows_and_bounds", lambda rows, bounds, _profile: (rows, bounds))
    monkeypatch.setattr(jobs_service, "_repair_page_flow_columns", lambda rows, previous_balance_hint=None: rows)
    monkeypatch.setattr(
        jobs_service,
        "parse_page_with_profile_fallback",
        lambda words, *_args, **_kwargs: (
            [
                {
                    "row_id": "001",
                    "date": "2026-03-01",
                    "description": " ".join(item.get("text", "") for item in words if isinstance(item, dict)).strip(),
                    "debit": None,
                    "credit": "10.00",
                    "balance": "10.00",
                    "row_type": "transaction",
                }
            ],
            [{"row_id": "001", "x1": 0.1, "y1": 0.2, "x2": 0.9, "y2": 0.25}],
            {"profile_detected": "GENERIC", "profile_selected": "GENERIC", "rows_parsed": 1},
        ),
    )

    def _fake_extract_text_page_raw_result(*, page_pdf_path, page_number):
        del page_pdf_path
        if page_number == 2:
            return {
                "provider": "pdftotext",
                "source_type": "ocr",
                "page_number": page_number,
                "width": 1000.0,
                "height": 1400.0,
                "text": "",
                "words": [],
                "is_digital": False,
            }
        return {
            "provider": "pdftotext",
            "source_type": "text",
            "page_number": page_number,
            "width": 1000.0,
            "height": 1400.0,
            "text": "Page one",
            "words": [{"text": "Page one", "x1": 10.0, "y1": 20.0, "x2": 120.0, "y2": 40.0}],
            "is_digital": True,
        }

    def _fake_process_ocr_page(*, page_file, pages_dir, cleaned_dir, ocr_dir, rate_limit_heartbeat=None, raw_result_callback=None, **_kwargs):
        del pages_dir, cleaned_dir, ocr_dir, rate_limit_heartbeat
        page_calls.append(("ocr", page_file))
        if callable(raw_result_callback):
            raw_result_callback(
                {
                    "provider": "google_vision",
                    "source_type": "ocr",
                    "page_number": 2,
                    "width": 1000.0,
                    "height": 1400.0,
                    "text": "Page two",
                    "words": [{"text": "Page two", "x1": 10.0, "y1": 20.0, "x2": 120.0, "y2": 40.0}],
                    "is_digital": False,
                }
            )
        return (
            "page_002",
            [
                {
                    "row_id": "001",
                    "date": "2026-03-02",
                    "description": "Page two",
                    "debit": None,
                    "credit": "20.00",
                    "balance": "20.00",
                    "row_type": "transaction",
                }
            ],
            [{"row_id": "001", "x1": 0.2, "y1": 0.3, "x2": 0.8, "y2": 0.35}],
            {"source_type": "ocr", "ocr_backend": "fake_ocr", "rows_parsed": 1},
        )

    monkeypatch.setattr(jobs_service, "_enqueue_finalize_job", lambda job_id, parse_mode: "finalize-task")
    monkeypatch.setattr(jobs_service, "_update_page_runtime_status", lambda **_kwargs: None, raising=False)
    monkeypatch.setattr(jobs_service, "_extract_text_page_raw_result", _fake_extract_text_page_raw_result)
    monkeypatch.setattr(jobs_service, "process_ocr_page", _fake_process_ocr_page)

    text_payload = jobs_service.process_job_page(
        job_id=job_id,
        parse_mode="auto",
        page_name="page_001",
        page_index=1,
        page_count=2,
        task_id="page-task-001",
    )
    ocr_payload = jobs_service.process_job_page(
        job_id=job_id,
        parse_mode="auto",
        page_name="page_002",
        page_index=2,
        page_count=2,
        task_id="page-task-002",
    )

    assert page_calls == [("ocr", "page_002.png")]
    assert repo.read_json(repo.path(job_id, "result", "page_fragments", "page_001.json"), default={})["rows"][0]["description"] == "Page one"
    assert repo.read_json(repo.path(job_id, "result", "page_fragments", "page_002.json"), default={})["rows"][0]["description"] == "Page two"
    assert ocr_payload == {}
    assert text_payload == {}


def test_job_status_progress_tracks_page_completion(client):
    job_id = client.post(
        "/jobs",
        files={"file": ("statement.pdf", _build_text_pdf_bytes(["Page one", "Page two", "Page three"]), "application/pdf")},
        data={"mode": "ocr", "auto_start": "false"},
    ).json()["job_id"]

    repo = jobs_service.JobsRepository(jobs_service.DATA_DIR)
    repo.write_json(repo.path(job_id, "result", "pages_manifest.json"), {"pages": ["page_001.png", "page_002.png", "page_003.png"]})
    repo.write_json(
        repo.path(job_id, "result", "page_status.json"),
        {
            "page_001": {"status": "done", "page_index": 1, "page_count": 3, "rows_parsed": 1},
            "page_002": {"status": "processing", "page_index": 2, "page_count": 3},
            "page_003": {"status": "pending", "page_index": 3, "page_count": 3},
        },
    )
    repo.write_json(
        repo.path(job_id, "result", "page_fragments", "page_001.json"),
        {"page": "page_001", "rows": [{"row_id": "001", "description": "Page one"}], "bounds": [], "diag": {"rows_parsed": 1}},
    )

    jobs_service._refresh_job_progress(repo, job_id, parse_mode="google_vision")

    status = client.get(f"/jobs/{job_id}")
    assert status.status_code == 200
    payload = status.json()
    assert payload["status"] == "parsing"
    assert payload["pages_done"] == 1
    assert payload["pages_inflight"] == 2
    assert payload["progress"] > 0

    repo.write_json(
        repo.path(job_id, "result", "page_status.json"),
        {
            "page_001": {"status": "done", "page_index": 1, "page_count": 3, "rows_parsed": 1},
            "page_002": {"status": "done", "page_index": 2, "page_count": 3, "rows_parsed": 1},
            "page_003": {"status": "done", "page_index": 3, "page_count": 3, "rows_parsed": 1},
        },
    )
    repo.write_json(
        repo.path(job_id, "result", "page_fragments", "page_002.json"),
        {"page": "page_002", "rows": [{"row_id": "001", "description": "Page two"}], "bounds": [], "diag": {"rows_parsed": 1}},
    )
    repo.write_json(
        repo.path(job_id, "result", "page_fragments", "page_003.json"),
        {"page": "page_003", "rows": [{"row_id": "001", "description": "Page three"}], "bounds": [], "diag": {"rows_parsed": 1}},
    )

    jobs_service._refresh_job_progress(repo, job_id, parse_mode="google_vision")

    completed = client.get(f"/jobs/{job_id}")
    assert completed.status_code == 200
    completed_payload = completed.json()
    assert completed_payload["status"] == "completed"
    assert completed_payload["pages_done"] == 3
    assert completed_payload["pages_inflight"] == 0
    assert completed_payload["progress"] == 100
