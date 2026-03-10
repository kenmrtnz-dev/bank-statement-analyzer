from app.jobs import service as jobs_service


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
