from app.jobs.repository import JobsRepository
from app.jobs.service import _collect_ocr_raw_payload


def test_collect_ocr_raw_payload_merges_per_page_sources(tmp_path):
    repo = JobsRepository(tmp_path)
    job_id = "job-123"
    repo.ensure_job_layout(job_id)

    repo.write_json(
        repo.path(job_id, "ocr", "page_001.json"),
        [{"text": "A"}],
    )
    repo.write_json(
        repo.path(job_id, "ocr", "page_001.openai_raw.json"),
        {"id": "resp_1"},
    )
    repo.write_json(
        repo.path(job_id, "ocr", "page_002.google_vision_raw.json"),
        {"responses": [{"textAnnotations": []}]},
    )

    payload = _collect_ocr_raw_payload(repo, job_id, ["page_001.png", "page_002.png", "page_003.png"])

    assert payload == {
        "pages": {
            "page_001": {
                "ocr_items": [{"text": "A"}],
                "openai_raw": {"id": "resp_1"},
            },
            "page_002": {
                "google_vision_raw": {"responses": [{"textAnnotations": []}]},
            },
        }
    }
