from pathlib import Path

from app.admin.service import set_bank_code_flags
from app.jobs.repository import JobTransactionsRepository, JobsRepository


def test_job_transactions_persist_and_backfill_is_flagged(app_with_temp_data):
    _app, tmp_path = app_with_temp_data
    repo = JobsRepository(tmp_path)
    job_id = "11111111-1111-1111-1111-111111111111"
    root = repo.ensure_job_layout(job_id)
    repo.write_bytes(root / "input" / "document.pdf", b"%PDF-1.4 test")
    repo.write_json(
        root / "meta.json",
        {
            "original_filename": "statement.pdf",
            "file_size": 14,
        },
    )

    parsed_repo = JobTransactionsRepository(tmp_path)
    parsed_repo.replace_job_rows(
        job_id=job_id,
        rows_by_page={
            "page_001": [
                {
                    "row_id": "001",
                    "date": "2026-02-01",
                    "description": "Transfer AB1 received",
                    "credit": "100.00",
                    "balance": "100.00",
                },
                {
                    "row_id": "002",
                    "date": "2026-02-02",
                    "description": "Cash deposit",
                    "credit": "50.00",
                    "balance": "150.00",
                },
            ]
        },
        bounds_by_page={},
        is_manual_edit=False,
    )

    rows = parsed_repo.get_rows_by_job(job_id)["page_001"]
    assert [row["is_flagged"] for row in rows] == [False, False]

    set_bank_code_flags(
        [
            {
                "bank_id": "TEST_BANK",
                "bank_name": "TEST BANK",
                "tx_code": "AB1",
                "particulars": "",
            }
        ]
    )

    rows = parsed_repo.get_rows_by_job(job_id)["page_001"]
    assert [row["is_flagged"] for row in rows] == [True, False]


def test_job_transactions_persist_disbalance_fields(app_with_temp_data):
    _app, tmp_path = app_with_temp_data
    repo = JobsRepository(tmp_path)
    job_id = "22222222-2222-2222-2222-222222222222"
    root = repo.ensure_job_layout(job_id)
    repo.write_bytes(root / "input" / "document.pdf", b"%PDF-1.4 test")
    repo.write_json(
        root / "meta.json",
        {
            "original_filename": "statement.pdf",
            "file_size": 14,
        },
    )

    parsed_repo = JobTransactionsRepository(tmp_path)
    parsed_repo.replace_job_rows(
        job_id=job_id,
        rows_by_page={
            "page_001": [
                {
                    "row_id": "001",
                    "date": "2026-02-01",
                    "description": "Opening",
                    "credit": "100.00",
                    "balance": "100.00",
                },
                {
                    "row_id": "002",
                    "date": "2026-02-02",
                    "description": "Debit row with wrong balance",
                    "debit": "10.00",
                    "balance": "80.00",
                },
            ]
        },
        bounds_by_page={},
        is_manual_edit=False,
    )

    rows = parsed_repo.get_rows_by_job(job_id)["page_001"]
    assert rows[0]["is_disbalanced"] is False
    assert rows[1]["is_disbalanced"] is True
    assert rows[1]["disbalance_expected_balance"] == 90.0
    assert rows[1]["disbalance_delta"] == -10.0


def test_replace_job_rows_can_overwrite_existing_pages_without_duplicate_insert(app_with_temp_data):
    _app, tmp_path = app_with_temp_data
    repo = JobsRepository(tmp_path)
    job_id = "33333333-3333-3333-3333-333333333333"
    root = repo.ensure_job_layout(job_id)
    repo.write_bytes(root / "input" / "document.pdf", b"%PDF-1.4 test")
    repo.write_json(
        root / "meta.json",
        {
            "original_filename": "statement.pdf",
            "file_size": 14,
        },
    )

    parsed_repo = JobTransactionsRepository(tmp_path)
    parsed_repo.replace_job_rows(
        job_id=job_id,
        rows_by_page={
            "page_001": [
                {
                    "row_id": "001",
                    "date": "2026-02-01",
                    "description": "First pass",
                    "credit": "100.00",
                    "balance": "100.00",
                }
            ]
        },
        bounds_by_page={},
        is_manual_edit=False,
    )

    parsed_repo.replace_job_rows(
        job_id=job_id,
        rows_by_page={
            "page_001": [
                {
                    "row_id": "001",
                    "date": "2026-02-02",
                    "description": "Second pass",
                    "credit": "120.00",
                    "balance": "220.00",
                }
            ]
        },
        bounds_by_page={},
        is_manual_edit=False,
    )

    rows = parsed_repo.get_rows_by_job(job_id)["page_001"]
    assert len(rows) == 1
    assert rows[0]["description"] == "Second pass"
    assert rows[0]["credit"] == 120.0
