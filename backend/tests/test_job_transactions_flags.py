import logging

from app.admin.service import set_bank_code_flags
from app.jobs.repository import JobStateRepository, JobTransactionsRepository, JobsRepository


def _prepare_job(tmp_path, job_id: str, *, is_reversed: bool = False) -> JobTransactionsRepository:
    repo = JobsRepository(tmp_path)
    root = repo.ensure_job_layout(job_id)
    repo.write_bytes(root / "input" / "document.pdf", b"%PDF-1.4 test")
    meta = {
        "original_filename": "statement.pdf",
        "file_size": 14,
        "is_reversed": is_reversed,
    }
    repo.write_json(root / "meta.json", meta)
    JobStateRepository(tmp_path).sync_job(job_id=job_id, meta=meta, status={"status": "queued", "pages": 1})
    return JobTransactionsRepository(tmp_path)


def test_job_transactions_persist_and_backfill_is_flagged(app_with_temp_data):
    _app, tmp_path = app_with_temp_data
    job_id = "11111111-1111-1111-1111-111111111111"
    parsed_repo = _prepare_job(tmp_path, job_id)
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
    job_id = "22222222-2222-2222-2222-222222222222"
    parsed_repo = _prepare_job(tmp_path, job_id)
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


def test_job_transactions_respect_reversed_statement_order(app_with_temp_data, caplog):
    _app, tmp_path = app_with_temp_data
    job_id = "44444444-4444-4444-4444-444444444444"
    parsed_repo = _prepare_job(tmp_path, job_id, is_reversed=True)
    caplog.set_level(logging.DEBUG, logger="app.jobs.repository")

    parsed_repo.replace_job_rows(
        job_id=job_id,
        rows_by_page={
            "page_001": [
                {
                    "row_id": "001",
                    "date": "2026-02-02",
                    "description": "Latest debit",
                    "debit": "10.00",
                    "balance": "90.00",
                },
                {
                    "row_id": "002",
                    "date": "2026-02-01",
                    "description": "Older credit",
                    "credit": "100.00",
                    "balance": "100.00",
                },
            ]
        },
        bounds_by_page={},
        is_manual_edit=False,
    )

    rows = parsed_repo.get_rows_by_job(job_id)["page_001"]
    assert [row["is_disbalanced"] for row in rows] == [False, False]
    assert any(
        "page_number=1" in record.message
        and "row_index=1" in record.message
        and "previous_balance=100.00" in record.message
        and "debit=10.00" in record.message
        and "credit=None" in record.message
        and "expected_balance=90.00" in record.message
        and "actual_balance=90.00" in record.message
        and "difference=0.00" in record.message
        for record in caplog.records
    )


def test_job_transactions_require_reversal_for_newest_first_rows(app_with_temp_data):
    _app, tmp_path = app_with_temp_data
    job_id = "55555555-5555-5555-5555-555555555555"
    parsed_repo = _prepare_job(tmp_path, job_id, is_reversed=False)

    parsed_repo.replace_job_rows(
        job_id=job_id,
        rows_by_page={
            "page_001": [
                {
                    "row_id": "001",
                    "date": "2026-02-02",
                    "description": "Latest debit",
                    "debit": "10.00",
                    "balance": "90.00",
                },
                {
                    "row_id": "002",
                    "date": "2026-02-01",
                    "description": "Older credit",
                    "credit": "100.00",
                    "balance": "100.00",
                },
            ]
        },
        bounds_by_page={},
        is_manual_edit=False,
    )

    rows = parsed_repo.get_rows_by_job(job_id)["page_001"]
    assert rows[0]["is_disbalanced"] is False
    assert rows[1]["is_disbalanced"] is True
    assert rows[1]["disbalance_expected_balance"] == 190.0
    assert rows[1]["disbalance_delta"] == -90.0


def test_job_transactions_pass_for_chronological_rows(app_with_temp_data):
    _app, tmp_path = app_with_temp_data
    job_id = "66666666-6666-6666-6666-666666666666"
    parsed_repo = _prepare_job(tmp_path, job_id, is_reversed=False)

    parsed_repo.replace_job_rows(
        job_id=job_id,
        rows_by_page={
            "page_001": [
                {
                    "row_id": "001",
                    "date": "2026-02-01",
                    "description": "Opening credit",
                    "credit": "100.00",
                    "balance": "100.00",
                },
                {
                    "row_id": "002",
                    "date": "2026-02-02",
                    "description": "Debit row",
                    "debit": "10.00",
                    "balance": "90.00",
                },
            ]
        },
        bounds_by_page={},
        is_manual_edit=False,
    )

    rows = parsed_repo.get_rows_by_job(job_id)["page_001"]
    assert [row["is_disbalanced"] for row in rows] == [False, False]


def test_job_transactions_validate_across_page_boundaries(app_with_temp_data):
    _app, tmp_path = app_with_temp_data
    job_id = "77777777-7777-7777-7777-777777777777"
    parsed_repo = _prepare_job(tmp_path, job_id, is_reversed=False)

    parsed_repo.replace_job_rows(
        job_id=job_id,
        rows_by_page={
            "page_001": [
                {
                    "row_id": "001",
                    "date": "2026-02-01",
                    "description": "Deposit",
                    "credit": "100.00",
                    "balance": "100.00",
                }
            ],
            "page_002": [
                {
                    "row_id": "001",
                    "date": "2026-02-02",
                    "description": "ATM withdrawal",
                    "debit": "10.00",
                    "balance": "90.00",
                }
            ],
        },
        bounds_by_page={},
        is_manual_edit=False,
    )

    rows_by_page = parsed_repo.get_rows_by_job(job_id)
    assert rows_by_page["page_001"][0]["is_disbalanced"] is False
    assert rows_by_page["page_002"][0]["is_disbalanced"] is False


def test_job_transactions_apply_tolerance_to_minor_decimal_differences(app_with_temp_data):
    _app, tmp_path = app_with_temp_data
    job_id = "88888888-8888-8888-8888-888888888888"
    parsed_repo = _prepare_job(tmp_path, job_id, is_reversed=False)

    parsed_repo.replace_job_rows(
        job_id=job_id,
        rows_by_page={
            "page_001": [
                {
                    "row_id": "001",
                    "date": "2026-02-01",
                    "description": "Deposit",
                    "credit": "100.00",
                    "balance": "100.00",
                },
                {
                    "row_id": "002",
                    "date": "2026-02-02",
                    "description": "Within tolerance",
                    "debit": "10.00",
                    "balance": "90.01",
                },
                {
                    "row_id": "003",
                    "date": "2026-02-03",
                    "description": "Outside tolerance",
                    "debit": "10.00",
                    "balance": "79.99",
                },
            ]
        },
        bounds_by_page={},
        is_manual_edit=False,
    )

    rows = parsed_repo.get_rows_by_job(job_id)["page_001"]
    assert rows[1]["is_disbalanced"] is False
    assert rows[2]["is_disbalanced"] is True
    assert rows[2]["disbalance_expected_balance"] == 80.01
    assert rows[2]["disbalance_delta"] == -0.02


def test_job_transactions_use_opening_balance_rows_as_anchors(app_with_temp_data):
    _app, tmp_path = app_with_temp_data
    job_id = "99999999-9999-9999-9999-999999999999"
    parsed_repo = _prepare_job(tmp_path, job_id, is_reversed=False)

    parsed_repo.replace_job_rows(
        job_id=job_id,
        rows_by_page={
            "page_001": [
                {
                    "row_id": "001",
                    "date": "2026-02-01",
                    "description": "Beginning balance",
                    "balance": "100.00",
                    "row_type": "opening_balance",
                },
                {
                    "row_id": "002",
                    "date": "2026-02-02",
                    "description": "Withdrawal",
                    "debit": "10.00",
                    "balance": "90.00",
                },
            ]
        },
        bounds_by_page={},
        is_manual_edit=False,
    )

    rows = parsed_repo.get_rows_by_job(job_id)["page_001"]
    assert rows[0]["is_disbalanced"] is False
    assert rows[0]["disbalance_expected_balance"] is None
    assert rows[0]["disbalance_delta"] is None
    assert rows[1]["is_disbalanced"] is False


def test_replace_job_rows_can_overwrite_existing_pages_without_duplicate_insert(app_with_temp_data):
    _app, tmp_path = app_with_temp_data
    job_id = "33333333-3333-3333-3333-333333333333"
    parsed_repo = _prepare_job(tmp_path, job_id)
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
