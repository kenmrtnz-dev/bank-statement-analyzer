import json

from app.jobs.repository import JobResultsRawRepository, JobStateRepository, JobTransactionsRepository


def test_ui_settings_available_for_evaluator(client):
    res = client.get("/ui/settings")
    assert res.status_code == 200
    body = res.json()
    assert body.get("ok") is True
    assert isinstance(body.get("upload_testing_enabled"), bool)
    flags = body.get("bank_code_flags")
    assert isinstance(flags, list)
    assert all(isinstance(item, dict) for item in flags)
    if flags:
        assert isinstance(flags[0].get("bank"), str)
        assert isinstance(flags[0].get("codes"), list)
    flat_rows = body.get("bank_code_flag_rows")
    assert isinstance(flat_rows, list)
    if flat_rows:
        assert isinstance(flat_rows[0].get("bank_id"), str)
        assert isinstance(flat_rows[0].get("bank_name"), str)
        assert isinstance(flat_rows[0].get("tx_code"), str)


def test_admin_can_toggle_upload_testing_setting(client):
    client.post("/auth/logout")
    login_admin = client.post("/auth/login", data={"username": "admin", "password": "admin123"})
    assert login_admin.status_code == 200

    get_before = client.get("/admin/settings")
    assert get_before.status_code == 200

    set_disabled = client.post("/admin/settings/upload-testing", json={"enabled": False})
    assert set_disabled.status_code == 200
    assert set_disabled.json().get("upload_testing_enabled") is False

    get_after = client.get("/admin/settings")
    assert get_after.status_code == 200
    assert get_after.json().get("upload_testing_enabled") is False


def test_admin_can_update_bank_code_flags_and_preserve_toggle(client):
    client.post("/auth/logout")
    login_admin = client.post("/auth/login", data={"username": "admin", "password": "admin123"})
    assert login_admin.status_code == 200

    set_toggle = client.post("/admin/settings/upload-testing", json={"enabled": True})
    assert set_toggle.status_code == 200
    assert set_toggle.json().get("upload_testing_enabled") is True

    update_flags = client.post(
        "/admin/settings/bank-code-flags",
        json={
            "rows": [
                {"bank_id": "TEST_BANK", "bank_name": "TEST BANK", "tx_code": "AB1"},
                {"bank_id": "TEST_BANK", "bank_name": "TEST BANK", "tx_code": "ab1"},
            ]
        },
    )
    assert update_flags.status_code == 200
    payload = update_flags.json()
    assert payload.get("upload_testing_enabled") is True
    rows = payload.get("bank_code_flags") or []
    assert rows == [{"bank": "TEST BANK", "codes": ["AB1"], "profile_aliases": ["TEST_BANK"]}]
    flat_rows = payload.get("bank_code_flag_rows") or []
    assert flat_rows == [{"bank_id": "TEST_BANK", "bank_name": "TEST BANK", "tx_code": "AB1", "particulars": ""}]


def test_admin_can_list_users_with_management_flags(client):
    client.post("/auth/logout")
    login_admin = client.post("/auth/login", data={"username": "admin", "password": "admin123"})
    assert login_admin.status_code == 200

    res = client.get("/admin/users")
    assert res.status_code == 200
    payload = res.json()
    assert payload.get("ok") is True
    assert payload.get("count") == 2

    rows = {row["username"]: row for row in payload.get("rows", [])}
    assert rows["admin"]["role"] == "admin"
    assert rows["admin"]["is_current_user"] is True
    assert rows["admin"]["can_change_role"] is False
    assert rows["admin"]["can_delete"] is False
    assert rows["admin"]["is_last_admin"] is True

    assert rows["eval_test"]["role"] == "evaluator"
    assert rows["eval_test"]["is_current_user"] is False
    assert rows["eval_test"]["can_change_role"] is True
    assert rows["eval_test"]["can_delete"] is True


def test_admin_can_update_user_username_password_and_role(client):
    client.post("/auth/logout")
    login_admin = client.post("/auth/login", data={"username": "admin", "password": "admin123"})
    assert login_admin.status_code == 200

    update_res = client.patch(
        "/admin/users/eval_test",
        json={"username": "eval_manager", "password": "newpass9", "role": "admin"},
    )
    assert update_res.status_code == 200
    update_payload = update_res.json()
    assert update_payload.get("ok") is True
    assert update_payload["user"]["username"] == "eval_manager"
    assert update_payload["user"]["role"] == "admin"

    client.post("/auth/logout")
    old_login = client.post("/auth/login", data={"username": "eval_test", "password": "evalpass1"})
    assert old_login.status_code == 401

    new_login = client.post("/auth/login", data={"username": "eval_manager", "password": "newpass9"})
    assert new_login.status_code == 200
    assert new_login.json()["role"] == "admin"


def test_admin_can_delete_other_users_but_not_delete_or_demote_self(client):
    client.post("/auth/logout")
    login_admin = client.post("/auth/login", data={"username": "admin", "password": "admin123"})
    assert login_admin.status_code == 200

    delete_res = client.delete("/admin/users/eval_test")
    assert delete_res.status_code == 200
    assert delete_res.json().get("ok") is True

    client.post("/auth/logout")
    deleted_login = client.post("/auth/login", data={"username": "eval_test", "password": "evalpass1"})
    assert deleted_login.status_code == 401

    login_admin = client.post("/auth/login", data={"username": "admin", "password": "admin123"})
    assert login_admin.status_code == 200

    self_delete = client.delete("/admin/users/admin")
    assert self_delete.status_code == 400
    assert self_delete.json()["detail"] == "current_admin_delete_forbidden"

    self_demote = client.patch("/admin/users/admin", json={"role": "evaluator"})
    assert self_demote.status_code == 400
    assert self_demote.json()["detail"] == "current_admin_role_change_forbidden"


def test_admin_can_list_paginated_job_transactions_with_filters(client, tmp_path):
    repo = JobTransactionsRepository(tmp_path)
    repo.replace_job_rows(
        job_id="job-alpha",
        rows_by_page={
            "page_001": [
                {
                    "row_id": "001",
                    "date": "01/01/2026",
                    "description": "Alpha Deposit",
                    "debit": None,
                    "credit": 100.0,
                    "balance": 100.0,
                    "row_type": "transaction",
                },
                {
                    "row_id": "002",
                    "date": "01/02/2026",
                    "description": "Alpha Withdrawal",
                    "debit": 25.0,
                    "credit": None,
                    "balance": 75.0,
                    "row_type": "transaction",
                },
            ]
        },
        bounds_by_page={
            "page_001": [
                {"row_id": "001", "x1": 0.1, "y1": 0.2, "x2": 0.9, "y2": 0.25},
                {"row_id": "002", "x1": 0.1, "y1": 0.3, "x2": 0.9, "y2": 0.35},
            ]
        },
    )
    repo.replace_job_rows(
        job_id="job-beta",
        rows_by_page={
            "page_002": [
                {
                    "row_id": "001",
                    "date": "02/01/2026",
                    "description": "Beta Payment",
                    "debit": 10.0,
                    "credit": None,
                    "balance": 90.0,
                    "row_type": "transaction",
                }
            ]
        },
    )

    client.post("/auth/logout")
    login_admin = client.post("/auth/login", data={"username": "admin", "password": "admin123"})
    assert login_admin.status_code == 200

    filtered = client.get("/admin/job-transactions", params={"job_id": "job-alpha", "q": "deposit"})
    assert filtered.status_code == 200
    body = filtered.json()
    assert body.get("ok") is True
    assert body["pagination"]["per_page"] == 50
    assert body["pagination"]["total_rows"] == 1
    assert len(body["rows"]) == 1
    assert body["rows"][0]["job_id"] == "job-alpha"
    assert body["rows"][0]["description"] == "Alpha Deposit"
    assert body["rows"][0]["bounds"]["x1"] == 0.1

    paged = client.get("/admin/job-transactions", params={"page": 1, "limit": 1})
    assert paged.status_code == 200
    paged_body = paged.json()
    assert paged_body["pagination"]["per_page"] == 1
    assert paged_body["pagination"]["total_rows"] == 3
    assert len(paged_body["rows"]) == 1


def test_admin_clear_store_removes_job_tables_and_raw_results(client, tmp_path):
    tx_repo = JobTransactionsRepository(tmp_path)
    raw_repo = JobResultsRawRepository(tmp_path)
    state_repo = JobStateRepository(tmp_path)

    tx_repo.replace_job_rows(
        job_id="job-clear-1",
        rows_by_page={
            "page_001": [
                {
                    "row_id": "001",
                    "date": "01/01/2026",
                    "description": "Sample",
                    "debit": None,
                    "credit": 10.0,
                    "balance": 10.0,
                    "row_type": "transaction",
                }
            ]
        },
    )
    raw_repo.upsert(job_id="job-clear-1", is_ocr=False, raw_xml="<pdf/>")
    state_repo.sync_job(
        job_id="job-clear-1",
        meta={"original_filename": "clear.pdf", "file_size": 123, "is_reversed": False},
        status={"status": "done", "updated_at": "2026-03-09T00:00:00Z"},
    )

    job_root = tmp_path / "jobs" / "job-clear-1" / "input"
    job_root.mkdir(parents=True, exist_ok=True)
    (job_root / "document.pdf").write_bytes(b"%PDF-1.4\n%%EOF")
    (tmp_path / "exports" / "sample.txt").write_text("x", encoding="utf-8")

    client.post("/auth/logout")
    login_admin = client.post("/auth/login", data={"username": "admin", "password": "admin123"})
    assert login_admin.status_code == 200

    res = client.post("/admin/clear-store")
    assert res.status_code == 200
    body = res.json()
    assert body["ok"] is True
    assert body["cleared_db_rows"] == 1
    assert body["cleared_raw_rows"] == 1
    assert body["cleared_job_state_rows"] == 1

    assert tx_repo.list_rows_paginated()["pagination"]["total_rows"] == 0
    assert raw_repo.get_by_job_id("job-clear-1") is None
    assert state_repo.get_job("job-clear-1") is None


def test_admin_can_list_jobs_with_owner_and_status_filters(client, tmp_path):
    job_one = tmp_path / "jobs" / "job-one"
    (job_one / "input").mkdir(parents=True, exist_ok=True)
    (job_one / "input" / "document.pdf").write_bytes(b"%PDF-1.4\n%%EOF")
    (job_one / "meta.json").write_text(
        json.dumps(
            {
                "original_filename": "statement-a.pdf",
                "requested_mode": "auto",
                "created_by": "eval_alpha",
                "created_role": "evaluator",
                "created_at": "2026-03-01T08:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    (job_one / "status.json").write_text(
        json.dumps(
            {
                "status": "processing",
                "step": "ocr_parsing",
                "progress": 44,
                "parse_mode": "ocr",
                "updated_at": "2026-03-01T08:20:00Z",
            }
        ),
        encoding="utf-8",
    )

    job_two = tmp_path / "jobs" / "job-two"
    (job_two / "input").mkdir(parents=True, exist_ok=True)
    (job_two / "input" / "document.pdf").write_bytes(b"%PDF-1.4\n%%EOF")
    (job_two / "meta.json").write_text(
        json.dumps(
            {
                "original_filename": "statement-b.pdf",
                "requested_mode": "text",
                "created_by": "eval_bravo",
                "created_role": "evaluator",
                "created_at": "2026-03-02T10:00:00Z",
            }
        ),
        encoding="utf-8",
    )
    (job_two / "status.json").write_text(
        json.dumps(
            {
                "status": "done",
                "step": "completed",
                "progress": 100,
                "parse_mode": "text",
                "updated_at": "2026-03-02T10:40:00Z",
            }
        ),
        encoding="utf-8",
    )

    client.post("/auth/logout")
    login_admin = client.post("/auth/login", data={"username": "admin", "password": "admin123"})
    assert login_admin.status_code == 200

    res = client.get("/admin/jobs", params={"status": "done", "owner": "eval_bravo"})
    assert res.status_code == 200
    payload = res.json()
    assert payload.get("ok") is True
    assert payload["pagination"]["total_rows"] == 1
    assert len(payload["rows"]) == 1
    row = payload["rows"][0]
    assert row["job_id"] == "job-two"
    assert row["owner_username"] == "eval_bravo"
    assert row["status"] == "done"
    assert row["progress"] == 100
    assert row["has_results"] is True


def test_admin_can_view_job_results_and_download_exports(client, tmp_path):
    job_id = "job-result-1"
    job_root = tmp_path / "jobs" / job_id
    (job_root / "input").mkdir(parents=True, exist_ok=True)
    (job_root / "result").mkdir(parents=True, exist_ok=True)
    (job_root / "input" / "document.pdf").write_bytes(b"%PDF-1.4\n%%EOF")
    (job_root / "meta.json").write_text(
        json.dumps(
            {
                "original_filename": "result-source.pdf",
                "requested_mode": "text",
                "created_by": "eval_result",
                "created_role": "evaluator",
            }
        ),
        encoding="utf-8",
    )
    (job_root / "status.json").write_text(
        json.dumps(
            {
                "status": "done",
                "step": "completed",
                "progress": 100,
                "parse_mode": "text",
                "updated_at": "2026-03-03T04:30:00Z",
            }
        ),
        encoding="utf-8",
    )
    (job_root / "result" / "parsed_rows.json").write_text(
        json.dumps(
            {
                "page_001": [
                    {
                        "row_id": "001",
                        "date": "2026-03-03",
                        "description": "Deposit",
                        "debit": "",
                        "credit": "100.00",
                        "balance": "100.00",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    client.post("/auth/logout")
    login_admin = client.post("/auth/login", data={"username": "admin", "password": "admin123"})
    assert login_admin.status_code == 200

    res = client.get(f"/admin/jobs/{job_id}/result")
    assert res.status_code == 200
    payload = res.json()
    assert payload.get("ok") is True
    assert payload["job_id"] == job_id
    assert payload["results"]["ready"] is True
    assert payload["results"]["total_rows"] == 1
    assert len(payload["results"]["rows"]) == 1
    assert payload["results"]["rows"][0]["description"] == "Deposit"
    assert payload["summary"]["total_transactions"] == 1
    assert payload["summary"]["total_credit"] == 100.0
    assert payload["downloads"]["pdf"] == f"/admin/jobs/{job_id}/export/pdf"
    assert payload["downloads"]["excel"] == f"/admin/jobs/{job_id}/export/excel"

    pdf_res = client.get(f"/admin/jobs/{job_id}/export/pdf")
    assert pdf_res.status_code == 200
    assert pdf_res.headers.get("content-type", "").startswith("application/pdf")

    excel_res = client.get(f"/admin/jobs/{job_id}/export/excel")
    assert excel_res.status_code == 200
    assert "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in excel_res.headers.get(
        "content-type", ""
    )
