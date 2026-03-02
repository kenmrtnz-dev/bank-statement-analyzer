from app.modules.jobs.repository import JobTransactionsRepository


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
