def test_ui_settings_available_for_evaluator(client):
    res = client.get("/ui/settings")
    assert res.status_code == 200
    body = res.json()
    assert body.get("ok") is True
    assert isinstance(body.get("upload_testing_enabled"), bool)


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
