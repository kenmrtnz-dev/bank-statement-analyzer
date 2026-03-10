import httpx

from app.crm import service as crm_service


class StubClient:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, url, params=None, headers=None):
        self.calls.append(
            {
                "url": url,
                "params": dict(params or {}),
                "headers": dict(headers or {}),
            }
        )
        if not self._responses:
            raise AssertionError("unexpected extra GET request")
        return self._responses.pop(0)


def test_fetch_entity_batch_retries_with_minimal_select_and_caches_choice():
    crm_service._ENTITY_SELECT_CACHE.clear()
    client = StubClient(
        [
            httpx.Response(500, json={"message": "crm select failed"}),
            httpx.Response(200, json={"list": [{"id": "lead-1"}], "total": 1}),
        ]
    )

    batch, has_more = crm_service._fetch_entity_batch(
        client,
        "https://crm.example/api/v1",
        {"x-api-key": "secret"},
        entity_name="Lead",
        select_fields=crm_service.LEAD_SELECT_FIELDS,
        offset=0,
        max_size=100,
    )

    assert batch == [{"id": "lead-1"}]
    assert has_more is False
    assert client.calls[0]["params"]["select"] == crm_service.LEAD_SELECT_FIELDS
    assert client.calls[1]["params"]["select"] == crm_service.LEAD_MINIMAL_SELECT_FIELDS

    cached_client = StubClient([httpx.Response(200, json={"list": [], "total": 0})])
    batch, has_more = crm_service._fetch_entity_batch(
        cached_client,
        "https://crm.example/api/v1",
        {"x-api-key": "secret"},
        entity_name="Lead",
        select_fields=crm_service.LEAD_SELECT_FIELDS,
        offset=0,
        max_size=100,
    )

    assert batch == []
    assert has_more is False
    assert len(cached_client.calls) == 1
    assert cached_client.calls[0]["params"]["select"] == crm_service.LEAD_MINIMAL_SELECT_FIELDS


def test_fetch_entity_batch_falls_back_to_unfiltered_request_after_select_failures():
    crm_service._ENTITY_SELECT_CACHE.clear()
    client = StubClient(
        [
            httpx.Response(500, json={"message": "default select failed"}),
            httpx.Response(500, json={"message": "minimal select failed"}),
            httpx.Response(200, json={"list": [{"id": "acct-1"}], "total": 1}),
        ]
    )

    batch, has_more = crm_service._fetch_entity_batch(
        client,
        "https://crm.example/api/v1",
        {"x-api-key": "secret"},
        entity_name="Account",
        select_fields=crm_service.ACCOUNT_SELECT_FIELDS,
        offset=0,
        max_size=100,
    )

    assert batch == [{"id": "acct-1"}]
    assert has_more is False
    assert client.calls[0]["params"]["select"] == crm_service.ACCOUNT_SELECT_FIELDS
    assert client.calls[1]["params"]["select"] == crm_service.ACCOUNT_MINIMAL_SELECT_FIELDS
    assert "select" not in client.calls[2]["params"]
