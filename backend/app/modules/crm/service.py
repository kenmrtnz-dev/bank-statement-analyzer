from __future__ import annotations

import base64
import datetime as dt
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

import httpx
from fastapi import HTTPException
from fastapi.responses import Response

from app.modules.jobs.repository import JobsRepository
from app.modules.jobs.service import create_job
from app.modules.jobs.service import export_excel

DEFAULT_ESPOCRM_BASE_URL = "https://staging-crm.discoverycsc.com/api/v1"
LEAD_SELECT_FIELDS = "id,accountName,cBankStatementsIds,createdAt,createdByName,assignedUserName"
ACCOUNT_SELECT_FIELDS = "id,name,cBankStatementsIds,createdAt,createdByName,assignedUserName"
DEFAULT_TIMEOUT = httpx.Timeout(25.0, connect=10.0)
ATTACHMENT_FILE_ENDPOINTS = ("Attachment", "Attachments")
DEFAULT_ATTACHMENTS_PAGE_SIZE = 25
MAX_ATTACHMENTS_PAGE_SIZE = 200
DEFAULT_ATTACHMENT_PROBE_MODE = "lazy"
DEFAULT_ATTACHMENT_CACHE_TTL_SECONDS = 90
DEFAULT_ATTACHMENT_PROBE_CONCURRENCY = 12
DEFAULT_ATTACHMENT_FILENAME_PROBE_CONCURRENCY = 6
ATTACHMENTS_PAGE_CACHE_VERSION = 3

_CACHE_LOCK = threading.Lock()
_ATTACHMENT_PAGE_CACHE: dict[tuple[str, int, int], dict[str, Any]] = {}
_ATTACHMENT_PROBE_CACHE: dict[tuple[str, str], dict[str, Any]] = {}


def _get_espocrm_settings() -> tuple[str, str]:
    base_url = str(os.getenv("ESPOCRM_BASE_URL", DEFAULT_ESPOCRM_BASE_URL) or "").strip().rstrip("/")
    api_key = str(os.getenv("ESPOCRM_API_KEY", "") or "").strip()
    if not base_url:
        raise HTTPException(status_code=500, detail="espocrm_base_url_not_configured")
    if not api_key:
        raise HTTPException(status_code=503, detail="espocrm_api_key_not_configured")
    return base_url, api_key


def _build_headers(api_key: str) -> dict[str, str]:
    return {"x-api-key": api_key}


def _extract_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("list", "items", "records", "data"):
        items = payload.get(key)
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict)]
    return []


def _collect_id_tokens(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        out: list[str] = []
        for item in raw:
            out.extend(_collect_id_tokens(item))
        return out
    if isinstance(raw, dict):
        if "id" in raw:
            return _collect_id_tokens(raw.get("id"))
        if "ids" in raw:
            return _collect_id_tokens(raw.get("ids"))
        return []

    text = str(raw).strip()
    if not text:
        return []

    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
            return _collect_id_tokens(parsed)
        except json.JSONDecodeError:
            pass

    return [part.strip() for part in text.split(",") if part and part.strip()]


def _normalize_attachment_ids(raw: Any) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for token in _collect_id_tokens(raw):
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


def _extract_filename(content_disposition: str | None) -> str | None:
    if not content_disposition:
        return None

    encoded_match = re.search(r"filename\*=([^;]+)", content_disposition, flags=re.IGNORECASE)
    if encoded_match:
        encoded_value = encoded_match.group(1).strip().strip('"')
        if "''" in encoded_value:
            _, _, encoded_part = encoded_value.partition("''")
            return unquote(encoded_part)
        return unquote(encoded_value)

    name_match = re.search(r'filename="?([^";]+)"?', content_disposition, flags=re.IGNORECASE)
    if name_match:
        return name_match.group(1).strip()
    return None


def _sanitize_filename(name: str | None, fallback: str) -> str:
    base = str(name or "").strip()
    if not base:
        base = fallback
    base = re.sub(r'[\r\n\\/:*?"<>|]+', "_", base)
    base = re.sub(r"\s+", " ", base).strip()
    return base or fallback


def _read_size_bytes(raw: str | None) -> int:
    if raw is None:
        return 0
    try:
        value = int(raw)
        return value if value >= 0 else 0
    except (TypeError, ValueError):
        return 0


def _raise_remote_http_error(prefix: str, status_code: int) -> None:
    if status_code in (401, 403):
        raise HTTPException(status_code=502, detail="espocrm_auth_failed")
    raise HTTPException(status_code=502, detail=f"{prefix}_failed_{status_code}")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _resolve_page_params(limit: int, offset: int) -> tuple[int, int]:
    safe_limit = max(1, min(MAX_ATTACHMENTS_PAGE_SIZE, int(limit)))
    safe_offset = max(0, int(offset))
    return safe_limit, safe_offset


def _resolve_probe_mode(probe: str | None) -> str:
    mode = str(probe or os.getenv("CRM_ATTACHMENT_PROBE_MODE", DEFAULT_ATTACHMENT_PROBE_MODE)).strip().lower()
    if mode in {"lazy", "eager"}:
        return mode
    return DEFAULT_ATTACHMENT_PROBE_MODE


def _normalize_search_query(raw: str | None) -> str:
    return str(raw or "").strip().lower()


def _matches_attachment_search(item: dict[str, Any], query: str) -> bool:
    if not query:
        return True
    hay = " ".join(
        [
            str(item.get("id") or ""),
            str(item.get("type") or ""),
            str(item.get("created_at") or ""),
            str(item.get("account_name") or ""),
            str(item.get("assigned_user") or ""),
            str(item.get("attachment_id") or ""),
            str(item.get("filename") or ""),
            str(item.get("process_job_id") or ""),
            str(item.get("process_status") or ""),
        ]
    ).lower()
    return query in hay


def _cache_get(cache: dict, key: Any) -> Any:
    now = time.time()
    with _CACHE_LOCK:
        item = cache.get(key)
        if not item:
            return None
        expires_at = float(item.get("expires_at") or 0)
        if expires_at and expires_at <= now:
            cache.pop(key, None)
            return None
        return item.get("value")


def _cache_set(cache: dict, key: Any, value: Any, ttl_seconds: int) -> None:
    ttl = max(1, int(ttl_seconds))
    with _CACHE_LOCK:
        cache[key] = {"value": value, "expires_at": time.time() + ttl}


def _build_attachment_file_urls(base_url: str, attachment_id: str) -> list[str]:
    encoded_id = quote(attachment_id, safe="")
    return [f"{base_url}/{entity}/file/{encoded_id}" for entity in ATTACHMENT_FILE_ENDPOINTS]


def _normalize_process_status(raw_status: Any) -> str:
    status = str(raw_status or "").strip().lower()
    if status == "done":
        return "completed"
    if status in {"queued", "processing", "completed", "failed", "needs_review"}:
        return status
    return "not_started"


def _read_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return default


def _write_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    os.replace(tmp, path)


def _load_attachment_process_index() -> dict[str, dict[str, Any]]:
    jobs_root = Path(os.getenv("DATA_DIR", "./data")) / "jobs"
    if not jobs_root.exists():
        return {}

    latest_by_attachment: dict[str, dict[str, Any]] = {}
    for job_dir in jobs_root.iterdir():
        if not job_dir.is_dir():
            continue
        meta = _read_json_file(job_dir / "meta.json", {})
        if not isinstance(meta, dict):
            continue
        attachment_id = str(meta.get("source_attachment_id") or "").strip()
        if not attachment_id:
            continue

        status_payload = _read_json_file(job_dir / "status.json", {})
        if not isinstance(status_payload, dict):
            status_payload = {}

        status_file = job_dir / "status.json"
        updated_at = int(status_file.stat().st_mtime) if status_file.exists() else int(job_dir.stat().st_mtime)
        current = latest_by_attachment.get(attachment_id)
        if current and int(current.get("_updated_at") or 0) >= updated_at:
            continue

        progress_raw = status_payload.get("progress")
        try:
            progress = int(progress_raw)
        except (TypeError, ValueError):
            progress = 0

        latest_by_attachment[attachment_id] = {
            "process_job_id": str(job_dir.name),
            "process_status": _normalize_process_status(status_payload.get("status")),
            "process_step": str(status_payload.get("step") or "").strip(),
            "process_progress": max(0, min(100, progress)),
            "_updated_at": updated_at,
        }

    for value in latest_by_attachment.values():
        value.pop("_updated_at", None)
    return latest_by_attachment


def _fetch_entity_batch(
    client: httpx.Client,
    base_url: str,
    headers: dict[str, str],
    *,
    entity_name: str,
    select_fields: str,
    offset: int,
    max_size: int,
) -> tuple[list[dict[str, Any]], bool]:
    try:
        response = client.get(
            f"{base_url}/{entity_name}",
            params={"select": select_fields, "maxSize": max_size, "offset": offset},
            headers=headers,
        )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"espocrm_{entity_name.lower()}_request_failed") from exc

    if response.status_code >= 400:
        _raise_remote_http_error(f"espocrm_{entity_name.lower()}_request", response.status_code)

    try:
        payload = response.json()
    except ValueError as exc:
        raise HTTPException(status_code=502, detail=f"espocrm_{entity_name.lower()}_response_invalid_json") from exc

    batch = _extract_records(payload)
    total = payload.get("total") if isinstance(payload, dict) else None
    if isinstance(total, int):
        has_more = offset + len(batch) < total
    else:
        has_more = len(batch) >= max_size
    return batch, has_more


def _probe_attachment_file(
    client: httpx.Client,
    base_url: str,
    headers: dict[str, str],
    attachment_id: str,
) -> dict[str, Any]:
    last_status: int | None = None
    for file_url in _build_attachment_file_urls(base_url, attachment_id):
        try:
            with client.stream("GET", file_url, headers=headers) as response:
                if response.status_code == 404:
                    last_status = 404
                    continue
                if response.status_code >= 400:
                    _raise_remote_http_error("espocrm_attachment_probe", response.status_code)

                filename = _sanitize_filename(
                    _extract_filename(response.headers.get("content-disposition")),
                    f"{attachment_id}.bin",
                )
                content_type = str(response.headers.get("content-type") or "application/octet-stream").strip()
                size_bytes = _read_size_bytes(response.headers.get("content-length"))
                return {
                    "filename": filename,
                    "content_type": content_type,
                    "size_bytes": size_bytes,
                }
        except httpx.RequestError as exc:
            raise HTTPException(status_code=502, detail="espocrm_attachment_probe_failed") from exc

    if last_status == 404:
        raise HTTPException(status_code=404, detail="espocrm_attachment_not_found")
    raise HTTPException(status_code=502, detail="espocrm_attachment_probe_failed")


def _probe_attachment_file_cached(
    base_url: str,
    headers: dict[str, str],
    attachment_id: str,
    cache_ttl_seconds: int,
) -> dict[str, Any]:
    cache_key = (base_url, attachment_id)
    cached = _cache_get(_ATTACHMENT_PROBE_CACHE, cache_key)
    if isinstance(cached, dict):
        if cached.get("ok") is True:
            return dict(cached.get("payload") or {})
        detail = str(cached.get("detail") or "espocrm_attachment_probe_failed")
        status_code = int(cached.get("status_code") or 502)
        raise HTTPException(status_code=status_code, detail=detail)

    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT, follow_redirects=True) as client:
            payload = _probe_attachment_file(client, base_url, headers, attachment_id)
        _cache_set(
            _ATTACHMENT_PROBE_CACHE,
            cache_key,
            {"ok": True, "payload": payload},
            ttl_seconds=cache_ttl_seconds,
        )
        return payload
    except HTTPException as exc:
        _cache_set(
            _ATTACHMENT_PROBE_CACHE,
            cache_key,
            {
                "ok": False,
                "status_code": int(exc.status_code),
                "detail": str(exc.detail or "espocrm_attachment_probe_failed"),
            },
            ttl_seconds=cache_ttl_seconds,
        )
        raise


def _collect_attachment_page(
    client: httpx.Client,
    base_url: str,
    headers: dict[str, str],
    limit: int,
    offset: int,
    search_query: str = "",
) -> dict[str, Any]:
    max_size = 100
    lead_offset = 0
    account_offset = 0
    lead_count = 0
    account_count = 0
    matched_seen = 0
    items: list[dict[str, Any]] = []
    has_more = False

    sources = [
        {
            "entity_name": "Lead",
            "select_fields": LEAD_SELECT_FIELDS,
            "label": "Lead",
            "offset_key": "lead_offset",
        },
        {
            "entity_name": "Account",
            "select_fields": ACCOUNT_SELECT_FIELDS,
            "label": "Business Profile",
            "offset_key": "account_offset",
        },
    ]
    source_done: set[str] = set()
    while len(source_done) < len(sources):
        progressed = False
        for source in sources:
            entity_name = str(source["entity_name"])
            if entity_name in source_done:
                continue
            current_offset = lead_offset if source["offset_key"] == "lead_offset" else account_offset
            batch, has_more_entities = _fetch_entity_batch(
                client,
                base_url,
                headers,
                entity_name=entity_name,
                select_fields=str(source["select_fields"]),
                offset=current_offset,
                max_size=max_size,
            )
            if not batch:
                source_done.add(entity_name)
                continue

            progressed = True
            if source["offset_key"] == "lead_offset":
                lead_offset += len(batch)
                lead_count += len(batch)
            else:
                account_offset += len(batch)
                account_count += len(batch)

            for record in batch:
                record_id = str(record.get("id") or "").strip()
                account_name = str(record.get("accountName") or record.get("name") or "").strip()
                assigned_user = _extract_assigned_user_name(record)
                attachment_ids = _normalize_attachment_ids(record.get("cBankStatementsIds"))
                for attachment_id in attachment_ids:
                    row = {
                        "id": record_id,
                        "type": str(source["label"]),
                        "created_at": str(record.get("createdAt") or "").strip(),
                        "account_name": account_name,
                        "assigned_user": assigned_user,
                        "attachment_id": attachment_id,
                        "filename": "",
                        "content_type": "",
                        "size_bytes": 0,
                        "status": "available",
                        "error": "",
                        "download_url": f"/crm/attachments/{quote(attachment_id, safe='')}/file",
                        "process_job_id": "",
                        "process_status": "not_started",
                        "process_step": "",
                        "process_progress": 0,
                    }
                    if not _matches_attachment_search(row, search_query):
                        continue

                    if matched_seen < offset:
                        matched_seen += 1
                        continue

                    if len(items) >= limit:
                        has_more = True
                        break

                    matched_seen += 1
                    items.append(row)
                if has_more:
                    break
            if has_more:
                break
            if not has_more_entities:
                source_done.add(entity_name)
        if has_more:
            break
        if not progressed:
            break

    next_offset = offset + len(items)
    return {
        "items": items,
        "lead_count": lead_count,
        "account_count": account_count,
        "attachment_count": len(items),
        "offset": offset,
        "limit": limit,
        "next_offset": next_offset,
        "has_more": has_more,
    }


def _extract_assigned_user_name(lead: dict[str, Any]) -> str:
    created_by = str(lead.get("createdByName") or "").strip()
    if created_by:
        return created_by
    direct = str(lead.get("assignedUserName") or "").strip()
    if direct:
        return direct
    return ""


def _normalize_business_name(raw: str | None) -> str:
    text = str(raw or "").strip().upper()
    if not text:
        return "UNKNOWNBUSINESS"
    normalized = "".join(ch for ch in text if ch.isalnum())
    return normalized or "UNKNOWNBUSINESS"


def _build_crm_export_basename(account_name: str | None, now: dt.datetime | None = None) -> str:
    clock = now or dt.datetime.now()
    ts = clock.strftime("%m%d%Y%H%M")
    business = _normalize_business_name(account_name)
    return f"{ts}-{business}-6MOS-BANKSTATEMENTS"


def _find_attachment_owner_record(
    client: httpx.Client,
    base_url: str,
    headers: dict[str, str],
    attachment_id: str,
) -> dict[str, str] | None:
    cleaned_attachment_id = str(attachment_id or "").strip()
    if not cleaned_attachment_id:
        return None

    sources = [
        {"entity_name": "Lead", "select_fields": LEAD_SELECT_FIELDS, "label": "Lead"},
        {"entity_name": "Account", "select_fields": ACCOUNT_SELECT_FIELDS, "label": "Business Profile"},
    ]

    for source in sources:
        offset = 0
        while True:
            batch, has_more_entities = _fetch_entity_batch(
                client,
                base_url,
                headers,
                entity_name=str(source["entity_name"]),
                select_fields=str(source["select_fields"]),
                offset=offset,
                max_size=100,
            )
            if not batch:
                break

            for record in batch:
                attachment_ids = _normalize_attachment_ids(record.get("cBankStatementsIds"))
                if cleaned_attachment_id not in attachment_ids:
                    continue
                return {
                    "record_id": str(record.get("id") or "").strip(),
                    "record_type": str(source["label"]),
                    "entity_name": str(source["entity_name"]),
                    "account_name": str(record.get("accountName") or record.get("name") or "").strip(),
                }

            if not has_more_entities:
                break
            offset += len(batch)

    return None


def list_bank_statement_attachments(
    limit: int = DEFAULT_ATTACHMENTS_PAGE_SIZE,
    offset: int = 0,
    probe: str | None = None,
    q: str | None = None,
) -> dict[str, Any]:
    limit, offset = _resolve_page_params(limit, offset)
    probe_mode = _resolve_probe_mode(probe)
    search_query = _normalize_search_query(q)
    cache_ttl_seconds = max(1, _env_int("CRM_ATTACHMENT_CACHE_TTL_SECONDS", DEFAULT_ATTACHMENT_CACHE_TTL_SECONDS))
    probe_concurrency = max(
        1,
        _env_int("CRM_ATTACHMENT_PROBE_CONCURRENCY", DEFAULT_ATTACHMENT_PROBE_CONCURRENCY),
    )
    filename_probe_concurrency = max(
        1,
        _env_int("CRM_ATTACHMENT_FILENAME_PROBE_CONCURRENCY", DEFAULT_ATTACHMENT_FILENAME_PROBE_CONCURRENCY),
    )

    base_url, api_key = _get_espocrm_settings()
    headers = _build_headers(api_key)
    process_index = _load_attachment_process_index()
    page_cache_key = (f"{base_url}|v{ATTACHMENTS_PAGE_CACHE_VERSION}", offset, limit, probe_mode, search_query)

    cached_page = _cache_get(_ATTACHMENT_PAGE_CACHE, page_cache_key)
    if isinstance(cached_page, dict):
        page_payload = {
            "items": [dict(item) for item in (cached_page.get("items") or [])],
            "lead_count": int(cached_page.get("lead_count") or 0),
            "account_count": int(cached_page.get("account_count") or 0),
            "attachment_count": int(cached_page.get("attachment_count") or 0),
            "offset": int(cached_page.get("offset") or offset),
            "limit": int(cached_page.get("limit") or limit),
            "next_offset": int(cached_page.get("next_offset") or (offset + limit)),
            "has_more": bool(cached_page.get("has_more")),
        }
    else:
        with httpx.Client(timeout=DEFAULT_TIMEOUT, follow_redirects=True) as client:
            page_payload = _collect_attachment_page(
                client=client,
                base_url=base_url,
                headers=headers,
                limit=limit,
                offset=offset,
                search_query=search_query,
            )
        _cache_set(_ATTACHMENT_PAGE_CACHE, page_cache_key, page_payload, ttl_seconds=cache_ttl_seconds)

    rows: list[dict[str, Any]] = [dict(item) for item in (page_payload.get("items") or [])]

    if rows:
        def _run_probe(target: dict[str, Any]) -> tuple[str, dict[str, Any] | None, HTTPException | None]:
            attachment_id = str(target.get("attachment_id") or "").strip()
            if not attachment_id:
                return "", None, HTTPException(status_code=400, detail="attachment_id_required")
            try:
                metadata = _probe_attachment_file_cached(
                    base_url=base_url,
                    headers=headers,
                    attachment_id=attachment_id,
                    cache_ttl_seconds=cache_ttl_seconds,
                )
                return attachment_id, metadata, None
            except HTTPException as exc:
                return attachment_id, None, exc

        by_attachment: dict[str, dict[str, Any]] = {str(item.get("attachment_id") or ""): item for item in rows}
        max_workers = probe_concurrency if probe_mode == "eager" else filename_probe_concurrency
        with ThreadPoolExecutor(max_workers=min(max_workers, len(rows))) as pool:
            futures = [pool.submit(_run_probe, item) for item in rows]
            for future in as_completed(futures):
                attachment_id, metadata, error = future.result()
                target = by_attachment.get(attachment_id)
                if not target:
                    continue
                if metadata is not None:
                    target.update(metadata)
                    target["status"] = "available"
                elif error is not None and probe_mode == "eager":
                    target["status"] = "unavailable"
                    target["error"] = str(error.detail or "espocrm_attachment_probe_failed")
    if probe_mode != "eager":
        for item in rows:
            attachment_id = str(item.get("attachment_id") or "").strip()
            item["filename"] = _sanitize_filename(item.get("filename"), f"{attachment_id}.pdf")
            item["content_type"] = "application/pdf"
            item["size_bytes"] = int(item.get("size_bytes") or 0)
            item["status"] = "available"

    for item in rows:
        attachment_id = str(item.get("attachment_id") or "").strip()
        process_info = process_index.get(attachment_id)
        if process_info:
            item.update(process_info)

    return {
        "items": rows,
        "lead_count": int(page_payload.get("lead_count") or 0),
        "account_count": int(page_payload.get("account_count") or 0),
        "attachment_count": len(rows),
        "offset": int(page_payload.get("offset") or offset),
        "limit": int(page_payload.get("limit") or limit),
        "next_offset": int(page_payload.get("next_offset") or (offset + len(rows))),
        "has_more": bool(page_payload.get("has_more")),
        "probe_mode": probe_mode,
        "cache_ttl_seconds": cache_ttl_seconds,
        "probe_concurrency": probe_concurrency if probe_mode == "eager" else 0,
        "filename_probe_concurrency": filename_probe_concurrency if probe_mode != "eager" else 0,
        "query": search_query,
    }


def download_bank_statement_attachment(attachment_id: str) -> Response:
    cleaned_attachment_id = str(attachment_id or "").strip()
    if not cleaned_attachment_id:
        raise HTTPException(status_code=400, detail="attachment_id_required")

    base_url, api_key = _get_espocrm_settings()
    headers = _build_headers(api_key)
    last_status: int | None = None
    response: httpx.Response | None = None
    try:
        with httpx.Client(timeout=httpx.Timeout(120.0, connect=10.0), follow_redirects=True) as client:
            for file_url in _build_attachment_file_urls(base_url, cleaned_attachment_id):
                candidate = client.get(file_url, headers=headers)
                if candidate.status_code == 404:
                    last_status = 404
                    continue
                response = candidate
                break
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail="espocrm_attachment_download_failed") from exc

    if response is None:
        if last_status == 404:
            raise HTTPException(status_code=404, detail="attachment_not_found")
        raise HTTPException(status_code=502, detail="espocrm_attachment_download_failed")

    if response.status_code in (401, 403):
        raise HTTPException(status_code=502, detail="espocrm_auth_failed")
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"espocrm_attachment_download_failed_{response.status_code}")

    filename = _sanitize_filename(
        _extract_filename(response.headers.get("content-disposition")),
        f"{cleaned_attachment_id}.bin",
    )
    media_type = str(response.headers.get("content-type") or "application/octet-stream")
    out_headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    length = response.headers.get("content-length")
    if length:
        out_headers["Content-Length"] = length

    return Response(content=response.content, media_type=media_type, headers=out_headers)


def create_job_from_attachment(attachment_id: str, requested_mode: str = "auto") -> dict[str, Any]:
    cleaned_attachment_id = str(attachment_id or "").strip()
    if not cleaned_attachment_id:
        raise HTTPException(status_code=400, detail="attachment_id_required")

    base_url, api_key = _get_espocrm_settings()
    headers = _build_headers(api_key)

    last_status: int | None = None
    response: httpx.Response | None = None
    try:
        with httpx.Client(timeout=httpx.Timeout(180.0, connect=10.0), follow_redirects=True) as client:
            for file_url in _build_attachment_file_urls(base_url, cleaned_attachment_id):
                candidate = client.get(file_url, headers=headers)
                if candidate.status_code == 404:
                    last_status = 404
                    continue
                response = candidate
                break
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail="espocrm_attachment_download_failed") from exc

    if response is None:
        if last_status == 404:
            raise HTTPException(status_code=404, detail="attachment_not_found")
        raise HTTPException(status_code=502, detail="espocrm_attachment_download_failed")

    if response.status_code in (401, 403):
        raise HTTPException(status_code=502, detail="espocrm_auth_failed")
    if response.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"espocrm_attachment_download_failed_{response.status_code}")

    source_name = _sanitize_filename(
        _extract_filename(response.headers.get("content-disposition")),
        f"{cleaned_attachment_id}.pdf",
    )
    if not source_name.lower().endswith(".pdf"):
        source_name = f"{source_name}.pdf"

    owner: dict[str, str] | None = None
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT, follow_redirects=True) as client:
            owner = _find_attachment_owner_record(client, base_url, headers, cleaned_attachment_id)
    except HTTPException:
        owner = None

    payload = create_job(
        file_bytes=response.content,
        filename=source_name,
        requested_mode=requested_mode,
        auto_start=True,
    )
    job_id = str(payload.get("job_id") or "").strip()
    if job_id:
        meta_path = Path(os.getenv("DATA_DIR", "./data")) / "jobs" / job_id / "meta.json"
        existing_meta = _read_json_file(meta_path, {})
        if not isinstance(existing_meta, dict):
            existing_meta = {}
        existing_meta["source_attachment_id"] = cleaned_attachment_id
        existing_meta["source_attachment_filename"] = source_name
        if owner:
            existing_meta["source_record_id"] = str(owner.get("record_id") or "")
            existing_meta["source_record_type"] = str(owner.get("record_type") or "")
            existing_meta["source_entity_name"] = str(owner.get("entity_name") or "")
            existing_meta["source_account_name"] = str(owner.get("account_name") or "")
            if str(owner.get("entity_name") or "").strip() == "Lead":
                existing_meta["source_lead_id"] = str(owner.get("record_id") or "")
        _write_json_file(meta_path, existing_meta)

    payload["attachment_id"] = cleaned_attachment_id
    payload["source_filename"] = source_name
    if owner:
        payload["account_name"] = str(owner.get("account_name") or "")
    if owner and str(owner.get("entity_name") or "").strip() == "Lead":
        payload["lead_id"] = str(owner.get("record_id") or "")
    return payload


def export_job_excel_to_crm_lead(job_id: str, lead_id: str | None = None) -> dict[str, Any]:
    cleaned_job_id = str(job_id or "").strip()
    if not cleaned_job_id:
        raise HTTPException(status_code=400, detail="job_id_required")

    repo = JobsRepository(os.getenv("DATA_DIR", "./data"))
    if not repo.job_dir(cleaned_job_id).exists():
        raise HTTPException(status_code=404, detail="job_not_found")

    meta = _read_json_file(repo.path(cleaned_job_id, "meta.json"), {})
    if not isinstance(meta, dict):
        meta = {}

    resolved_lead_id = str(lead_id or "").strip()
    if not resolved_lead_id:
        resolved_lead_id = str(meta.get("source_lead_id") or "").strip()
    if not resolved_lead_id:
        source_entity_name = str(meta.get("source_entity_name") or "").strip()
        source_record_id = str(meta.get("source_record_id") or "").strip()
        if source_entity_name == "Lead" and source_record_id:
            resolved_lead_id = source_record_id
    if not resolved_lead_id:
        raise HTTPException(status_code=400, detail="lead_id_required_for_crm_export")

    base_url, api_key = _get_espocrm_settings()
    auth_headers = _build_headers(api_key)

    workbook_bytes, _ = export_excel(cleaned_job_id)
    account_name = str(meta.get("source_account_name") or "").strip()
    source_attachment_id = str(meta.get("source_attachment_id") or "").strip()
    if not account_name and source_attachment_id:
        try:
            with httpx.Client(timeout=DEFAULT_TIMEOUT, follow_redirects=True) as client:
                owner = _find_attachment_owner_record(client, base_url, auth_headers, source_attachment_id)
            if owner:
                account_name = str(owner.get("account_name") or "").strip()
                meta["source_account_name"] = account_name
                if not meta.get("source_record_id"):
                    meta["source_record_id"] = str(owner.get("record_id") or "")
                if not meta.get("source_record_type"):
                    meta["source_record_type"] = str(owner.get("record_type") or "")
                if not meta.get("source_entity_name"):
                    meta["source_entity_name"] = str(owner.get("entity_name") or "")
                if str(owner.get("entity_name") or "").strip() == "Lead" and not meta.get("source_lead_id"):
                    meta["source_lead_id"] = str(owner.get("record_id") or "")
                repo.write_json(repo.path(cleaned_job_id, "meta.json"), meta)
        except HTTPException:
            pass

    safe_filename = f"{_build_crm_export_basename(account_name)}.xlsx"

    mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    encoded = base64.b64encode(workbook_bytes).decode("ascii").replace("\n", "").replace("\r", "")
    file_payload = f"data:{mime};base64,{encoded}"

    headers = dict(auth_headers)
    headers["Content-Type"] = "application/json"

    attachment_body = {
        "role": "Attachment",
        "parentType": "Lead",
        "field": "cBankStatementResult",
        "name": safe_filename,
        "size": len(workbook_bytes),
        "type": mime,
        "file": file_payload,
    }

    attachment_id = ""
    try:
        with httpx.Client(timeout=httpx.Timeout(120.0, connect=10.0), follow_redirects=True) as client:
            upload_res = client.post(
                f"{base_url}/Attachment",
                headers=headers,
                json=attachment_body,
            )
            if upload_res.status_code in (401, 403):
                raise HTTPException(status_code=502, detail="espocrm_auth_failed")
            if upload_res.status_code >= 400:
                raise HTTPException(status_code=502, detail=f"espocrm_attachment_create_failed_{upload_res.status_code}")
            try:
                upload_payload = upload_res.json()
            except ValueError as exc:
                raise HTTPException(status_code=502, detail="espocrm_attachment_create_invalid_json") from exc

            attachment_id = str((upload_payload or {}).get("id") or "").strip()
            if not attachment_id:
                raise HTTPException(status_code=502, detail="espocrm_attachment_id_missing")

            lead_update_body = {
                "cBankStatementResultIds": [attachment_id],
                "cBankStatementResultNames": {
                    attachment_id: safe_filename,
                },
                "cBankStatementResultTypes": {
                    attachment_id: mime,
                },
            }

            update_res = client.put(
                f"{base_url}/Lead/{quote(resolved_lead_id, safe='')}",
                headers=headers,
                json=lead_update_body,
            )
            if update_res.status_code in (401, 403):
                raise HTTPException(status_code=502, detail="espocrm_auth_failed")
            if update_res.status_code >= 400:
                raise HTTPException(status_code=502, detail=f"espocrm_lead_update_failed_{update_res.status_code}")
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail="espocrm_export_request_failed") from exc

    return {
        "job_id": cleaned_job_id,
        "lead_id": resolved_lead_id,
        "attachment_id": attachment_id,
        "filename": safe_filename,
        "mime_type": mime,
        "size": len(workbook_bytes),
    }


__all__ = [
    "create_job_from_attachment",
    "download_bank_statement_attachment",
    "export_job_excel_to_crm_lead",
    "list_bank_statement_attachments",
]
