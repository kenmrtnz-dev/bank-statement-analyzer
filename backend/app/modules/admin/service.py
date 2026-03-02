from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile
from typing import Any

from app.modules.jobs.repository import BankCodeFlagsRepository, JobTransactionsRepository

DEFAULT_BANK_CODE_FLAGS = [
    {
        "bank": "BDO",
        "codes": [
            "ASC",
            "CAJ",
            "CD",
            "CK",
            "CM",
            "DAJ",
            "DEPN",
            "DM",
            "DRT",
            "INT",
            "ITA",
            "SC",
            "TRA",
            "WD",
            "WDA",
            "WDC",
            "WDN",
            "WT",
            "ZBC",
            "ZBD",
        ],
        "profile_aliases": ["BDO"],
    },
    {
        "bank": "BPI",
        "codes": [
            "CKL",
            "CMG",
            "CSD",
            "DMG",
            "INT",
            "NCK",
            "NCS",
            "NOS",
            "NRC",
            "OSD",
            "CKR",
            "NEC",
            "WDL",
            "TAX",
        ],
        "profile_aliases": ["BPI"],
    },
    {
        "bank": "METROBANK",
        "codes": [
            "CL",
            "CM",
            "CR",
            "DM",
            "DP",
            "DX",
            "FW",
            "IE",
            "OC",
            "OK",
            "SC",
            "SL",
            "SR",
            "TX",
            "WL",
        ],
        "profile_aliases": ["METROBANK"],
    },
    {
        "bank": "AUB",
        "codes": [
            "ATM",
            "ATMWD",
            "BCD",
            "BCK",
            "CD",
            "CK",
            "CM",
            "DM",
            "DRT",
            "ENC",
            "HRT",
            "ICC",
            "INT",
            "LCK",
            "LMC",
            "ONUS",
            "OTC",
            "RAM",
            "RBCD",
            "RBCK",
            "RCD",
            "RCK",
            "RCM",
            "RCRT",
            "RDM",
            "RDRT",
            "RENC",
            "RICC",
            "RONUS",
            "ROTC",
            "SC",
            "TAX",
            "TFC",
            "TFD",
        ],
        "profile_aliases": ["AUB"],
    },
    {
        "bank": "RCBC",
        "codes": [
            "AC",
            "AF",
            "AW",
            "BL",
            "BP",
            "CB",
            "CC",
            "CD",
            "CE",
            "CM",
            "CT",
            "DA",
            "DM",
            "DT",
            "FT",
            "FX",
            "HC",
            "IC",
            "IE",
            "IN",
            "IP",
            "LD",
            "LO",
            "MB",
            "MC",
            "OU",
            "PN",
            "PP",
            "PY",
            "RC",
            "RI",
        ],
        "profile_aliases": ["RCBC"],
    },
    {
        "bank": "SECB",
        "codes": [
            "ATMC",
            "ATPO",
            "ATRC",
            "ATWD",
            "BPMT",
            "CHKD",
            "CHKE",
            "CMGN",
            "CSHD",
            "CSWD",
            "DFCH",
            "DHRC",
            "DMGN",
            "DRMC",
            "FTFR",
            "FTRD",
            "ICC",
            "INRT",
            "LIND",
            "MCBT",
            "MINB",
            "NCHK",
            "OBFC",
            "OLIC",
            "OUSD",
            "RCOC",
            "RCOT",
            "TDMT",
            "TDPL",
            "WTAX",
        ],
        "profile_aliases": ["SECB", "SECURITY_BANK"],
    },
]

_PROFILE_ALIASES_BY_BANK = {
    "AUB": ["AUB"],
    "ASIA UNITED BANK (AUB)": ["AUB"],
    "BDO": ["BDO"],
    "BPI": ["BPI"],
    "BANK OF THE PHILIPPINE ISLAND (BPI)": ["BPI"],
    "BANK OF THE PHILIPPINES ISLAND (BPI)": ["BPI"],
    "CHINABANK": ["CHINABANK"],
    "METROBANK": ["METROBANK"],
    "RCBC": ["RCBC"],
    "RIZAL COMMERCIAL BANKING CORPORATION": ["RCBC"],
    "SECB": ["SECB", "SECURITY_BANK"],
    "SECURITY BANK": ["SECB", "SECURITY_BANK"],
}

def _data_dir() -> Path:
    return Path(os.getenv("DATA_DIR", "./data"))


def _settings_file() -> Path:
    return _data_dir() / "config" / "admin_settings.json"


def _normalize_profile_alias(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    text = text.replace(" ", "_")
    return text


def _normalize_bank(value: Any) -> str:
    return str(value or "").strip().upper()


def _normalize_bank_code(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    return "".join(ch for ch in text if ch.isalnum() or ch in {"_", "-"})


def _default_profile_aliases(bank: str) -> list[str]:
    aliases = _PROFILE_ALIASES_BY_BANK.get(bank) or _PROFILE_ALIASES_BY_BANK.get(bank.replace("_", " ")) or [bank]
    out: list[str] = []
    seen: set[str] = set()
    for alias in aliases:
        normalized = _normalize_profile_alias(alias)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
    return out


def _default_bank_id(bank: str) -> str:
    normalized = _normalize_bank(bank)
    if not normalized:
        return ""
    ordered_banks = [
        "BDO",
        "BPI",
        "METROBANK",
        "AUB",
        "RCBC",
        "SECB",
        "CHINABANK",
    ]
    if normalized in ordered_banks:
        return str(ordered_banks.index(normalized) + 1)
    return ""


def _seed_workbook_path() -> Path | None:
    configured = str(os.getenv("BANK_CODE_SEED_XLSX") or "").strip()
    if configured:
        candidate = Path(configured).expanduser()
        return candidate if candidate.exists() else None
    data_dir_candidate = _data_dir() / "config" / "bank_code_seed.xlsx"
    if data_dir_candidate.exists():
        return data_dir_candidate
    fallback = Path.home() / "Downloads" / "BANK TRANSACTION CODE (1).xlsx"
    return fallback if fallback.exists() else None


def _normalize_bank_code_flags(raw_rows: Any, *, fallback: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    candidate_rows = raw_rows if isinstance(raw_rows, list) else fallback or []
    normalized_rows: list[dict[str, Any]] = []

    for item in candidate_rows:
        if not isinstance(item, dict):
            continue
        bank = _normalize_bank(item.get("bank"))
        if not bank:
            continue

        raw_codes = item.get("codes")
        if isinstance(raw_codes, str):
            split_codes = [part.strip() for part in raw_codes.replace("\n", ",").split(",")]
        elif isinstance(raw_codes, list):
            split_codes = [str(part or "").strip() for part in raw_codes]
        else:
            split_codes = []

        codes: list[str] = []
        seen_codes: set[str] = set()
        for value in split_codes:
            code = _normalize_bank_code(value)
            if not code or code in seen_codes:
                continue
            seen_codes.add(code)
            codes.append(code)
        if not codes:
            continue

        raw_aliases = item.get("profile_aliases")
        aliases: list[str] = []
        if isinstance(raw_aliases, list):
            for value in raw_aliases:
                alias = _normalize_profile_alias(value)
                if alias and alias not in aliases:
                    aliases.append(alias)
        if not aliases:
            aliases = _default_profile_aliases(bank)

        normalized_rows.append({"bank": bank, "codes": codes, "profile_aliases": aliases})

    return normalized_rows


def _legacy_bank_code_flag_rows(raw_rows: Any, *, fallback: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    grouped_rows = _normalize_bank_code_flags(raw_rows, fallback=fallback)
    output: list[dict[str, Any]] = []
    for bank_index, item in enumerate(grouped_rows, start=1):
        bank_name = _normalize_bank(item.get("bank"))
        if not bank_name:
            continue
        bank_id = str(bank_index)
        for code in item.get("codes") or []:
            tx_code = _normalize_bank_code(code)
            if not tx_code:
                continue
            output.append(
                {
                    "bank_id": bank_id,
                    "bank_name": bank_name,
                    "tx_code": tx_code,
                    "particulars": "",
                }
            )
    return output


def _read_seed_bank_code_rows_from_workbook() -> list[dict[str, Any]]:
    workbook_path = _seed_workbook_path()
    if not workbook_path:
        return []

    ns = {
        "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }
    def cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
        cell_type = str(cell.attrib.get("t") or "")
        if cell_type == "inlineStr":
            inline_node = cell.find("a:is", ns)
            if inline_node is None:
                return ""
            return "".join(str(node.text or "") for node in inline_node.iterfind(".//a:t", ns))
        value_node = cell.find("a:v", ns)
        if value_node is None:
            return ""
        raw = str(value_node.text or "")
        if cell_type == "s":
            try:
                idx = int(raw)
            except ValueError:
                return raw
            return shared_strings[idx] if 0 <= idx < len(shared_strings) else raw
        return raw

    try:
        with ZipFile(workbook_path) as archive:
            shared_strings: list[str] = []
            if "xl/sharedStrings.xml" in archive.namelist():
                root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
                for item in root.findall("a:si", ns):
                    shared_strings.append("".join(str(node.text or "") for node in item.iterfind(".//a:t", ns)))

            workbook_root = ET.fromstring(archive.read("xl/workbook.xml"))
            rel_root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
            rel_map = {str(rel.attrib.get("Id") or ""): str(rel.attrib.get("Target") or "") for rel in rel_root}

            rows_out: list[dict[str, Any]] = []
            sheets_root = workbook_root.find("a:sheets", ns)
            if sheets_root is None:
                return []

            for sheet_index, sheet in enumerate(sheets_root, start=1):
                sheet_name = str(sheet.attrib.get("name") or "").strip()
                rel_id = str(sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id") or "")
                target = rel_map.get(rel_id)
                if not sheet_name or not target:
                    continue
                sheet_xml = ET.fromstring(archive.read("xl/" + target.lstrip("/")))
                bank_id = str(sheet_index)
                bank_name = ""
                for row_index, row in enumerate(sheet_xml.findall(".//a:sheetData/a:row", ns), start=1):
                    values = [str(cell_value(cell, shared_strings) or "").strip() for cell in row.findall("a:c", ns)]
                    if not values:
                        continue
                    if row_index == 1:
                        bank_name = _normalize_bank(values[0])
                        continue
                    if row_index == 2:
                        continue
                    tx_code = _normalize_bank_code(values[0] if len(values) > 0 else "")
                    particulars = str(values[1] if len(values) > 1 else "").strip()
                    if not bank_id or not bank_name or not tx_code:
                        continue
                    rows_out.append(
                        {
                            "bank_id": bank_id,
                            "bank_name": bank_name,
                            "tx_code": tx_code,
                            "particulars": particulars,
                        }
                    )
            return rows_out
    except Exception:
        return []


def _group_bank_code_flag_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for item in rows or []:
        if not isinstance(item, dict):
            continue
        bank_id = _normalize_profile_alias(item.get("bank_id"))
        bank_name = _normalize_bank(item.get("bank_name"))
        tx_code = _normalize_bank_code(item.get("tx_code"))
        particulars = str(item.get("particulars") or "").strip()
        if not bank_id or not bank_name or not tx_code:
            continue
        entry = grouped.setdefault(
            bank_id,
            {
                "bank": bank_name,
                "codes": [],
                "profile_aliases": _default_profile_aliases(bank_name),
            },
        )
        if tx_code not in entry["codes"]:
            entry["codes"].append(tx_code)
        if particulars:
            code_particulars = entry.setdefault("particulars_by_code", {})
            particulars_list = code_particulars.setdefault(tx_code, [])
            if particulars not in particulars_list:
                particulars_list.append(particulars)
    output = list(grouped.values())
    for entry in output:
        entry["codes"].sort()
        particulars_by_code = entry.get("particulars_by_code")
        if isinstance(particulars_by_code, dict):
            for code in list(particulars_by_code.keys()):
                particulars_by_code[code] = sorted(str(item) for item in particulars_by_code[code])
    output.sort(key=lambda item: str(item.get("bank") or ""))
    return output


def _read_settings_payload() -> dict[str, Any]:
    path = _settings_file()
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _normalize_settings_payload(raw: dict[str, Any] | None) -> dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    return {
        "upload_testing_enabled": bool(payload.get("upload_testing_enabled", False)),
        "bank_code_flags": _normalize_bank_code_flags(payload.get("bank_code_flags"), fallback=[]),
    }


def _write_settings_payload(payload: dict[str, Any]) -> dict[str, Any]:
    path = _settings_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    os.replace(tmp, path)
    return payload


def _load_bank_code_flag_rows(settings_payload: dict[str, Any]) -> list[dict[str, Any]]:
    repo = BankCodeFlagsRepository(_data_dir())
    existing_rows = repo.list_rows()
    seed_rows = _read_seed_bank_code_rows_from_workbook()
    if seed_rows:
        return repo.seed_rows(seed_rows)
    # If the workbook is unavailable, do not regress an already-populated DB back to
    # older JSON fallback data. Keep the existing DB rows as the source of truth.
    if existing_rows:
        return existing_rows
    seed_rows = _legacy_bank_code_flag_rows(settings_payload.get("bank_code_flags"), fallback=DEFAULT_BANK_CODE_FLAGS)
    if not seed_rows:
        seed_rows = _legacy_bank_code_flag_rows(None, fallback=DEFAULT_BANK_CODE_FLAGS)
    return repo.seed_rows(seed_rows)


def get_ui_settings() -> dict:
    payload = _normalize_settings_payload(_read_settings_payload())
    rows = _load_bank_code_flag_rows(payload)
    return {
        "upload_testing_enabled": bool(payload.get("upload_testing_enabled", False)),
        "bank_code_flags": _group_bank_code_flag_rows(rows),
        "bank_code_flag_rows": rows,
    }


def set_upload_testing_enabled(enabled: bool) -> dict:
    payload = _normalize_settings_payload(_read_settings_payload())
    payload["upload_testing_enabled"] = bool(enabled)
    _write_settings_payload(payload)
    return get_ui_settings()


def set_bank_code_flags(rows: list[dict[str, Any]]) -> dict:
    BankCodeFlagsRepository(_data_dir()).replace_all(rows)
    return get_ui_settings()


def list_job_transactions(
    *,
    page: int = 1,
    limit: int = 50,
    job_id: str | None = None,
    page_key: str | None = None,
    search: str | None = None,
) -> dict[str, Any]:
    repo = JobTransactionsRepository(_data_dir())
    return repo.list_rows_paginated(
        page=page,
        limit=limit,
        job_id=str(job_id or "").strip() or None,
        page_key=str(page_key or "").strip() or None,
        search=str(search or "").strip() or None,
    )


def clear_jobs_and_exports() -> dict:
    root = _data_dir()
    jobs_dir = root / "jobs"
    exports_dir = root / "exports"
    cleared_db_rows = JobTransactionsRepository(root).clear_all()

    removed_jobs = 0
    if jobs_dir.exists():
        for item in jobs_dir.iterdir():
            if item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
                removed_jobs += 1

    removed_exports = 0
    if exports_dir.exists():
        for item in exports_dir.iterdir():
            if item.is_file():
                item.unlink(missing_ok=True)
                removed_exports += 1
            elif item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
                removed_exports += 1

    jobs_dir.mkdir(parents=True, exist_ok=True)
    exports_dir.mkdir(parents=True, exist_ok=True)
    return {"cleared_jobs": removed_jobs, "cleared_exports": removed_exports, "cleared_db_rows": cleared_db_rows}
