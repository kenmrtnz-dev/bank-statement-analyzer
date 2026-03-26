import json
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

from app.paths import get_data_dir


@dataclass(frozen=True)
class BankProfile:
    name: str
    date_tokens: List[str]
    description_tokens: List[str]
    debit_tokens: List[str]
    credit_tokens: List[str]
    balance_tokens: List[str]
    date_order: List[str]
    noise_tokens: List[str]
    account_name_patterns: List[str]
    account_number_patterns: List[str]


@dataclass(frozen=True)
class DetectionRule:
    profile: str
    contains_any: List[str]
    contains_all: List[str]


def _default_packaged_config_path() -> Path:
    return Path(__file__).with_name("bank_profiles.json")


def _config_path() -> Path:
    configured = os.getenv("BANK_PROFILES_CONFIG", "").strip()
    if configured:
        return Path(configured)
    data_dir = get_data_dir()
    return data_dir / "config" / "bank_profiles.json"


def _normalize_items(values: List[str]) -> List[str]:
    return [str(v).strip().lower() for v in values if str(v).strip()]


def _normalize_capture_patterns(values: List[str]) -> List[str]:
    normalized: List[str] = []
    for raw in values:
        text = str(raw).strip()
        if not text:
            continue
        try:
            compiled = re.compile(text, flags=re.IGNORECASE | re.MULTILINE)
        except re.error:
            continue
        if compiled.groups < 1:
            text = f"({text})"
            try:
                re.compile(text, flags=re.IGNORECASE | re.MULTILINE)
            except re.error:
                continue
        normalized.append(text)
    return normalized


def _load_json_file(path: Path) -> dict:
    with path.open() as f:
        data = json.load(f)
    if isinstance(data, dict):
        return data
    return {}


def _merge_packaged_defaults(active: dict, packaged: dict) -> dict:
    merged = dict(active)

    active_profiles = dict(active.get("profiles") or {})
    packaged_profiles = dict(packaged.get("profiles") or {})
    for name, raw in packaged_profiles.items():
        if name not in active_profiles:
            active_profiles[name] = raw
    merged["profiles"] = active_profiles

    active_rules = list(active.get("detection_rules") or [])
    packaged_rules = list(packaged.get("detection_rules") or [])
    existing_keys = {
        (
            str(rule.get("profile", "")).strip(),
            tuple(_normalize_items(rule.get("contains_any", []))),
            tuple(_normalize_items(rule.get("contains_all", []))),
        )
        for rule in active_rules
        if isinstance(rule, dict)
    }
    for rule in packaged_rules:
        if not isinstance(rule, dict):
            continue
        key = (
            str(rule.get("profile", "")).strip(),
            tuple(_normalize_items(rule.get("contains_any", []))),
            tuple(_normalize_items(rule.get("contains_all", []))),
        )
        if key in existing_keys:
            continue
        active_rules.append(rule)
        existing_keys.add(key)
    merged["detection_rules"] = active_rules

    return merged


def _load_profiles_config() -> Tuple[Dict[str, BankProfile], List[DetectionRule]]:
    global ACTIVE_CONFIG_PATH
    path = _config_path()
    if not path.exists():
        packaged = _default_packaged_config_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            if packaged.exists():
                shutil.copyfile(packaged, path)
        except Exception:
            if packaged.exists():
                path = packaged
    if not path.exists():
        raise RuntimeError(f"bank_profiles_config_missing:{path}")
    ACTIVE_CONFIG_PATH = path

    data = _load_json_file(path)
    packaged = _default_packaged_config_path()
    if packaged.exists() and packaged != path:
        try:
            data = _merge_packaged_defaults(data, _load_json_file(packaged))
        except Exception:
            pass

    raw_profiles = data.get("profiles", {})
    raw_rules = data.get("detection_rules", [])
    if "GENERIC" not in raw_profiles:
        raise RuntimeError("bank_profiles_config_invalid:GENERIC_profile_required")

    profiles: Dict[str, BankProfile] = {}
    for name, raw in raw_profiles.items():
        profiles[name] = BankProfile(
            name=name,
            date_tokens=_normalize_items(raw.get("date_tokens", [])),
            description_tokens=_normalize_items(raw.get("description_tokens", [])),
            debit_tokens=_normalize_items(raw.get("debit_tokens", [])),
            credit_tokens=_normalize_items(raw.get("credit_tokens", [])),
            balance_tokens=_normalize_items(raw.get("balance_tokens", [])),
            date_order=[str(v).strip().lower() for v in raw.get("date_order", []) if str(v).strip()],
            noise_tokens=_normalize_items(raw.get("noise_tokens", [])),
            account_name_patterns=_normalize_capture_patterns(raw.get("account_name_patterns", [])),
            account_number_patterns=_normalize_capture_patterns(raw.get("account_number_patterns", [])),
        )

    rules: List[DetectionRule] = []
    for raw in raw_rules:
        profile = str(raw.get("profile", "")).strip()
        if profile not in profiles:
            continue
        rules.append(
            DetectionRule(
                profile=profile,
                contains_any=_normalize_items(raw.get("contains_any", [])),
                contains_all=_normalize_items(raw.get("contains_all", [])),
            )
        )

    return profiles, rules


PROFILES: Dict[str, BankProfile] = {}
DETECTION_RULES: List[DetectionRule] = []
ACTIVE_CONFIG_PATH: Path = _config_path()


def reload_profiles() -> Tuple[Dict[str, BankProfile], List[DetectionRule]]:
    profiles, rules = _load_profiles_config()
    PROFILES.clear()
    PROFILES.update(profiles)
    DETECTION_RULES.clear()
    DETECTION_RULES.extend(rules)
    return PROFILES, DETECTION_RULES


reload_profiles()


def _matches_rule(text: str, rule: DetectionRule) -> bool:
    if rule.contains_all and not all(token in text for token in rule.contains_all):
        return False
    if rule.contains_any and not any(token in text for token in rule.contains_any):
        return False
    return bool(rule.contains_all or rule.contains_any)


def detect_bank_profile(page_text: str) -> BankProfile:
    lower = (page_text or "").lower()
    for rule in DETECTION_RULES:
        if _matches_rule(lower, rule):
            return PROFILES[rule.profile]
    bdo_digital_profile = PROFILES.get("AUTO_BUSINESS_BANKING_GROWIDE")
    if bdo_digital_profile:
        bdo_digital_tokens = [
            "posting date",
            "description",
            "debit",
            "credit",
            "running balance",
            "check number",
        ]
        if all(token in lower for token in bdo_digital_tokens):
            return bdo_digital_profile
    return PROFILES["GENERIC"]
