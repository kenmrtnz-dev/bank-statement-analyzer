import json
import os
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


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
    ocr_backends: List[str]
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
    data_dir = Path(os.getenv("DATA_DIR", "./data"))
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

    with path.open() as f:
        data = json.load(f)

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
            ocr_backends=[str(v).strip().lower() for v in raw.get("ocr_backends", []) if str(v).strip()],
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


def get_profiles_config_path() -> Path:
    return ACTIVE_CONFIG_PATH


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


def _extract_first(text: str, patterns: List[str]) -> Optional[str]:
    for pattern in patterns:
        try:
            m = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        except re.error:
            continue
        if not m:
            continue
        try:
            captured = m.group(1) if m.lastindex and m.lastindex >= 1 else m.group(0)
        except IndexError:
            captured = m.group(0)
        value = (captured or "").strip()
        if value:
            return value
    return None


def _normalize_account_name(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    value = re.sub(r"\s+", " ", raw).strip(" -:|")
    value = re.sub(
        r"^(account\s*name|account\s*holder|depositor\s*name)\s*[:\-]?\s*",
        "",
        value,
        flags=re.IGNORECASE,
    ).strip()
    stop_tokens = [
        "available balance",
        "account currency",
        "date from",
        "date to",
        "extracted date",
        "invalid datetime",
        "account number",
        "acct no",
        "statement period",
    ]
    lower = value.lower()
    for token in stop_tokens:
        idx = lower.find(token)
        if idx == 0:
            value = value[len(token):].strip(" -:|")
            lower = value.lower()
            continue
        if idx > 0:
            value = value[:idx].strip(" -:|")
            lower = value.lower()
    value = re.sub(r"\s+(php|usd|eur|sgd)$", "", value, flags=re.IGNORECASE).strip()
    if len(value) > 90:
        return None
    if len(value) < 3:
        return None
    if value.lower() in {"n/a", "na", "none", "null"}:
        return None
    return value


def _normalize_account_number(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    value = re.sub(r"\s+", "", raw).strip(" -:|")
    value = value.replace("—", "-")
    if len(value) < 4:
        return None
    if not re.search(r"\d", value):
        return None
    return value


def extract_account_identity(page_text: str, profile: BankProfile) -> Dict[str, Optional[str]]:
    text = page_text or ""
    generic = PROFILES.get("GENERIC")

    name_patterns = list(profile.account_name_patterns)
    number_patterns = list(profile.account_number_patterns)
    if generic and profile.name != "GENERIC":
        name_patterns.extend(generic.account_name_patterns)
        number_patterns.extend(generic.account_number_patterns)

    account_name = _normalize_account_name(_extract_first(text, name_patterns))
    account_number = _normalize_account_number(_extract_first(text, number_patterns))

    return {
        "account_name": account_name,
        "account_number": account_number,
    }


def find_value_bounds(
    words: List[Dict],
    page_width: float,
    page_height: float,
    value: Optional[str],
    page_name: Optional[str] = None,
) -> Optional[Dict[str, float]]:
    if not value or not words or page_width <= 0 or page_height <= 0:
        return None

    tokens = _value_tokens(value)
    if not tokens:
        return None

    ordered = [
        w for w in words
        if isinstance(w, dict) and str(w.get("text") or "").strip()
    ]
    if not ordered:
        return None
    ordered.sort(key=lambda w: (float(w.get("y1", 0.0)), float(w.get("x1", 0.0))))
    normalized_words = [_normalize_token(str(w.get("text") or "")) for w in ordered]

    best_span = None
    max_scan = max(12, len(tokens) * 4)

    for i in range(len(ordered)):
        k = i
        matched_indices: List[int] = []
        for token in tokens:
            found_idx = None
            scan_limit = min(len(ordered), k + max_scan)
            while k < scan_limit:
                if _token_matches(normalized_words[k], token):
                    found_idx = k
                    k += 1
                    break
                k += 1
            if found_idx is None:
                matched_indices = []
                break
            matched_indices.append(found_idx)

        if not matched_indices:
            continue

        span = matched_indices[-1] - matched_indices[0]
        if best_span is None or span < best_span[0]:
            best_span = (span, matched_indices)

    if not best_span:
        return None

    _, idxs = best_span
    xs1 = [float(ordered[idx].get("x1", 0.0)) for idx in idxs]
    ys1 = [float(ordered[idx].get("y1", 0.0)) for idx in idxs]
    xs2 = [float(ordered[idx].get("x2", page_width)) for idx in idxs]
    ys2 = [float(ordered[idx].get("y2", page_height)) for idx in idxs]

    result = {
        "x1": max(0.0, min(1.0, min(xs1) / page_width)),
        "y1": max(0.0, min(1.0, min(ys1) / page_height)),
        "x2": max(0.0, min(1.0, max(xs2) / page_width)),
        "y2": max(0.0, min(1.0, max(ys2) / page_height)),
    }
    if page_name:
        result["page"] = page_name
    return result


def _normalize_token(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (text or "").lower())


def _value_tokens(value: str) -> List[str]:
    tokens = []
    for raw in re.split(r"\s+", str(value or "").strip()):
        token = _normalize_token(raw)
        if token:
            tokens.append(token)
    return tokens


def _token_matches(word_token: str, value_token: str) -> bool:
    if not word_token or not value_token:
        return False
    if _is_strict_numeric_token(value_token):
        return word_token == value_token
    return (
        word_token == value_token
        or word_token in value_token
        or value_token in word_token
    )


def _is_strict_numeric_token(token: str) -> bool:
    return bool(token) and len(token) >= 6 and re.fullmatch(r"[0-9x]+", token) is not None
