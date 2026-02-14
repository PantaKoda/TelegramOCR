"""Deterministic semantic normalization for parsed schedule entries."""

from __future__ import annotations

from difflib import SequenceMatcher
import re
import unicodedata
from dataclasses import dataclass
from typing import Any

from parser.entity_identity import customer_fingerprint, location_fingerprint
from parser.layout_parser import Entry

POSTAL_CODE_RE = re.compile(r"\b(\d{3})\s?(\d{2})\b")
TIME_VALUE_RE = re.compile(r"^\s*(\d{1,2})[:.](\d{2})\s*$")
TITLE_BULLET_RE = re.compile(r"\s*[•·]\s*")
TRAILING_DURATION_RE = re.compile(
    r"(?:\b\d+\s*h(?:\s*\d+\s*m)?\b|\b\d+\s*m(?:in)?\b)\s*$",
    re.IGNORECASE,
)
TRAILING_COUNTER_RE = re.compile(r"(?:\s+\d+)+\s*$")
TRAILING_PUNCT_RE = re.compile(r"^[\s\-–—:;,.!?()\[\]{}]+|[\s\-–—:;,.!?()\[\]{}]+$")
RAW_LABEL_WORD_RE = re.compile(r"[a-z]{2,}")
COMPANY_NOISE_TOKENS = {"ab", "hb", "stadservice", "stadtjanst", "stadning"}
JOB_TYPE_HINT_TOKENS = {
    "lunch",
    "restid",
    "personalmote",
    "utbildning",
    "stadservice",
    "stadservice",
    "stadning",
    "storstadning",
    "inledande",
    "reklamation",
    "omstadning",
    "extra",
    "fonsterputs",
    "kylskapsrengoring",
    "ugnsrengoring",
    "hemstadning",
    "kontor",
    "skola",
    "nyckelhantering",
    "forberedelser",
    "disponibel",
    "avbokade",
    "avokade",
    "vard",
    "barn",
    "clickandgo",
}

SHIFT_TYPE_WORK = "WORK"
SHIFT_TYPE_TRAVEL = "TRAVEL"
SHIFT_TYPE_TRAINING = "TRAINING"
SHIFT_TYPE_BREAK = "BREAK"
SHIFT_TYPE_MEETING = "MEETING"
SHIFT_TYPE_ADMIN = "ADMIN"
SHIFT_TYPE_LEAVE = "LEAVE"
SHIFT_TYPE_UNAVAILABLE = "UNAVAILABLE"
SHIFT_TYPE_UNKNOWN = "UNKNOWN"

SHIFT_TYPE_PRIORITY = {
    SHIFT_TYPE_UNKNOWN: 0,
    SHIFT_TYPE_BREAK: 1,
    SHIFT_TYPE_TRAVEL: 2,
    SHIFT_TYPE_MEETING: 3,
    SHIFT_TYPE_ADMIN: 4,
    SHIFT_TYPE_LEAVE: 5,
    SHIFT_TYPE_TRAINING: 6,
    SHIFT_TYPE_UNAVAILABLE: 7,
    SHIFT_TYPE_WORK: 8,
}

ACTIVITY_LABEL_OVERRIDES = {
    "thank you for today": "Thank You For Today",
    "thank you for today!": "Thank You For Today",
    "inter tid": "Inter Tid",
    "personalmote": "Personalmote",
    "vard av barn": "Vard Av Barn",
    "nyckelhantering": "Nyckelhantering",
    "forberedelser till iss": "Forberedelser Till Iss",
    "ej disponibel": "Ej Disponibel",
    "avbokade uppdrag": "Avbokade Uppdrag",
    "avokade uppdrag": "Avbokade Uppdrag",
}

KNOWN_TYPE_LABEL_PATTERNS: tuple[tuple[str, str], ...] = (
    ("utbildning handledarhus", "Utbildning Handledarhus"),
    ("forberedelser till iss", "Forberedelser Till Iss"),
    ("avbokade uppdrag", "Avbokade Uppdrag"),
    ("avokade uppdrag", "Avbokade Uppdrag"),
    ("ej disponibel", "Ej Disponibel"),
    ("extra stadtillfalle", "Extra Stadtillfalle"),
    ("inledande storstadning", "Inledande Storstadning"),
    ("reklamation omstadning", "Reklamation Omstadning"),
    ("kylskapsrengoring", "Kylskapsrengoring"),
    ("ugnsrengoring", "Ugnsrengoring"),
    ("nyckelhantering", "Nyckelhantering"),
    ("personalmote", "Personalmote"),
    ("thank you for today", "Thank You For Today"),
    ("inter tid", "Inter Tid"),
    ("vard av barn", "Vard Av Barn"),
    ("clickandgo", "ClickAndGo"),
    ("storstadning", "Storstadning"),
    ("stadservice", "Stadservice"),
    ("reklamation", "Reklamation"),
    ("fonsterputs", "Fonsterputs"),
    ("utbildning", "Utbildning"),
    ("restid", "Restid"),
    ("lunch", "Lunch"),
)

NON_WORK_ACTIVITY_TYPES = {
    SHIFT_TYPE_BREAK,
    SHIFT_TYPE_TRAVEL,
    SHIFT_TYPE_MEETING,
    SHIFT_TYPE_ADMIN,
    SHIFT_TYPE_LEAVE,
    SHIFT_TYPE_UNAVAILABLE,
    SHIFT_TYPE_TRAINING,
}
KNOWN_LABEL_FUZZY_MIN_LEN = 5
KNOWN_LABEL_FUZZY_THRESHOLD = 0.82


@dataclass(frozen=True)
class CanonicalShift:
    start: str
    end: str
    customer_name: str
    customer_fingerprint: str
    street: str
    street_number: str
    postal_code: str
    postal_area: str
    city: str
    location_fingerprint: str
    shift_type: str
    raw_type_label: str = ""


@dataclass(frozen=True)
class AddressParts:
    street: str
    street_number: str
    postal_code: str
    postal_area: str
    city: str


def normalize_entry(entry: Entry | dict[str, Any]) -> CanonicalShift:
    normalized = _coerce_entry(entry)
    customer_title, job_type_hint = _split_title_components(normalized.title)
    address = _decompose_address(normalized.address, normalized.location)
    raw_type_label = _extract_raw_type_label(normalized, customer_title=customer_title, job_type_hint=job_type_hint)
    shift_type = _classify_shift(normalized, address, raw_type_label=raw_type_label)
    customer_name = _extract_customer_name(
        normalized,
        customer_title=customer_title,
        job_type_hint=job_type_hint,
        raw_type_label=raw_type_label,
        shift_type=shift_type,
        address=address,
    )
    location_key = location_fingerprint(
        street=address.street,
        street_number=address.street_number,
        postal_area=address.postal_area,
        city=address.city,
    )
    identity_anchor = customer_name or _normalize_customer_name(raw_type_label) or shift_type
    customer_key = customer_fingerprint(identity_anchor)

    return CanonicalShift(
        start=_normalize_time(normalized.start, "start"),
        end=_normalize_time(normalized.end, "end"),
        customer_name=customer_name,
        customer_fingerprint=customer_key,
        street=address.street,
        street_number=address.street_number,
        postal_code=address.postal_code,
        postal_area=address.postal_area,
        city=address.city,
        location_fingerprint=location_key,
        shift_type=shift_type,
        raw_type_label=raw_type_label,
    )


def normalize_entries(entries: list[Entry | dict[str, Any]]) -> list[CanonicalShift]:
    return [normalize_entry(entry) for entry in entries]


def _coerce_entry(value: Entry | dict[str, Any]) -> Entry:
    if isinstance(value, Entry):
        return value
    if isinstance(value, dict):
        return Entry(
            start=str(value.get("start", "")),
            end=str(value.get("end", "")),
            title=str(value.get("title", "")),
            location=str(value.get("location", "")),
            address=str(value.get("address", "")),
        )
    raise TypeError(f"Unsupported entry value: {type(value)!r}")


def _normalize_time(value: str, field_name: str) -> str:
    match = TIME_VALUE_RE.fullmatch(value)
    if match is None:
        raise ValueError(f"Invalid {field_name} value: {value}")
    hour = int(match.group(1))
    minute = int(match.group(2))
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"Invalid {field_name} value: {value}")
    return f"{hour:02d}:{minute:02d}"


def _decompose_address(address_text: str, location_hint: str) -> AddressParts:
    normalized_address = _normalize_text(address_text)
    normalized_location = _normalize_place(location_hint)

    postal_code = ""
    postal_area = ""
    city = normalized_location

    street_source = normalized_address
    postal_match = POSTAL_CODE_RE.search(normalized_address)
    if postal_match is not None:
        postal_code = f"{postal_match.group(1)} {postal_match.group(2)}"
        before = _collapse_whitespace(normalized_address[: postal_match.start()])
        after = _collapse_whitespace(normalized_address[postal_match.end() :])
        street_source = before
        postal_area = _normalize_place(after)
        if postal_area:
            city = postal_area

    tokens = [token for token in street_source.split(" ") if token]
    street_number = ""
    street = ""
    trailing_tokens: list[str] = []

    number_index = _last_number_index(tokens)
    if number_index is not None:
        street = _normalize_street(" ".join(tokens[:number_index]))
        street_number = _normalize_street_number(tokens[number_index])
        trailing_tokens = tokens[number_index + 1 :]
    else:
        street = _normalize_street(street_source)

    if not city and trailing_tokens:
        city = _extract_city_from_tokens(trailing_tokens)

    if not city and not postal_area and postal_code:
        city = postal_area

    if postal_code and not postal_area and city:
        postal_area = city

    if not city and normalized_location:
        city = normalized_location

    return AddressParts(
        street=street,
        street_number=street_number,
        postal_code=postal_code,
        postal_area=postal_area,
        city=city,
    )


def _normalize_customer_name(value: str) -> str:
    normalized = _normalize_text(_strip_trailing_duration(value))
    tokens = [token for token in normalized.lower().split(" ") if token and token not in COMPANY_NOISE_TOKENS]
    if not tokens:
        tokens = [token for token in normalized.lower().split(" ") if token]
    return _to_title_case(" ".join(tokens))


def _extract_customer_name(
    entry: Entry,
    *,
    customer_title: str,
    job_type_hint: str,
    raw_type_label: str,
    shift_type: str,
    address: AddressParts,
) -> str:
    candidate = _normalize_customer_name(customer_title or entry.title)
    if not raw_type_label:
        return candidate
    combined_hint_label = _normalize_type_label(" ".join(token for token in (customer_title, job_type_hint) if token))
    combined_hint_type = _classify_from_normalized_label(_normalize_text(combined_hint_label).lower()) if combined_hint_label else SHIFT_TYPE_UNKNOWN
    has_customer_hint = bool(_normalize_text(customer_title)) and bool(_normalize_text(job_type_hint))
    has_location_context = bool(
        address.street
        or address.street_number
        or address.city
        or address.postal_code
        or _normalize_text(entry.address)
        or _normalize_text(entry.location)
    )
    if combined_hint_type in NON_WORK_ACTIVITY_TYPES and not has_location_context:
        return ""
    if shift_type in NON_WORK_ACTIVITY_TYPES and not has_location_context:
        return ""
    if has_customer_hint or has_location_context:
        return candidate
    if shift_type in NON_WORK_ACTIVITY_TYPES:
        return ""
    return candidate


def _classify_shift(entry: Entry, address: AddressParts, *, raw_type_label: str) -> str:
    normalized_raw = _normalize_text(raw_type_label).lower()
    classified_raw = _classify_from_normalized_label(normalized_raw)
    if classified_raw != SHIFT_TYPE_UNKNOWN:
        return classified_raw

    combined = " ".join(
        [
            _normalize_text(entry.title).lower(),
            _normalize_text(entry.address).lower(),
            _normalize_text(entry.location).lower(),
        ]
    )
    classified_combined = _classify_from_normalized_label(combined)
    if classified_combined != SHIFT_TYPE_UNKNOWN:
        return classified_combined
    if "hem" in combined or (address.street and address.street_number):
        return SHIFT_TYPE_WORK
    return SHIFT_TYPE_UNKNOWN


def _classify_from_normalized_label(value: str) -> str:
    if not value:
        return SHIFT_TYPE_UNKNOWN

    if any(token in value for token in ("restid", "inter tid")):
        return SHIFT_TYPE_TRAVEL
    if any(token in value for token in ("lunch", "rast", "thank you for today")):
        return SHIFT_TYPE_BREAK
    if "personalmote" in value:
        return SHIFT_TYPE_MEETING
    if any(token in value for token in ("nyckelhantering", "forberedelser till iss")):
        return SHIFT_TYPE_ADMIN
    if "vard av barn" in value:
        return SHIFT_TYPE_LEAVE
    if any(token in value for token in ("ej disponibel", "avbokade uppdrag", "avokade uppdrag")):
        return SHIFT_TYPE_UNAVAILABLE
    if "utbildning" in value:
        return SHIFT_TYPE_TRAINING
    if any(
        token in value
        for token in (
            "stadservice",
            "stadning",
            "storstadning",
            "inledande storstadning",
            "reklamation",
            "omstadning",
            "extra stadtillfalle",
            "fonsterputs",
            "kylskapsrengoring",
            "ugnsrengoring",
            "clickandgo",
            "skola",
            "kontor",
            "hemstadning",
        )
    ):
        return SHIFT_TYPE_WORK
    return SHIFT_TYPE_UNKNOWN


def _split_title_components(value: str) -> tuple[str, str]:
    collapsed = _collapse_whitespace(value)
    if not collapsed:
        return "", ""

    if TITLE_BULLET_RE.search(collapsed):
        left, right = TITLE_BULLET_RE.split(collapsed, maxsplit=1)
        customer = _collapse_whitespace(left)
        job_type = _collapse_whitespace(_strip_trailing_duration(right))
        return customer, job_type

    without_duration = _strip_trailing_duration(collapsed)
    tokens = without_duration.split(" ")
    for index, token in enumerate(tokens):
        if index == 0:
            continue
        normalized = _normalize_text(token).lower()
        if normalized in JOB_TYPE_HINT_TOKENS:
            return _collapse_whitespace(" ".join(tokens[:index])), _collapse_whitespace(" ".join(tokens[index:]))
    return without_duration, ""


def _extract_raw_type_label(entry: Entry, *, customer_title: str, job_type_hint: str) -> str:
    combined_hint = _normalize_type_label(" ".join(token for token in (customer_title, job_type_hint) if token))
    if combined_hint:
        combined_normalized = _normalize_text(combined_hint).lower()
        combined_classification = _classify_from_normalized_label(combined_normalized)
        if combined_normalized in ACTIVITY_LABEL_OVERRIDES:
            return ACTIVITY_LABEL_OVERRIDES[combined_normalized]
        if combined_classification in NON_WORK_ACTIVITY_TYPES:
            return _canonicalize_type_label(combined_hint)

    if job_type_hint:
        hint_candidate = _canonicalize_type_label(job_type_hint)
        if _is_usable_raw_type_label(hint_candidate):
            return hint_candidate

    title_candidate = _canonicalize_type_label(entry.title)
    if not title_candidate:
        address_candidate = _extract_label_from_context_text(entry.address)
        if address_candidate:
            return address_candidate
        return _extract_label_from_context_text(entry.location)

    normalized = _normalize_text(title_candidate).lower()
    if normalized in ACTIVITY_LABEL_OVERRIDES:
        return ACTIVITY_LABEL_OVERRIDES[normalized]
    if _classify_from_normalized_label(normalized) != SHIFT_TYPE_UNKNOWN:
        return title_candidate

    address_candidate = _extract_label_from_context_text(entry.address)
    if address_candidate:
        return address_candidate
    location_candidate = _extract_label_from_context_text(entry.location)
    if location_candidate:
        return location_candidate
    return ""


def _normalize_type_label(value: str) -> str:
    cleaned = _collapse_whitespace(value.replace("•", " ").replace("·", " "))
    cleaned = _strip_trailing_duration(cleaned)
    cleaned = TRAILING_COUNTER_RE.sub("", cleaned).strip()
    cleaned = TRAILING_PUNCT_RE.sub("", cleaned)
    cleaned = _collapse_whitespace(cleaned)
    if not cleaned:
        return ""
    normalized = _normalize_text(cleaned)
    return _to_title_case(normalized)


def _canonicalize_type_label(value: str) -> str:
    cleaned = _normalize_type_label(value)
    if not cleaned:
        return ""
    canonical = _canonical_known_label(_normalize_text(cleaned).lower())
    return canonical or cleaned


def _extract_label_from_context_text(value: str) -> str:
    normalized = _normalize_text(value).lower()
    if not normalized:
        return ""
    return _canonical_known_label(normalized)


def _canonical_known_label(normalized: str) -> str:
    if not normalized:
        return ""
    if normalized in ACTIVITY_LABEL_OVERRIDES:
        return ACTIVITY_LABEL_OVERRIDES[normalized]
    for pattern, canonical in KNOWN_TYPE_LABEL_PATTERNS:
        if re.search(rf"\b{re.escape(pattern)}\b", normalized):
            return canonical
    fuzzy = _fuzzy_canonical_known_label(normalized)
    if fuzzy:
        return fuzzy
    return ""


def _is_usable_raw_type_label(value: str) -> bool:
    normalized = _normalize_text(value).lower()
    if not normalized:
        return False
    if normalized in ACTIVITY_LABEL_OVERRIDES:
        return True
    if _canonical_known_label(normalized):
        return True
    return RAW_LABEL_WORD_RE.search(normalized) is not None


def _fuzzy_canonical_known_label(normalized: str) -> str:
    tokens = [token for token in normalized.split() if token]
    if not tokens:
        return ""

    candidates: set[str] = set()
    for size in (1, 2, 3):
        if len(tokens) < size:
            continue
        for index in range(len(tokens) - size + 1):
            phrase = " ".join(tokens[index : index + size])
            if len(phrase.replace(" ", "")) >= KNOWN_LABEL_FUZZY_MIN_LEN:
                candidates.add(phrase)

    best_score = 0.0
    best_label = ""
    for candidate in candidates:
        for pattern, canonical in KNOWN_TYPE_LABEL_PATTERNS:
            if abs(len(candidate) - len(pattern)) > 6:
                continue
            score = SequenceMatcher(None, candidate, pattern).ratio()
            if score > best_score and score >= KNOWN_LABEL_FUZZY_THRESHOLD:
                best_score = score
                best_label = canonical
    return best_label


def _strip_trailing_duration(value: str) -> str:
    previous = None
    current = _collapse_whitespace(value)
    while previous != current:
        previous = current
        current = TRAILING_DURATION_RE.sub("", current).strip()
    return _collapse_whitespace(current)


def _normalize_street(value: str) -> str:
    return _to_title_case(_normalize_text(value))


def _normalize_place(value: str) -> str:
    return _to_title_case(_normalize_text(value))


def _normalize_street_number(value: str) -> str:
    normalized = _normalize_text(value).replace(" ", "")
    return normalized.upper()


def _extract_city_from_tokens(tokens: list[str]) -> str:
    city_tokens: list[str] = []
    for token in reversed(tokens):
        if any(char.isdigit() for char in token):
            break
        normalized = _normalize_place(token)
        if not normalized:
            break
        if len(normalized) <= 2 and city_tokens:
            break
        city_tokens.append(normalized)
        if len(city_tokens) == 2:
            break
    if not city_tokens:
        return ""
    city_tokens.reverse()
    return " ".join(city_tokens)


def _last_number_index(tokens: list[str]) -> int | None:
    for index in range(len(tokens) - 1, -1, -1):
        if any(char.isdigit() for char in tokens[index]):
            return index
    return None


def _normalize_text(value: str) -> str:
    collapsed = _collapse_whitespace(value)
    if not collapsed:
        return ""

    fixed = collapsed.replace("|", "l").replace("I", "i")
    fixed = _replace_ocr_digit_confusions(fixed)
    stripped = _strip_accents(fixed)
    alnum = re.sub(r"[^A-Za-z0-9\s\-']", " ", stripped)
    return _collapse_whitespace(alnum)


def _replace_ocr_digit_confusions(value: str) -> str:
    chars = list(value)
    for index, char in enumerate(chars):
        prev_is_alpha = index > 0 and chars[index - 1].isalpha()
        next_is_alpha = index + 1 < len(chars) and chars[index + 1].isalpha()
        if char == "0" and prev_is_alpha and next_is_alpha:
            chars[index] = "o"
        elif char == "1" and prev_is_alpha and next_is_alpha:
            chars[index] = "i"
    return "".join(chars)


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def _to_title_case(value: str) -> str:
    if not value:
        return ""
    return " ".join(_title_token(token) for token in value.split(" "))


def _title_token(token: str) -> str:
    if not token:
        return token
    return token[0].upper() + token[1:].lower()


def _collapse_whitespace(value: str) -> str:
    return " ".join(value.split())
