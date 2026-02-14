"""Deterministic semantic normalization for parsed schedule entries."""

from __future__ import annotations

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
COMPANY_NOISE_TOKENS = {"ab", "hb", "stadservice", "stadtjanst", "stadning"}
JOB_TYPE_HINT_TOKENS = {
    "stadservice",
    "stadning",
    "storstadning",
    "hemstadning",
    "kontor",
    "skola",
    "vard",
    "barn",
    "clickandgo",
}


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
    customer_name = _normalize_customer_name(customer_title or normalized.title)
    address = _decompose_address(normalized.address, normalized.location)
    shift_type = _classify_shift(normalized, address, job_type_hint=job_type_hint)
    location_key = location_fingerprint(
        street=address.street,
        street_number=address.street_number,
        postal_area=address.postal_area,
        city=address.city,
    )
    customer_key = customer_fingerprint(customer_name)

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


def _classify_shift(entry: Entry, address: AddressParts, *, job_type_hint: str) -> str:
    combined = " ".join(
        [
            _normalize_text(entry.title).lower(),
            _normalize_text(entry.address).lower(),
            _normalize_text(entry.location).lower(),
            _normalize_text(job_type_hint).lower(),
        ]
    )
    if "skola" in combined:
        return "SCHOOL"
    if "kontor" in combined:
        return "OFFICE"
    if any(token in combined for token in ("stadservice", "stadning", "storstadning", "hemstadning", "vard av barn")):
        return "HOME_VISIT"
    if "hem" in combined or (address.street and address.street_number):
        return "HOME_VISIT"
    return "UNKNOWN"


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
