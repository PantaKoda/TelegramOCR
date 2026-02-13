"""Deterministic entity fingerprinting for semantic schedule objects."""

from __future__ import annotations

import hashlib
import re
import unicodedata

COMPANY_NOISE_TOKENS = {"ab", "hb", "stadservice", "stadtjanst", "stadning"}


def location_fingerprint(
    *,
    street: str,
    street_number: str,
    postal_area: str,
    city: str,
) -> str:
    place = postal_area or city
    source = "|".join(
        [
            _normalize_component(street),
            _normalize_component(street_number),
            _normalize_component(place),
        ]
    )
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def customer_fingerprint(customer_name: str) -> str:
    normalized = _normalize_readable_text(customer_name).lower()
    raw_tokens = [token for token in normalized.split(" ") if token]
    tokens = [token for token in raw_tokens if token not in COMPANY_NOISE_TOKENS]
    if not tokens:
        tokens = raw_tokens
    if not tokens:
        return hashlib.sha256(b"").hexdigest()

    surname = max(tokens, key=len)
    initials = sorted(token[0] for token in tokens if token != surname and token)
    source = f"{surname}|{''.join(initials)}"
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def _normalize_component(value: str) -> str:
    base = _normalize_readable_text(value).lower()
    if not base:
        return ""
    base = re.sub(r"[0o]", "o", base)
    base = re.sub(r"[1il|]", "l", base)
    base = re.sub(r"[^a-z0-9]", "", base)
    return base


def _normalize_readable_text(value: str) -> str:
    collapsed = " ".join(value.split())
    if not collapsed:
        return ""

    stripped = _strip_accents(collapsed)
    alnum = re.sub(r"[^A-Za-z0-9\s\-']", " ", stripped)
    return " ".join(alnum.split())


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(char for char in normalized if not unicodedata.combining(char))

