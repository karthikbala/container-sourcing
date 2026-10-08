"""ISO 6346 container numbers: normalise and validate the check digit before any fetch."""

from __future__ import annotations

import re

_LETTER_VALUES = {}
_v = 10
for _ch in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
    if _v % 11 == 0:  # values that are multiples of 11 are skipped
        _v += 1
    _LETTER_VALUES[_ch] = _v
    _v += 1

_PATTERN = re.compile(r"^[A-Z]{3}[UJZR]\d{7}$")


def normalize(number: str) -> str:
    """Uppercase and drop spaces, dashes and dots (`ymlu 517636-2` -> `YMLU5176362`)."""
    return re.sub(r"[^A-Za-z0-9]", "", number or "").upper()


def check_digit(first10: str) -> int:
    total = 0
    for i, ch in enumerate(first10):
        value = _LETTER_VALUES[ch] if ch.isalpha() else int(ch)
        total += value * (2**i)
    return total % 11 % 10


def is_valid(number: str) -> bool:
    n = normalize(number)
    if not _PATTERN.match(n):
        return False
    return check_digit(n[:10]) == int(n[10])


def explain(number: str) -> str | None:
    """None when valid, otherwise a short reason for the UI."""
    n = normalize(number)
    if len(n) != 11:
        return f"expected 11 characters, got {len(n)}"
    if not _PATTERN.match(n):
        return "expected 4 letters (owner code + U/J/Z/R) then 7 digits"
    expected = check_digit(n[:10])
    if expected != int(n[10]):
        return f"check digit should be {expected}"
    return None
