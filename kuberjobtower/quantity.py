"""Kubernetes resource quantities ("56Gi", "6", "500m") as numbers. Standard library only."""

import re

BINARY = {"Ki": 1 << 10, "Mi": 1 << 20, "Gi": 1 << 30, "Ti": 1 << 40, "Pi": 1 << 50}
DECIMAL = {"k": 10**3, "M": 10**6, "G": 10**9, "T": 10**12, "P": 10**15}
PATTERN = re.compile(r"(\d+(?:\.\d+)?)(Ki|Mi|Gi|Ti|Pi|k|M|G|T|P|m)?")


def parse(text: str) -> float:
    """The quantity in base units: bytes for memory and storage, cores for CPU."""
    match = PATTERN.fullmatch(text.strip())
    if not match:
        raise ValueError(f"{text!r} is not a Kubernetes quantity")
    number, unit = float(match[1]), match[2]
    if unit is None:
        return number
    if unit == "m":
        return number / 1000
    return number * (BINARY.get(unit) or DECIMAL[unit])


def millicores(text: str) -> int:
    return round(parse(text) * 1000)


def bytes_(text: str) -> int:
    return round(parse(text))
