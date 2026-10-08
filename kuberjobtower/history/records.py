"""The records that cross the journal: one JSON object per line, ``{"v": 1, "t": type, "d": {...}}``.

Readers ignore unknown fields and unknown types, so a newer writer never breaks an older reader.
Times are epoch milliseconds (UTC). Nothing secret goes in: environment variable *names* at most.
"""

import collections.abc
import datetime
import hashlib
import json
import secrets
import string
import time
import typing

VERSION = 1
TYPES = ("run", "config", "job", "job_tiles", "pod", "sample", "event", "log_ref")
BASE36 = string.digits + string.ascii_lowercase

Record = dict[str, typing.Any]


def now_ms() -> int:
    return int(time.time() * 1000)


def ms(moment: datetime.datetime | str | None) -> int | None:
    if moment is None:
        return None
    if isinstance(moment, str):
        moment = datetime.datetime.fromisoformat(moment.replace("Z", "+00:00"))
    return int(moment.timestamp() * 1000)


def new_run_uid(moment: datetime.datetime | None = None) -> str:
    """``<yyyymmddHHMM>-<4 base36>``: sortable, and two people in one minute do not collide."""
    moment = moment or datetime.datetime.now(datetime.UTC)
    return f"{moment:%Y%m%d%H%M}-{''.join(secrets.choice(BASE36) for _ in range(4))}"


def make(type_: str, **data: typing.Any) -> Record:
    assert type_ in TYPES, type_
    return {"v": VERSION, "t": type_, "d": {k: v for k, v in data.items() if v is not None}}


def encode(records: collections.abc.Iterable[Record]) -> bytes:
    return ("\n".join(json.dumps(r, separators=(",", ":"), sort_keys=True) for r in records) + "\n").encode()


def decode(text: str) -> list[Record]:
    """The records of one journal object; a damaged line or a record of a newer format is skipped."""
    out = []
    for line in text.splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict) and record.get("v") == VERSION and record.get("t") in TYPES:
            out.append(record)
    return out


def config_sha(config: collections.abc.Mapping[str, typing.Any]) -> str:
    """Content address of a Job's configuration, so equal configurations are stored once."""
    return hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()[:16]


def job_id(run_uid: str, job_name: str) -> str:
    return f"{run_uid}/{job_name}"


def pod_id(run_uid: str, job_name: str, pod_name: str) -> str:
    return f"{run_uid}/{job_name}/{pod_name}"
