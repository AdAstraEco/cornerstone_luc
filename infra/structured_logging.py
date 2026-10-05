"""One JSON object per log line on stdout, in the shape Cloud Logging parses into ``jsonPayload``.

GKE ships a container's stdout to Cloud Logging, and a line that is a JSON object becomes
structured: ``severity`` sets the entry's severity (plain text on stderr is all ``ERROR``),
``message`` its text, and every other key a queryable field. ``configure`` stamps the run's
fixed identifiers (``run_id``, ``phase``, ``tile``, ``pod``) on every line so a query by run
or tile needs no regex on text. ``text`` stays the default for local runs; note that both modes write to stdout, where the old
text mode wrote to stderr (so ``run_phase.py ... 2> errors.log`` no longer captures log lines).

A record may carry ``fields`` (``logger.info(..., extra={"fields": {...}})``): in JSON mode
they are merged into the top level and ``kind`` becomes the message; in text mode the logged
message is shown as is.
"""

import datetime
import json
import logging
import sys
import typing


class JsonFormatter(logging.Formatter):
    def __init__(self, context: dict[str, typing.Any]) -> None:
        super().__init__()
        self._context = context

    def format(self, record: logging.LogRecord) -> str:
        fields: dict[str, typing.Any] = getattr(record, "fields", {})
        entry: dict[str, typing.Any] = {
            "severity": record.levelname,
            "time": datetime.datetime.fromtimestamp(
                record.created, tz=datetime.UTC
            ).isoformat(),
            # a structured record's ``kind`` is its name; its text would only repeat the fields
            "message": fields.get("kind") or record.getMessage(),
            "logger": record.name,
            **self._context,
            **fields,
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=str)


def configure(log_format: str, context: dict[str, typing.Any]) -> None:
    """Route the root logger to stdout, as ``json`` (with ``context``) or the legacy ``text``."""
    if log_format == "json":
        formatter: logging.Formatter = JsonFormatter(
            {k: v for k, v in context.items() if v is not None}
        )
    else:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    handler = logging.StreamHandler(sys.stdout)  # flushes after every record
    handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
