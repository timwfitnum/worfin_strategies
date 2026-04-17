"""
config/logging_config.py
Structured logging configuration for WorFIn.

MODES:
  DEVELOPMENT — human-readable, coloured if possible (StreamHandler)
  PAPER / LIVE — JSON Lines to file + stderr, daily rotation, 30-day retention

STRUCTURE (all JSON lines in PAPER/LIVE):
  {
    "ts":            "2026-04-16T14:23:01.123456+00:00",  // ISO-8601 UTC
    "level":         "INFO",
    "logger":        "worfin.data.ingestion.fx_rates",
    "message":       "FX rate for 2026-04-16: 1.271234 (from FRED).",
    "correlation_id": "4f9a2b3c-...",                     // UUID per process
    "pid":           12345
  }

CORRELATION ID:
  A single UUID is generated when configure_logging() is called and injected
  into every log line via LogRecord injection. One UUID per process start →
  each backtest run, each live session, each paper session is traceable.

USAGE:
  # In main entry-point (once):
  from worfin.config.logging_config import configure_logging
  configure_logging()

  # Everywhere else:
  import logging
  logger = logging.getLogger(__name__)
  logger.info("Strategy %s signal: %.4f", strategy_id, signal)

NEVER call configure_logging() more than once — use the guard at the bottom.
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

LOG_DIR = Path("logs")
LOG_FILENAME_PATTERN = "worfin_{date}.log"   # date injected at rotation time
LOG_ROTATION_WHEN = "midnight"               # rotate at midnight UTC
LOG_RETENTION_COUNT = 30                     # 30 days of log files

# Per-module level overrides — applied after the root level is set.
# Keeps noisy third-party libraries quiet without losing WorFIn detail.
MODULE_LEVELS: dict[str, int] = {
    "worfin":                            logging.DEBUG,   # all worfin modules: verbose
    "worfin.backtest.engine":            logging.DEBUG,
    "worfin.data.ingestion":             logging.DEBUG,
    "worfin.data.pipeline":              logging.INFO,
    "worfin.risk":                       logging.INFO,
    "worfin.execution":                  logging.WARNING, # quiet in backtest; noisy in live
    "worfin.monitoring":                 logging.INFO,
    # Third-party
    "sqlalchemy.engine":                 logging.WARNING,
    "sqlalchemy.pool":                   logging.WARNING,
    "urllib3":                           logging.WARNING,
    "requests":                          logging.WARNING,
    "ib_insync":                         logging.WARNING,
}

# ─────────────────────────────────────────────────────────────────────────────
# PROCESS-LEVEL CORRELATION ID
# ─────────────────────────────────────────────────────────────────────────────
# Generated once when this module is first imported. Attached to every log
# line so distributed log aggregators (Loki, Datadog, etc.) can group all
# log lines from one backtest run / live session.

_CORRELATION_ID: str = str(uuid.uuid4())


def get_correlation_id() -> str:
    """Return the correlation ID for the current process."""
    return _CORRELATION_ID


# ─────────────────────────────────────────────────────────────────────────────
# FORMATTERS
# ─────────────────────────────────────────────────────────────────────────────


class _CorrelationFilter(logging.Filter):
    """
    Injects `correlation_id` and `pid` into every LogRecord so that both the
    JSON and human-readable formatters can reference them without extra code.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.correlation_id = _CORRELATION_ID  # type: ignore[attr-defined]
        record.pid = os.getpid()                  # type: ignore[attr-defined]
        return True


class _JsonFormatter(logging.Formatter):
    """
    Emit one JSON object per line — compatible with log aggregators.

    Fields:
      ts, level, logger, message, correlation_id, pid
      + any `extra={}` kwargs passed to the logger call
      + exc_info serialised as "exception" string if present
    """

    # Keys that are standard LogRecord attributes (not "extra")
    _RESERVED = frozenset(
        {
            "args",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "message",
            "module",
            "msecs",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "taskName",
            "thread",
            "threadName",
        }
    )

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        ts = datetime.fromtimestamp(record.created, tz=UTC).isoformat()

        payload: dict[str, object] = {
            "ts": ts,
            "level": record.levelname,
            "logger": record.name,
            "message": record.message,
            "correlation_id": getattr(record, "correlation_id", ""),
            "pid": getattr(record, "pid", os.getpid()),
        }

        # Splice in any extra={"key": val} the caller provided
        for key, val in record.__dict__.items():
            if key not in self._RESERVED and not key.startswith("_"):
                if key not in payload:
                    payload[key] = val

        # Serialise exception if present
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str)


class _HumanFormatter(logging.Formatter):
    """
    Readable format for DEVELOPMENT:
      2026-04-16 14:23:01.123 [INFO    ] worfin.data.fx_rates — FX rate: 1.27
    """

    _FMT = "%(asctime)s [%(levelname)-8s] %(name)s — %(message)s"
    _DATEFMT = "%Y-%m-%d %H:%M:%S"

    def __init__(self) -> None:
        super().__init__(fmt=self._FMT, datefmt=self._DATEFMT)

    def format(self, record: logging.LogRecord) -> str:
        result = super().format(record)
        # Append correlation_id only in DEBUG to keep INFO output clean
        if record.levelno <= logging.DEBUG:
            cid = getattr(record, "correlation_id", "")
            if cid:
                result += f"  [{cid[:8]}]"
        return result


# ─────────────────────────────────────────────────────────────────────────────
# LOG FILE HANDLER — daily rotation with date in filename
# ─────────────────────────────────────────────────────────────────────────────


class _DailyFileHandler(logging.handlers.TimedRotatingFileHandler):
    """
    Rotates at midnight, keeps LOG_RETENTION_COUNT backups, and names files
    `worfin_YYYY-MM-DD.log` rather than `worfin.log.YYYY-MM-DD`.
    """

    def __init__(self, log_dir: Path) -> None:
        log_dir.mkdir(parents=True, exist_ok=True)
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        filepath = log_dir / LOG_FILENAME_PATTERN.format(date=today)
        super().__init__(
            filename=str(filepath),
            when=LOG_ROTATION_WHEN,
            interval=1,
            backupCount=LOG_RETENTION_COUNT,
            encoding="utf-8",
            utc=True,
        )

    def doRollover(self) -> None:
        """Override to rename the current file before rotating."""
        super().doRollover()
        # After rotation the base filename still has today's date baked in;
        # we recreate a new file with tomorrow's date on first write — Python's
        # TimedRotatingFileHandler does this automatically when the suffix is set.
        today = datetime.now(UTC).strftime("%Y-%m-%d")
        new_path = LOG_DIR / LOG_FILENAME_PATTERN.format(date=today)
        self.baseFilename = str(new_path)
        if self.stream:
            self.stream.close()
        self.stream = self._open()


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC API
# ─────────────────────────────────────────────────────────────────────────────

_configured = False  # Guard against double-configuration


def configure_logging(
    log_level: str | None = None,
    log_dir: Path | None = None,
    force: bool = False,
) -> None:
    """
    Configure the WorFIn logging stack. Call exactly once at process startup.

    Behaviour depends on the ENVIRONMENT env var:
      DEVELOPMENT → human-readable StreamHandler (no file)
      PAPER / LIVE → JSON StreamHandler + JSON rotating file handler

    Args:
        log_level: Override the log level (e.g. "DEBUG"). Defaults to the
                   LOG_LEVEL env var, or "INFO" if not set.
        log_dir:   Override the log directory. Defaults to LOG_DIR ("logs/").
        force:     Re-configure even if already called. Useful in tests.
    """
    global _configured  # noqa: PLW0603

    if _configured and not force:
        return
    _configured = True

    # Resolve environment and level
    env = os.environ.get("ENVIRONMENT", "development").lower()
    level_name = (log_level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    resolved_dir = log_dir or LOG_DIR

    # Shared filter — injected on root so every handler gets it
    corr_filter = _CorrelationFilter()

    # ── Root logger ──────────────────────────────────────────────────────────
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # handlers do their own level filtering
    root.handlers.clear()
    root.addFilter(corr_filter)

    if env == "development":
        # Human-readable to stderr
        handler: logging.Handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(level)
        handler.setFormatter(_HumanFormatter())
        root.addHandler(handler)
    else:
        # JSON to stderr
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(level)
        stderr_handler.setFormatter(_JsonFormatter())
        root.addHandler(stderr_handler)

        # JSON to rotating daily file
        try:
            file_handler = _DailyFileHandler(resolved_dir)
            file_handler.setLevel(level)
            file_handler.setFormatter(_JsonFormatter())
            root.addHandler(file_handler)
        except OSError as exc:
            # Don't crash the process if we can't open the log file
            logging.getLogger(__name__).warning(
                "Could not open log file in %s: %s. Logging to stderr only.",
                resolved_dir,
                exc,
            )

    # ── Per-module level overrides ────────────────────────────────────────────
    for module_name, module_level in MODULE_LEVELS.items():
        logging.getLogger(module_name).setLevel(module_level)

    # ── Confirmation ──────────────────────────────────────────────────────────
    logger = logging.getLogger(__name__)
    logger.info(
        "Logging configured. env=%s level=%s correlation_id=%s",
        env,
        level_name,
        _CORRELATION_ID,
    )