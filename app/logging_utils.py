from __future__ import annotations

import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
import traceback
import uuid

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"
ERROR_LOG_DIR = LOG_DIR / "errors"
APP_LOG_FILE = LOG_DIR / "app.log"


def ensure_log_dirs() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    ERROR_LOG_DIR.mkdir(parents=True, exist_ok=True)


def setup_logging(app_version: str) -> None:
    ensure_log_dirs()

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    has_file_handler = any(
        isinstance(h, RotatingFileHandler) and Path(getattr(h, "baseFilename", "")) == APP_LOG_FILE
        for h in root_logger.handlers
    )
    if not has_file_handler:
        file_handler = RotatingFileHandler(
            filename=APP_LOG_FILE,
            maxBytes=5 * 1024 * 1024,
            backupCount=10,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    has_stream_handler = any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler)
        for h in root_logger.handlers
    )
    if not has_stream_handler:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        root_logger.addHandler(stream_handler)

    logging.getLogger("watchfiles.main").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").info("Логирование инициализировано. Версия: %s", app_version)


def write_exception_log(context: str, exc: Exception) -> tuple[str, Path]:
    ensure_log_dirs()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    error_id = f"ERR_{stamp}_{uuid.uuid4().hex[:8]}"
    error_file = ERROR_LOG_DIR / f"{error_id}.log"

    content = (
        f"error_id: {error_id}\n"
        f"time: {datetime.now().isoformat()}\n"
        f"context: {context}\n"
        f"exception_type: {type(exc).__name__}\n"
        f"exception_message: {exc}\n\n"
        "traceback:\n"
        f"{traceback.format_exc()}"
    )

    error_file.write_text(content, encoding="utf-8")
    return error_id, error_file
