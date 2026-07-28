#!/usr/bin/env python3
"""Build a clean cross-platform ZIP for local one-click startup."""

from __future__ import annotations

import stat
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
VERSION = (ROOT / "VERSION").read_text(encoding="utf-8").strip()
DIST_DIR = ROOT / "dist"
ARCHIVE = DIST_DIR / f"tabel_local_v{VERSION}.zip"
ARCHIVE_ROOT = "tabel-local"

FILES = [
    "VERSION",
    "requirements.txt",
    "run.sh",
    "start.command",
    "start_windows.bat",
    "windows_setup.ps1",
    "ЛОКАЛЬНЫЙ_ЗАПУСК.txt",
    "README.md",
    "app/__init__.py",
    "app/excel_parsers.py",
    "app/logging_utils.py",
    "app/main.py",
    "app/scheduler.py",
    "app/validation.py",
    "app/work_rules.py",
    "static/index.html",
    "static/app.js",
    "static/styles.css",
    "templates/t13_template.xlsx",
    "templates/t13_form.pdf",
]

EXECUTABLES = {"run.sh", "start.command"}


def add_file(archive: zipfile.ZipFile, relative_path: str) -> None:
    source = ROOT / relative_path
    if not source.is_file():
        raise FileNotFoundError(f"Required bundle file is missing: {source}")

    info = zipfile.ZipInfo.from_file(source, f"{ARCHIVE_ROOT}/{relative_path}")
    info.compress_type = zipfile.ZIP_DEFLATED
    if relative_path in EXECUTABLES:
        info.external_attr = (stat.S_IFREG | 0o755) << 16
    payload = source.read_bytes()
    if relative_path.endswith(".bat"):
        payload = payload.replace(b"\r\n", b"\n").replace(b"\n", b"\r\n")
    with archive.open(info, "w") as output_file:
        output_file.write(payload)


def main() -> None:
    DIST_DIR.mkdir(exist_ok=True)
    with zipfile.ZipFile(ARCHIVE, "w") as archive:
        for relative_path in FILES:
            add_file(archive, relative_path)
        # Keep an obvious all-caps entry at the archive root for Windows users.
        source = ROOT / "start_windows.bat"
        info = zipfile.ZipInfo.from_file(source, f"{ARCHIVE_ROOT}/START_WINDOWS.bat")
        info.compress_type = zipfile.ZIP_DEFLATED
        payload = source.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n")
        with archive.open(info, "w") as output_file:
            output_file.write(payload)
        for directory in ("outputs", "logs", "logs/errors"):
            archive.writestr(f"{ARCHIVE_ROOT}/{directory}/.gitkeep", "")

    print(ARCHIVE)


if __name__ == "__main__":
    main()
