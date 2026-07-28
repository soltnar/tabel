"""Start the bundled Windows application without requiring system Python."""

from __future__ import annotations

import os
import socket
import sys
import threading
import time
import webbrowser

import uvicorn

from app.main import app as application


HOST = "127.0.0.1"
PORT = 8000
URL = f"http://{HOST}:{PORT}"


def _port_is_available() -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        try:
            sock.bind((HOST, PORT))
        except OSError:
            return False
    return True


def _open_browser_when_ready() -> None:
    if os.environ.get("TABEL_NO_BROWSER") == "1":
        return
    for _ in range(60):
        try:
            with socket.create_connection((HOST, PORT), timeout=0.5):
                webbrowser.open(URL)
                return
        except OSError:
            time.sleep(0.5)


def main() -> None:
    if not _port_is_available():
        print(f"Порт {PORT} уже занят. Открываю существующее приложение: {URL}")
        if os.environ.get("TABEL_NO_BROWSER") != "1":
            webbrowser.open(URL)
        return

    print("Генератор графика сотрудников запускается...")
    print(f"После запуска откроется: {URL}")
    print("Для остановки закройте это окно.")

    threading.Thread(target=_open_browser_when_ready, daemon=True).start()
    uvicorn.run(application, host=HOST, port=PORT, log_level="info")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\nНе удалось запустить приложение: {exc}")
        input("Нажмите Enter для закрытия окна...")
        sys.exit(1)
