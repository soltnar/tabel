#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -x "./run.sh" ]; then
  chmod +x "./run.sh"
fi

/usr/bin/env bash "./run.sh"
status=$?

if [ "$status" -ne 0 ]; then
  echo
  echo "Запуск завершился с ошибкой (код $status)."
  read -r -p "Нажмите Enter для закрытия окна..."
fi

exit "$status"
