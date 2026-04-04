# Генератор графика сотрудников (FastAPI + Web UI)

Веб‑приложение для генерации графика и табеля Т‑13 на основе **расчетных листков Excel**.

## Что делает приложение

- принимает расчетные листки (`.xlsx/.xls`);
- определяет сотрудников, подразделения, роли, лимиты дней/часов и период;
- строит график смен с учетом ограничений;
- формирует:
  - `График (Excel)`,
  - `Табель Т‑13 (Excel)`,
  - `Табель Т‑13 (PDF)`,
  - preview в интерфейсе.

## Входные данные

- Основной файл: **Расчетные листки**.
- Дополнительный файл сотрудников сейчас в UI не используется.

## Ключевая логика генерации

- лимиты сотрудника берутся из расчетного листка (дни/часы);
- распределение смен более равномерное по месяцу;
- учитываются выходные/праздничные дни (для распределения нагрузки);
- поддерживаются группы взаимозаменяемости ролей:
  - `Кухня`, `Зал`, `Касса`, `Бар`, `Обслуживание`;
- группы можно менять вручную в UI перед генерацией;
- поддерживаются межресторанные замены;
- часть ролей считается необязательной для полного покрытия (дефицит по ним не блокирует результат).

## Структура проекта

- [app/main.py](/Users/macbook/Documents/Табеля сотрудников/app/main.py) — API, загрузка/генерация/скачивание.
- [app/excel_parsers.py](/Users/macbook/Documents/Табеля сотрудников/app/excel_parsers.py) — парсинг расчетных листков.
- [app/scheduler.py](/Users/macbook/Documents/Табеля сотрудников/app/scheduler.py) — генерация графика, экспорт Excel/PDF.
- [app/logging_utils.py](/Users/macbook/Documents/Табеля сотрудников/app/logging_utils.py) — логирование и error-логи.
- [static/index.html](/Users/macbook/Documents/Табеля сотрудников/static/index.html) + `styles.css`, `app.js` — frontend.
- [templates/t13_template.xlsx](/Users/macbook/Documents/Табеля сотрудников/templates/t13_template.xlsx) — шаблон Т‑13.
- [render.yaml](/Users/macbook/Documents/Табеля сотрудников/render.yaml) — деплой на Render.
- `outputs/` — сгенерированные файлы.
- `logs/` — `app.log` и `logs/errors/*.log`.

## Локальный запуск

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Открыть: [http://localhost:8000](http://localhost:8000)

Также можно запускать через:

```bash
./start.command
```

## Docker

```bash
docker compose up --build
```

## API (актуально)

- `POST /upload` — загрузка расчетных листков.
- `POST /generate` — генерация графика и файлов.
- `GET /preview?offset=&limit=` — постраничный preview.
- `GET /download` — скачать график Excel.
- `GET /download_t13` — скачать Т‑13 Excel.
- `GET /download_t13_pdf` — скачать Т‑13 PDF.
- `GET /download_log` — скачать `app.log`.
- `GET /download_error_log/{error_id}` — скачать лог конкретной ошибки.
- `GET /version` — версия приложения.

## Т‑13 Excel и PDF

### Excel
- строится из шаблона `templates/t13_template.xlsx`;
- листы: общий + по подразделениям;
- данные сотрудников/дней/часов синхронизированы с расчетным листком и генерацией.

### PDF
- кнопка `Скачать табель Т-13 (PDF)` использует один endpoint;
- при наличии `LibreOffice (soffice)` PDF создается **конвертацией готового T‑13 Excel** (максимально идентичный вид);
- если `soffice` недоступен — применяется fallback PDF‑генерация через `reportlab`.

## Деплой на Render

Конфиг уже есть в [render.yaml](/Users/macbook/Documents/Табеля сотрудников/render.yaml):

- Build: `pip install -r requirements.txt`
- Start: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

Если нужен PDF максимально как Excel, используйте окружение/образ с установленным `LibreOffice`.

## Версии и логи

- версия: файл [VERSION](/Users/macbook/Documents/Табеля сотрудников/VERSION);
- история: [versions.md](/Users/macbook/Documents/Табеля сотрудников/versions.md);
- рабочий лог: `logs/app.log`;
- ошибки: `logs/errors/ERR_*.log`.
