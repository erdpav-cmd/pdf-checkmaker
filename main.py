from __future__ import annotations

import csv
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any

from weasyprint import HTML


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
TEMPLATES_DIR = BASE_DIR / "templates"
OUTPUT_DIR = BASE_DIR / "output"

SUPPORTED_DATA_SUFFIXES = {".csv", ".json"}
SUPPORTED_TEMPLATE_SUFFIXES = {".html", ".htm"}
INVOICE_ID_CANDIDATES = (
    "invoice_id",
    "invoiceid",
    "invoice id",
    "invoice",
    "id",
)
ITEM_HEADER_LABELS = {
    "name": "Наименование",
    "title": "Наименование",
    "description": "Описание",
    "quantity": "Количество",
    "qty": "Количество",
    "price": "Цена",
    "unit_price": "Цена",
    "amount": "Сумма",
    "sum": "Сумма",
    "total": "Итого",
    "currency": "Валюта",
    "sku": "Артикул",
}


class InvoiceError(Exception):
    pass


def print_header(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def list_files(directory: Path, suffixes: set[str]) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(
        [path for path in directory.iterdir() if path.is_file() and path.suffix.lower() in suffixes],
        key=lambda path: path.name.lower(),
    )


def print_numbered_menu(title: str, items: list[str]) -> None:
    print_header(title)
    if not items:
        print("  Нет доступных вариантов.")
        return
    for index, item in enumerate(items, start=1):
        print(f"  {index}. {item}")


def choose_from_menu(prompt: str, items: list[Any]) -> Any:
    if not items:
        raise InvoiceError("Список вариантов пуст.")

    while True:
        raw_value = input(f"\n{prompt}: ").strip()
        if not raw_value.isdigit():
            print("Введите номер варианта.")
            continue

        index = int(raw_value) - 1
        if 0 <= index < len(items):
            return items[index]

        print("Номер вне диапазона, попробуйте еще раз.")


def normalized_key_map(row: dict[str, Any]) -> dict[str, str]:
    return {normalize_key(key): key for key in row.keys()}


def normalize_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def detect_invoice_key(row: dict[str, Any]) -> str:
    key_map = normalized_key_map(row)
    for candidate in INVOICE_ID_CANDIDATES:
        normalized_candidate = normalize_key(candidate)
        if normalized_candidate in key_map:
            return key_map[normalized_candidate]
    raise InvoiceError(
        "Не удалось найти поле invoice id. Ожидаются столбцы вида invoice_id, invoice id или id."
    )


def read_csv_file(path: Path) -> dict[str, dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    if not rows:
        raise InvoiceError(f"CSV-файл '{path.name}' пуст.")

    invoice_key = detect_invoice_key(rows[0])
    grouped_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in rows:
        invoice_id = str(row.get(invoice_key, "")).strip()
        if not invoice_id:
            continue
        cleaned_row = {str(key): value for key, value in row.items()}
        grouped_rows[invoice_id].append(cleaned_row)

    if not grouped_rows:
        raise InvoiceError(f"В CSV-файле '{path.name}' не найдено ни одного invoice id.")

    result: dict[str, dict[str, Any]] = {}
    for invoice_id, items in grouped_rows.items():
        first_row = items[0]
        invoice = {
            **first_row,
            "invoice_id": invoice_id,
            "items": items,
            "source_file": path.name,
        }
        result[invoice_id] = invoice

    return result


def read_json_file(path: Path) -> dict[str, dict[str, Any]]:
    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)

    invoices = extract_invoices_from_json(payload)
    if not invoices:
        raise InvoiceError(f"В JSON-файле '{path.name}' не найдено чеков.")

    result: dict[str, dict[str, Any]] = {}
    for invoice in invoices:
        invoice_id = extract_invoice_id(invoice)
        invoice_copy = dict(invoice)
        invoice_copy["invoice_id"] = invoice_id
        invoice_copy["source_file"] = path.name

        items = invoice_copy.get("items")
        if not isinstance(items, list):
            invoice_copy["items"] = []

        result[invoice_id] = invoice_copy

    return result


def extract_invoices_from_json(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        if isinstance(payload.get("invoices"), list):
            return [item for item in payload["invoices"] if isinstance(item, dict)]

        if extract_invoice_id(payload, allow_missing=True):
            return [payload]

        invoices: list[dict[str, Any]] = []
        for key, value in payload.items():
            if isinstance(value, dict):
                invoice = dict(value)
                invoice.setdefault("invoice_id", key)
                invoices.append(invoice)
        return invoices

    return []


def extract_invoice_id(invoice: dict[str, Any], allow_missing: bool = False) -> str:
    key_map = normalized_key_map(invoice)
    for candidate in INVOICE_ID_CANDIDATES:
        normalized_candidate = normalize_key(candidate)
        source_key = key_map.get(normalized_candidate)
        if not source_key:
            continue
        value = str(invoice.get(source_key, "")).strip()
        if value:
            return value

    if allow_missing:
        return ""

    raise InvoiceError(
        "В JSON-объекте не найден invoice id. Ожидается поле invoice_id, invoice id или id."
    )


def load_data_file(path: Path) -> dict[str, dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return read_csv_file(path)
    if suffix == ".json":
        return read_json_file(path)
    raise InvoiceError(f"Неподдерживаемый формат файла: {path.name}")


def flatten_data(value: Any, prefix: str = "") -> dict[str, str]:
    result: dict[str, str] = {}

    if isinstance(value, dict):
        for key, item in value.items():
            normalized = normalize_key(key)
            next_prefix = f"{prefix}_{normalized}" if prefix else normalized
            result.update(flatten_data(item, next_prefix))
    elif isinstance(value, list):
        return result
    elif value is not None:
        result[prefix] = escape(str(value))

    return result


def build_items_table(items: list[Any]) -> str:
    dict_items = [item for item in items if isinstance(item, dict)]
    if not dict_items:
        return "<p>Нет позиций для отображения.</p>"

    headers: list[str] = []
    for item in dict_items:
        for key in item.keys():
            if key not in headers:
                headers.append(str(key))

    header_html = "".join(f"<th>{escape(get_item_header_label(header))}</th>" for header in headers)
    row_html_parts: list[str] = []

    for item in dict_items:
        cells = "".join(f"<td>{escape(str(item.get(header, '')))}</td>" for header in headers)
        row_html_parts.append(f"<tr>{cells}</tr>")

    return (
        "<table>"
        "<thead><tr>"
        f"{header_html}"
        "</tr></thead>"
        "<tbody>"
        f"{''.join(row_html_parts)}"
        "</tbody>"
        "</table>"
    )


def get_item_header_label(header: str) -> str:
    normalized_header = normalize_key(header)
    return ITEM_HEADER_LABELS.get(normalized_header, header.replace("_", " ").capitalize())


def build_template_context(invoice: dict[str, Any]) -> dict[str, str]:
    items = invoice.get("items")
    if not isinstance(items, list):
        items = []

    context = flatten_data(invoice)
    context["invoice_json"] = escape(json.dumps(invoice, ensure_ascii=False, indent=2))
    context["items_json"] = escape(json.dumps(items, ensure_ascii=False, indent=2))
    context["items_table"] = build_items_table(items)
    context["generated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    context["invoice_id"] = str(invoice.get("invoice_id", ""))
    return context


def render_html_template(template_text: str, context: dict[str, str]) -> str:
    pattern = re.compile(r"{{\s*([a-zA-Z0-9_]+)\s*}}")

    def replacer(match: re.Match[str]) -> str:
        key = match.group(1)
        return str(context.get(key, ""))

    return pattern.sub(replacer, template_text)


def build_global_css() -> str:
    font_css = build_font_css()
    return f"""
    {font_css}
    body {{
      font-family: "DejaVu Sans", "Roboto", "Segoe UI", Arial, sans-serif;
      font-size: 12px;
      line-height: 1.5;
      color: #222;
      margin: 24px;
    }}
    h1, h2, h3, h4 {{
      margin-bottom: 8px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 12px;
    }}
    th, td {{
      border: 1px solid #cfcfcf;
      padding: 6px 8px;
      text-align: left;
      vertical-align: top;
    }}
    th {{
      background: #f5f5f5;
    }}
    pre {{
      white-space: pre-wrap;
      word-break: break-word;
    }}
"""


def prepare_html_document(rendered_html: str) -> str:
    global_css = build_global_css()
    style_block = f"<style>{global_css}</style>"

    if re.search(r"<head\b", rendered_html, flags=re.IGNORECASE):
        return re.sub(r"</head>", style_block + "</head>", rendered_html, count=1, flags=re.IGNORECASE)

    if re.search(r"<html\b", rendered_html, flags=re.IGNORECASE):
        return re.sub(r"<html[^>]*>", lambda match: match.group(0) + f"<head>{style_block}</head>", rendered_html, count=1, flags=re.IGNORECASE)

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  {style_block}
</head>
<body>
{rendered_html}
</body>
</html>
"""


def build_font_css() -> str:
    font_candidates = [
        ("DejaVu Sans", Path(r"C:\Windows\Fonts\DejaVuSans.ttf")),
        ("Roboto", Path(r"C:\Windows\Fonts\Roboto-Regular.ttf")),
        ("DejaVu Sans", Path("/Library/Fonts/DejaVuSans.ttf")),
        ("Roboto", Path("/Library/Fonts/Roboto-Regular.ttf")),
    ]

    for font_name, font_path in font_candidates:
        if font_path.exists():
            font_uri = font_path.resolve().as_uri()
            return (
                "@font-face {"
                f'font-family: "{font_name}";'
                f'src: url("{font_uri}") format("truetype");'
                "}"
            )

    return (
        '@font-face {font-family: "DejaVu Sans"; src: local("DejaVu Sans");}'
        '@font-face {font-family: "Roboto"; src: local("Roboto");}'
    )


def open_file_in_system_viewer(path: Path) -> None:
    if sys.platform.startswith("win"):
        os.startfile(str(path))
        return

    if sys.platform == "darwin":
        subprocess.run(["open", str(path)], check=False)
        return

    subprocess.run(["xdg-open", str(path)], check=False)


def generate_pdf(invoice: dict[str, Any], template_path: Path) -> Path:
    template_text = template_path.read_text(encoding="utf-8")
    context = build_template_context(invoice)
    rendered_html = render_html_template(template_text, context)
    full_html = prepare_html_document(rendered_html)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    safe_invoice_id = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(invoice["invoice_id"]))
    output_path = OUTPUT_DIR / f"invoice_{safe_invoice_id}.pdf"

    HTML(string=full_html, base_url=str(BASE_DIR)).write_pdf(str(output_path))
    return output_path


def main() -> None:
    data_files = list_files(DATA_DIR, SUPPORTED_DATA_SUFFIXES)
    template_files = list_files(TEMPLATES_DIR, SUPPORTED_TEMPLATE_SUFFIXES)

    print_numbered_menu("Доступные файлы с данными", [path.name for path in data_files])
    print_numbered_menu("Доступные HTML-шаблоны", [path.name for path in template_files])

    if not data_files:
        raise InvoiceError(f"В папке '{DATA_DIR.name}' не найдено CSV или JSON файлов.")
    if not template_files:
        raise InvoiceError(f"В папке '{TEMPLATES_DIR.name}' не найдено HTML-шаблонов.")

    selected_data_file = choose_from_menu("Выберите файл с данными", data_files)
    selected_template = choose_from_menu("Выберите HTML-шаблон", template_files)

    invoices = load_data_file(selected_data_file)
    invoice_ids = sorted(invoices.keys())

    print_numbered_menu("Доступные чеки (invoice id)", invoice_ids)
    selected_invoice_id = choose_from_menu("Выберите invoice id", invoice_ids)

    output_path = generate_pdf(invoices[selected_invoice_id], selected_template)

    print_header("Готово")
    print(f"PDF сохранен: {output_path}")
    print("Файл будет открыт в системной программе...")
    open_file_in_system_viewer(output_path)


if __name__ == "__main__":
    try:
        main()
    except InvoiceError as error:
        print(f"\nОшибка: {error}")
    except KeyboardInterrupt:
        print("\nОперация отменена пользователем.")
