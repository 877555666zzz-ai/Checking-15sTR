import os
import re
import json
import time
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

# ====== ENV ======
SUMMARY_SPREADSHEET_ID = os.environ["SUMMARY_SPREADSHEET_ID"]     # куда пишем "Сводная - ..."
OUR_GRID_ID = os.environ["OUR_GRID_ID"]                           # откуда читаем "Наша сетка"
YANDEX_GRID_ID = os.environ["YANDEX_GRID_ID"]                     # откуда читаем "Яндекс сетка"

SUMMARY_SETTINGS_SHEET_NAME = os.getenv("SUMMARY_SETTINGS_SHEET_NAME", "Settings")
TZ = os.getenv("TZ", "Asia/Almaty")

# worker каждые 15 сек, но писать не чаще этого (и только если изменилось)
MIN_WRITE_INTERVAL_SEC = int(os.getenv("SUMMARY_MIN_WRITE_INTERVAL_SEC", "30"))

# "красная зона" начинается после N пустых строк подряд в колонке менеджера
RED_GAP_ROWS = int(os.getenv("RED_GAP_ROWS", "5"))

# если хочешь ограничить по времени, можно поменять (по умолчанию 0-24)
WORK_START_HOUR = int(os.getenv("WORK_START_HOUR", "0"))
WORK_END_HOUR = int(os.getenv("WORK_END_HOUR", "24"))

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

KEYWORDS = {
    "MANAGER": ["менеджер", "сотрудник", "manager"],
    "OPF": ["опф", "форма"],
    "CONTRACT": ["договор", "контракт"],
    "ACCEPT": ["акцепт", "платежки", "оплата", "поехали"],
    "TAGS": ["метки", "наличие метки", "nib"],
}


def now_local() -> datetime:
    return datetime.now(ZoneInfo(TZ))


def in_work_window(dt: datetime) -> bool:
    return WORK_START_HOUR <= dt.hour < WORK_END_HOUR


def normalize_name(name: str):
    if not name:
        return None
    s = str(name).strip()
    if len(s) < 2:
        return None
    return s[:1].upper() + s[1:].lower()


def find_idx(headers, keywords):
    for i, h in enumerate(headers):
        for k in keywords:
            if k in h:
                return i
    return -1


def get_service():
    """
    Railway: используем переменную GCP_SA_JSON (полный текст json).
    Локально можно использовать GOOGLE_APPLICATION_CREDENTIALS (путь к json файлу).
    """
    sa_json = os.getenv("GCP_SA_JSON")
    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)

    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def read_values(service, spreadsheet_id: str, a1_range: str):
    # аналог getDisplayValues()
    resp = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=a1_range,
        valueRenderOption="FORMATTED_VALUE",
    ).execute()
    return resp.get("values", [])


def clear_sheet(service, spreadsheet_id: str, sheet_name: str):
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=sheet_name,
        body={},
    ).execute()


def write_values(service, spreadsheet_id: str, a1_range: str, values):
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=a1_range,
        valueInputOption="RAW",
        body={"values": values},
    ).execute()


def get_spreadsheet_metadata(service, spreadsheet_id: str):
    return service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()


def ensure_sheet_exists(service, spreadsheet_id: str, sheet_name: str):
    meta = get_spreadsheet_metadata(service, spreadsheet_id)
    for s in meta.get("sheets", []):
        if s["properties"]["title"] == sheet_name:
            return
    req = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=req).execute()


def list_sheets(service, spreadsheet_id: str):
    meta = get_spreadsheet_metadata(service, spreadsheet_id)
    return [s["properties"]["title"] for s in meta.get("sheets", [])]


def find_sheet_smart(service, spreadsheet_id: str, partial_name: str):
    """
    Как в GAS: сначала exact, потом fuzzy includes по имени листа.
    """
    if not partial_name:
        return None

    search = str(partial_name).strip()
    search_low = search.lower()
    search_clean = re.sub(r"\s+", "", search_low)

    names = list_sheets(service, spreadsheet_id)

    # exact
    for n in names:
        if n == search:
            return n

    # fuzzy
    for n in names:
        clean = re.sub(r"\s+", "", n.lower())
        if (search_clean in clean) or (clean in search_clean):
            return n

    return None


def compute_hash(values) -> str:
    s = json.dumps(values, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def state_path(report_sheet_name: str) -> str:
    safe = re.sub(r"[^a-zA-Z0-9_.-]+", "_", report_sheet_name)
    return f"/tmp/summary_state_{safe}.json"


def read_state(report_sheet_name: str):
    p = state_path(report_sheet_name)
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def write_state(report_sheet_name: str, state: dict):
    p = state_path(report_sheet_name)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(state, f)


def analyze_single_sheet(service, source_id: str, sheet_name: str):
    """
    1-в-1 логика GAS analyzeSingleSheet():
    - ищем лист умно
    - шапка по ключевым словам
    - "красная зона" после RED_GAP_ROWS пустых строк по менеджеру
    - считаем totals/ip/too/contract/accept/nib_sale/nib/zero/empty/other/red
    - сортировка А-Я
    """
    if not sheet_name:
        return [["Нет листа (пусто в Settings)"]]

    real_name = find_sheet_smart(service, source_id, sheet_name)
    if not real_name:
        return [[f'❌ Лист "{sheet_name}" не найден']]

    data = read_values(service, source_id, real_name)
    if len(data) < 2:
        return [["Лист пуст"]]

    headers = [str(h).lower().strip() for h in data[0]]

    idx = {
        "man": find_idx(headers, KEYWORDS["MANAGER"]),
        "opf": find_idx(headers, KEYWORDS["OPF"]),
        "contract": find_idx(headers, KEYWORDS["CONTRACT"]),
        "accept": find_idx(headers, KEYWORDS["ACCEPT"]),
        "tags": find_idx(headers, KEYWORDS["TAGS"]),
    }

    # если шапка смещена
    if idx["man"] == -1 and len(data) > 2:
        headers2 = [str(h).lower().strip() for h in data[1]]
        idx["man"] = find_idx(headers2, KEYWORDS["MANAGER"])

    if idx["man"] == -1:
        return [["Не найдена колонка \"Менеджер\""]]

    stats = {}
    is_red_section = False
    consecutive_empty_rows = 0

    for i in range(1, len(data)):
        row = data[i]
        manager_raw = row[idx["man"]] if idx["man"] < len(row) else ""

        # разрыв таблицы
        if not manager_raw or str(manager_raw).strip() == "":
            consecutive_empty_rows += 1
            if consecutive_empty_rows >= RED_GAP_ROWS:
                is_red_section = True
            continue
        else:
            consecutive_empty_rows = 0

        manager = normalize_name(manager_raw)
        if not manager or manager.lower() == "менеджер":
            continue

        if manager not in stats:
            stats[manager] = {
                "total": 0, "ip": 0, "too": 0, "contract": 0, "accept": 0,
                "nib_sale": 0, "nib": 0, "zero": 0, "empty_tag": 0, "other_tag": 0, "red": 0
            }

        s = stats[manager]

        # красная зона
        if is_red_section:
            s["red"] += 1
            continue

        # основная зона
        s["total"] += 1

        # ОПФ (как в GAS: опф + вся строка)
        opf_text = ""
        if idx["opf"] > -1 and idx["opf"] < len(row):
            opf_text += str(row[idx["opf"]]).lower()
        opf_text += " " + " ".join(str(x).lower() for x in row)

        if ("ип " in opf_text) or ('ип"' in opf_text) or ("жк " in opf_text):
            s["ip"] += 1
        if "тоо" in opf_text:
            s["too"] += 1

        # Договор
        if idx["contract"] > -1 and idx["contract"] < len(row):
            val = str(row[idx["contract"]]).lower().strip()
            if val not in ("", "нет", "0", "-", "—"):
                s["contract"] += 1

        # Акцепт
        if idx["accept"] > -1 and idx["accept"] < len(row):
            val = str(row[idx["accept"]]).lower()
            if len(val) > 1 and ("нет" not in val) and ("отказ" not in val) and ("ошибка" not in val):
                s["accept"] += 1

        # Метки
        tag_val = str(row[idx["tags"]]).lower().strip() if idx["tags"] > -1 and idx["tags"] < len(row) else ""
        if "nib_sale" in tag_val:
            s["nib_sale"] += 1
        elif tag_val == "nib" or " nib " in f" {tag_val} ":
            s["nib"] += 1
        elif tag_val in ("0", "0.0"):
            s["zero"] += 1
        elif tag_val == "":
            s["empty_tag"] += 1
        else:
            s["other_tag"] += 1

        # доп красные по слову
        if "красн" in " ".join(str(x).lower() for x in row):
            s["red"] += 1

    # финальный массив
    result = []
    for m, s in stats.items():
        percent = (s["accept"] / s["total"]) if s["total"] > 0 else 0
        result.append([
            m, s["total"], s["ip"], s["too"], s["contract"], s["accept"], percent,
            s["nib_sale"], s["nib"], s["zero"], s["empty_tag"], s["other_tag"], s["red"]
        ])

    result.sort(key=lambda x: str(x[0]))
    return result


def col_to_a1(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def build_report_values(our_title: str, our_data, yandex_title: str, yandex_data):
    headers = [
        "Менеджеры", "Офферты всего", "ИП", "ТОО", "Договор есть", "Акцепт/Оплата",
        "Акцепт %", "Метка nib_sales", "Метка nib", "Метка 0", "Пусто", "Другое", "Красные"
    ]

    values = []

    # НАША СЕТКА
    values.append([our_title] + [""] * 12)
    values.append(headers)
    if our_data and len(our_data[0]) == 1:
        values.append([our_data[0][0]] + [""] * 12)
    elif our_data:
        values.extend(our_data)
    else:
        values.append(["Нет данных"] + [""] * 12)

    # gap 5 rows
    for _ in range(5):
        values.append([""] * 13)

    # ЯНДЕКС СЕТКА
    values.append([yandex_title] + [""] * 12)
    values.append(headers)
    if yandex_data and len(yandex_data[0]) == 1:
        values.append([yandex_data[0][0]] + [""] * 12)
    elif yandex_data:
        values.extend(yandex_data)
    else:
        values.append(["Нет данных"] + [""] * 12)

    return values


def update_one_month(service, our_sheet_name: str, yandex_sheet_name: str):
    raw_name = (our_sheet_name or yandex_sheet_name or "").strip()
    report_sheet_name = f"Сводная - {raw_name}"

    our_data = analyze_single_sheet(service, OUR_GRID_ID, our_sheet_name)
    yandex_data = analyze_single_sheet(service, YANDEX_GRID_ID, yandex_sheet_name)

    values = build_report_values(
        f"НАША СЕТКА ({our_sheet_name or '-'})",
        our_data,
        f"ЯНДЕКС СЕТКА ({yandex_sheet_name or '-'})",
        yandex_data,
    )

    # hash + throttle
    new_hash = compute_hash(values)
    st = read_state(report_sheet_name)
    old_hash = st.get("hash")
    last_write = float(st.get("last_write_ts", 0))

    if old_hash == new_hash:
        print(f"[INFO] SUMMARY NO-CHANGE: {report_sheet_name}")
        return

    if time.time() - last_write < MIN_WRITE_INTERVAL_SEC:
        print(f"[INFO] SUMMARY THROTTLE: {report_sheet_name}")
        return

    ensure_sheet_exists(service, SUMMARY_SPREADSHEET_ID, report_sheet_name)

    rows = len(values)
    cols = 13
    end_a1 = f"{col_to_a1(cols)}{rows}"
    rng = f"{report_sheet_name}!A1:{end_a1}"

    clear_sheet(service, SUMMARY_SPREADSHEET_ID, report_sheet_name)
    write_values(service, SUMMARY_SPREADSHEET_ID, rng, values)

    updated = now_local().strftime("%d.%m %H:%M")
    write_values(service, SUMMARY_SPREADSHEET_ID, f"{report_sheet_name}!N1", [[f"Обновлено: {updated}"]])

    write_state(report_sheet_name, {"hash": new_hash, "last_write_ts": time.time()})
    print(f"[OK] SUMMARY SYNC: {report_sheet_name} ({rows}x13)")


def run_summary_once():
    dt = now_local()
    if not in_work_window(dt):
        print("[INFO] SUMMARY вне времени — пропуск")
        return

    service = get_service()

    settings = read_values(service, SUMMARY_SPREADSHEET_ID, f"{SUMMARY_SETTINGS_SHEET_NAME}!A2:B")
    if not settings:
        print("[WARN] SUMMARY Settings пустой (A2:B)")
        return

    for row in settings:
        our_name = row[0] if len(row) > 0 else ""
        yandex_name = row[1] if len(row) > 1 else ""
        if (not str(our_name).strip()) and (not str(yandex_name).strip()):
            continue
        update_one_month(service, str(our_name).strip(), str(yandex_name).strip())


if __name__ == "__main__":
    try:
        run_summary_once()
    except HttpError as e:
        print("[ERROR] SUMMARY Google API:", e)
        raise