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

SUMMARY_SPREADSHEET_ID = os.environ["SUMMARY_SPREADSHEET_ID"]
OUR_GRID_ID = os.environ["OUR_GRID_ID"]
YANDEX_GRID_ID = os.environ["YANDEX_GRID_ID"]

SUMMARY_SETTINGS_SHEET_NAME = os.getenv("SUMMARY_SETTINGS_SHEET_NAME", "Settings")
TZ = os.getenv("TZ", "Asia/Almaty")

# запуск каждые 15 сек, но запись не чаще этого (на каждый лист отдельно)
MIN_WRITE_INTERVAL_SEC = int(os.getenv("SUMMARY_MIN_WRITE_INTERVAL_SEC", "60"))

# красная зона после N пустых строк подряд по менеджеру
RED_GAP_ROWS = int(os.getenv("RED_GAP_ROWS", "5"))

# ограничить по времени (по умолчанию всегда)
WORK_START_HOUR = int(os.getenv("WORK_START_HOUR", "0"))
WORK_END_HOUR = int(os.getenv("WORK_END_HOUR", "24"))

# сколько месяцев считать за один запуск (чтобы не упираться в 60 read/min)
MAX_MONTHS_PER_RUN = int(os.getenv("MAX_MONTHS_PER_RUN", "1"))

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

KEYWORDS = {
    "MANAGER": ["менеджер", "сотрудник", "manager"],
    "OPF": ["опф", "форма"],
    "CONTRACT": ["договор", "контракт"],
    "ACCEPT": ["акцепт", "платежки", "оплата", "поехали"],
    "TAGS": ["метки", "наличие метки", "nib"],
}


# ---------------- time ----------------
def now_local() -> datetime:
    return datetime.now(ZoneInfo(TZ))


def in_work_window(dt: datetime) -> bool:
    return WORK_START_HOUR <= dt.hour < WORK_END_HOUR


# ---------------- utils ----------------
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


def col_to_a1(n: int) -> str:
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


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


def cursor_path() -> str:
    return "/tmp/summary_cursor.json"


def read_cursor():
    try:
        with open(cursor_path(), "r", encoding="utf-8") as f:
            return json.load(f).get("i", 0)
    except Exception:
        return 0


def write_cursor(i: int):
    with open(cursor_path(), "w", encoding="utf-8") as f:
        json.dump({"i": i}, f)


# ---------------- API helpers ----------------
def api_call_with_backoff(fn, *, max_retries=8, base_sleep=1.0):
    """
    Backoff для 429/5xx. Не валим процесс мгновенно.
    """
    for attempt in range(max_retries):
        try:
            return fn()
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            if status in (429, 500, 503):
                sleep_s = base_sleep * (2 ** attempt)
                print(f"[WARN] API {status}, retry in {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue
            raise
    raise RuntimeError("Too many retries for Google API")


def get_service():
    sa_json = os.getenv("GCP_SA_JSON")
    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)

    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def read_values(service, spreadsheet_id: str, a1_range: str):
    def _call():
        return service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=a1_range,
            valueRenderOption="FORMATTED_VALUE",  # как getDisplayValues()
        ).execute()
    resp = api_call_with_backoff(_call)
    return resp.get("values", [])


def write_values(service, spreadsheet_id: str, a1_range: str, values):
    def _call():
        return service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=a1_range,
            valueInputOption="RAW",
            body={"values": values},
        ).execute()
    api_call_with_backoff(_call)


def clear_range(service, spreadsheet_id: str, a1_range: str):
    """
    ВАЖНО: чистим только диапазон, НЕ весь лист (чтобы не слетали ширины/мерджи/стили).
    """
    def _call():
        return service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=a1_range,
            body={},
        ).execute()
    api_call_with_backoff(_call)


def get_sheet_titles_lower(service, spreadsheet_id: str):
    def _call():
        return service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    meta = api_call_with_backoff(_call)
    mapping = {}
    for s in meta.get("sheets", []):
        title = s["properties"]["title"]
        mapping[title.lower()] = title
    return mapping


def ensure_sheet_exists(service, spreadsheet_id: str, sheet_name: str, titles_lower: dict):
    """
    FIX: без учета регистра. Если лист уже есть — просто возвращаем реальное имя.
    """
    key = sheet_name.lower()
    if key in titles_lower:
        return titles_lower[key]

    req = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}

    def _call():
        return service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=req).execute()

    try:
        api_call_with_backoff(_call)
        titles_lower[key] = sheet_name
        return sheet_name
    except HttpError as e:
        if "already exists" in str(e).lower():
            titles_lower.update(get_sheet_titles_lower(service, spreadsheet_id))
            return titles_lower.get(key, sheet_name)
        raise


def find_sheet_smart(titles_lower: dict, partial_name: str):
    """
    Ищем лист без лишних API запросов (по уже полученному списку).
    """
    if not partial_name:
        return None

    search = str(partial_name).strip().lower()
    search_clean = re.sub(r"\s+", "", search)

    if search in titles_lower:
        return titles_lower[search]

    for low, real in titles_lower.items():
        clean = re.sub(r"\s+", "", low)
        if (search_clean in clean) or (clean in search_clean):
            return real

    return None


# ---------------- core logic ----------------
def analyze_single_sheet(service, source_id: str, source_titles_lower: dict, sheet_name: str):
    if not sheet_name:
        return [["Нет листа (пусто в Settings)"]]

    real_name = find_sheet_smart(source_titles_lower, sheet_name)
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

        if is_red_section:
            s["red"] += 1
            continue

        s["total"] += 1

        # OPF (как в GAS: опф + вся строка)
        opf_text = ""
        if idx["opf"] > -1 and idx["opf"] < len(row):
            opf_text += str(row[idx["opf"]]).lower()
        opf_text += " " + " ".join(str(x).lower() for x in row)

        if ("ип " in opf_text) or ('ип"' in opf_text) or ("жк " in opf_text):
            s["ip"] += 1
        if "тоо" in opf_text:
            s["too"] += 1

        # contract
        if idx["contract"] > -1 and idx["contract"] < len(row):
            val = str(row[idx["contract"]]).lower().strip()
            if val not in ("", "нет", "0", "-", "—"):
                s["contract"] += 1

        # accept
        if idx["accept"] > -1 and idx["accept"] < len(row):
            val = str(row[idx["accept"]]).lower()
            if len(val) > 1 and ("нет" not in val) and ("отказ" not in val) and ("ошибка" not in val):
                s["accept"] += 1

        # tags
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

        if "красн" in " ".join(str(x).lower() for x in row):
            s["red"] += 1

    result = []
    for m, s in stats.items():
        percent = (s["accept"] / s["total"]) if s["total"] > 0 else 0
        result.append([
            m, s["total"], s["ip"], s["too"], s["contract"], s["accept"], percent,
            s["nib_sale"], s["nib"], s["zero"], s["empty_tag"], s["other_tag"], s["red"]
        ])

    result.sort(key=lambda x: str(x[0]))
    return result


def build_report_values(our_title: str, our_data, yandex_title: str, yandex_data):
    headers = [
        "Менеджеры", "Офферты всего", "ИП", "ТОО", "Договор есть", "Акцепт/Оплата",
        "Акцепт %", "Метка nib_sales", "Метка nib", "Метка 0", "Пусто", "Другое", "Красные"
    ]

    values = []
    values.append([our_title] + [""] * 12)
    values.append(headers)
    values.extend(our_data if (our_data and len(our_data[0]) != 1) else [[our_data[0][0]] + [""] * 12] if our_data else [["Нет данных"] + [""] * 12])

    for _ in range(5):
        values.append([""] * 13)

    values.append([yandex_title] + [""] * 12)
    values.append(headers)
    values.extend(yandex_data if (yandex_data and len(yandex_data[0]) != 1) else [[yandex_data[0][0]] + [""] * 12] if yandex_data else [["Нет данных"] + [""] * 12])

    return values


def update_one_month(service, summary_titles_lower: dict, our_titles_lower: dict, yandex_titles_lower: dict,
                     our_sheet_name: str, yandex_sheet_name: str):
    raw_name = (our_sheet_name or yandex_sheet_name or "").strip()
    report_sheet_name = f"Сводная - {raw_name}"

    our_data = analyze_single_sheet(service, OUR_GRID_ID, our_titles_lower, our_sheet_name)
    yandex_data = analyze_single_sheet(service, YANDEX_GRID_ID, yandex_titles_lower, yandex_sheet_name)

    values = build_report_values(
        f"НАША СЕТКА ({our_sheet_name or '-'})",
        our_data,
        f"ЯНДЕКС СЕТКА ({yandex_sheet_name or '-'})",
        yandex_data,
    )

    new_hash = compute_hash(values)
    st = read_state(report_sheet_name)
    old_hash = st.get("hash")
    last_write = float(st.get("last_write_ts", 0))

    if old_hash == new_hash:
        print(f"[INFO] NO-CHANGE: {report_sheet_name}")
        return

    if time.time() - last_write < MIN_WRITE_INTERVAL_SEC:
        print(f"[INFO] THROTTLE: {report_sheet_name}")
        return

    real_title = ensure_sheet_exists(service, SUMMARY_SPREADSHEET_ID, report_sheet_name, summary_titles_lower)

    rows = len(values)
    cols = 13
    end_a1 = f"{col_to_a1(cols)}{rows}"
    rng = f"{real_title}!A1:{end_a1}"

    # ЧИСТИМ ТОЛЬКО ДИАПАЗОН (а не весь лист!)
    clear_range(service, SUMMARY_SPREADSHEET_ID, rng)
    write_values(service, SUMMARY_SPREADSHEET_ID, rng, values)

    updated = now_local().strftime("%d.%m %H:%M")
    write_values(service, SUMMARY_SPREADSHEET_ID, f"{real_title}!N1", [[f"Обновлено: {updated}"]])

    write_state(report_sheet_name, {"hash": new_hash, "last_write_ts": time.time()})
    print(f"[OK] SYNC: {real_title} ({rows}x13)")


def run_summary_once():
    dt = now_local()
    if not in_work_window(dt):
        print("[INFO] вне времени — пропуск")
        return

    service = get_service()

    # КЭШ: 3 мета-запроса за цикл
    summary_titles_lower = get_sheet_titles_lower(service, SUMMARY_SPREADSHEET_ID)
    our_titles_lower = get_sheet_titles_lower(service, OUR_GRID_ID)
    yandex_titles_lower = get_sheet_titles_lower(service, YANDEX_GRID_ID)

    settings = read_values(service, SUMMARY_SPREADSHEET_ID, f"{SUMMARY_SETTINGS_SHEET_NAME}!A2:B")
    if not settings:
        print("[WARN] Settings пустой (A2:B)")
        return

    pairs = []
    for row in settings:
        our_name = row[0] if len(row) > 0 else ""
        yandex_name = row[1] if len(row) > 1 else ""
        if (not str(our_name).strip()) and (not str(yandex_name).strip()):
            continue
        pairs.append((str(our_name).strip(), str(yandex_name).strip()))

    if not pairs:
        print("[WARN] Нет ни одной пары в Settings")
        return

    # round-robin
    start = read_cursor() % len(pairs)
    end = start + max(1, MAX_MONTHS_PER_RUN)
    chosen = [pairs[i % len(pairs)] for i in range(start, end)]
    write_cursor(end % len(pairs))

    for our_name, yandex_name in chosen:
        update_one_month(
            service,
            summary_titles_lower,
            our_titles_lower,
            yandex_titles_lower,
            our_name,
            yandex_name,
        )