import os, re, json, time, hashlib
from datetime import datetime
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

load_dotenv()

# ===== ENV =====
SUMMARY_SPREADSHEET_ID = os.environ["SUMMARY_SPREADSHEET_ID"]
OUR_GRID_ID = os.environ["OUR_GRID_ID"]
YANDEX_GRID_ID = os.environ["YANDEX_GRID_ID"]
SUMMARY_SETTINGS_SHEET_NAME = os.getenv("SUMMARY_SETTINGS_SHEET_NAME", "Settings")

TZ = os.getenv("TZ", "Asia/Almaty")

HOT_MONTH = os.getenv("HOT_MONTH", "Март 2026")
HOT_WRITE_INTERVAL_SEC = int(os.getenv("HOT_WRITE_INTERVAL_SEC", "15"))

COLD_REFRESH_SEC = int(os.getenv("COLD_REFRESH_SEC", str(24 * 60 * 60)))
COLD_MONTHS = set(m.strip().lower() for m in os.getenv(
    "COLD_MONTHS",
    "Февраль 2026,Январь 2026,Декабрь 2025"
).split(","))

MAX_DATA_ROWS = int(os.getenv("MAX_DATA_ROWS", "60"))
RED_GAP_ROWS = int(os.getenv("RED_GAP_ROWS", "5"))

WORK_START_HOUR = int(os.getenv("WORK_START_HOUR", "0"))
WORK_END_HOUR = int(os.getenv("WORK_END_HOUR", "24"))

META_TTL_SEC = int(os.getenv("META_TTL_SEC", "600"))
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# ===== Source parse keywords =====
KEYWORDS = {
    "MANAGER": ["менеджер", "сотрудник", "manager"],
    "OPF": ["опф", "форма"],
    "CONTRACT": ["договор", "контракт"],
    "ACCEPT": ["акцепт", "платежки", "оплата", "поехали"],
    "TAGS": ["метки", "наличие метки", "nib"],
}

# ===== “Do not touch” header detection =====
HEADER_MARKERS = [
    "менеджер", "менеджеры", "офферт", "офферты", "ип", "тоо",
    "договор", "акцепт", "акцепт %", "пусто", "другое", "красные",
    "метка"
]

_META_CACHE = {}

def now_local():
    return datetime.now(ZoneInfo(TZ))

def in_work_window(dt):
    return WORK_START_HOUR <= dt.hour < WORK_END_HOUR

def api_call_with_backoff(fn, max_retries=8, base_sleep=1.0):
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
    raise RuntimeError("Too many retries")

def get_service():
    sa_json = os.getenv("GCP_SA_JSON")
    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds_path = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
        creds = service_account.Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)

def read_values(service, spreadsheet_id, a1_range):
    def _call():
        return service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=a1_range,
            valueRenderOption="FORMATTED_VALUE",
        ).execute()
    resp = api_call_with_backoff(_call)
    return resp.get("values", [])

def write_values(service, spreadsheet_id, a1_range, values):
    def _call():
        return service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=a1_range,
            valueInputOption="RAW",
            body={"values": values},
        ).execute()
    api_call_with_backoff(_call)

def get_sheet_titles_lower_cached(service, spreadsheet_id):
    now = time.time()
    hit = _META_CACHE.get(spreadsheet_id)
    if hit and (now - hit["ts"] < META_TTL_SEC):
        return hit["data"]

    def _call():
        return service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

    meta = api_call_with_backoff(_call)
    mapping = {s["properties"]["title"].lower(): s["properties"]["title"] for s in meta.get("sheets", [])}
    _META_CACHE[spreadsheet_id] = {"ts": now, "data": mapping}
    return mapping

def ensure_sheet_exists(service, spreadsheet_id, sheet_name, titles_lower):
    key = sheet_name.lower()
    if key in titles_lower:
        return titles_lower[key]

    req = {"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]}
    def _call():
        return service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=req).execute()

    api_call_with_backoff(_call)
    _META_CACHE.pop(spreadsheet_id, None)
    titles_lower = get_sheet_titles_lower_cached(service, spreadsheet_id)
    return titles_lower.get(key, sheet_name)

def find_sheet_smart(titles_lower, partial_name):
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

def normalize_name(name):
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

def compute_hash(values):
    s = json.dumps(values, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

# ===== states =====
def state_path(report_sheet_name):
    safe = re.sub(r"[^a-zA-Z0-9_.-]+", "_", report_sheet_name)
    return f"/tmp/state_{safe}.json"

def read_state(report_sheet_name):
    try:
        with open(state_path(report_sheet_name), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def write_state(report_sheet_name, state):
    with open(state_path(report_sheet_name), "w", encoding="utf-8") as f:
        json.dump(state, f)

def cold_state_path():
    return "/tmp/cold_refresh_state.json"

def read_cold_state():
    try:
        with open(cold_state_path(), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def write_cold_state(st):
    with open(cold_state_path(), "w", encoding="utf-8") as f:
        json.dump(st, f)

def should_refresh_cold(key):
    st = read_cold_state()
    last = float(st.get(key, 0))
    return (time.time() - last) >= COLD_REFRESH_SEC

def mark_refreshed_cold(key):
    st = read_cold_state()
    st[key] = time.time()
    write_cold_state(st)

# ===== HARD “DO NOT TOUCH” logic =====
def row_looks_like_header(row_vals):
    if not row_vals:
        return False
    txt = " ".join(str(x).strip().lower() for x in row_vals if str(x).strip())
    if not txt:
        return False
    hits = sum(1 for m in HEADER_MARKERS if m in txt)
    return hits >= 2 or ("менедж" in txt)

def find_title_row(service, sheet_title, label, search_rows=120):
    vals = read_values(service, SUMMARY_SPREADSHEET_ID, f"{sheet_title}!A1:M{search_rows}")
    lab = label.lower()
    for i, row in enumerate(vals, start=1):
        txt = " ".join(str(x).strip().lower() for x in row if str(x).strip())
        if lab in txt:
            return i
    return None

def get_block_rows(service, sheet_title, label):
    """
    Returns: title_row, header_row, data_start_row
    NEVER touch title_row/header_row
    """
    title_row = find_title_row(service, sheet_title, label)
    if not title_row:
        raise RuntimeError(f"Block title '{label}' not found in '{sheet_title}'")

    header_row = title_row + 1
    data_start = title_row + 2

    # Safety: if data_start accidentally points to header -> STOP by shifting ONLY if header detected there
    vals = read_values(service, SUMMARY_SPREADSHEET_ID, f"{sheet_title}!A{data_start}:M{data_start}")
    row = vals[0] if vals else []
    if row_looks_like_header(row):
        # This means sheet layout is weird; shift once
        data_start += 1

    # Final safety: if data_start still looks like header -> ABORT (do not write)
    vals2 = read_values(service, SUMMARY_SPREADSHEET_ID, f"{sheet_title}!A{data_start}:M{data_start}")
    row2 = vals2[0] if vals2 else []
    if row_looks_like_header(row2):
        raise RuntimeError(f"Refusing to write: data_start_row={data_start} looks like HEADER for '{label}' in '{sheet_title}'")

    return title_row, header_row, data_start

# ===== analyze source =====
def analyze_single_sheet(service, source_id, source_titles_lower, sheet_name):
    if not sheet_name:
        return []
    real_name = find_sheet_smart(source_titles_lower, sheet_name)
    if not real_name:
        print(f"[WARN] Source tab not found: '{sheet_name}' in {source_id}")
        return []

    data = read_values(service, source_id, real_name)
    if len(data) < 2:
        return []

    headers = [str(h).lower().strip() for h in data[0]]

    idx = {
        "man": find_idx(headers, KEYWORDS["MANAGER"]),
        "opf": find_idx(headers, KEYWORDS["OPF"]),
        "contract": find_idx(headers, KEYWORDS["CONTRACT"]),
        "accept": find_idx(headers, KEYWORDS["ACCEPT"]),
        "tags": find_idx(headers, KEYWORDS["TAGS"]),
    }

    if idx["man"] == -1 and len(data) > 2:
        headers2 = [str(h).lower().strip() for h in data[1]]
        idx["man"] = find_idx(headers2, KEYWORDS["MANAGER"])
    if idx["man"] == -1:
        print(f"[WARN] Manager column not found in '{real_name}'")
        return []

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
        if not manager:
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

        opf_text = ""
        if idx["opf"] > -1 and idx["opf"] < len(row):
            opf_text += str(row[idx["opf"]]).lower()
        opf_text += " " + " ".join(str(x).lower() for x in row)

        if ("ип " in opf_text) or ('ип"' in opf_text) or ("жк " in opf_text):
            s["ip"] += 1
        if "тоо" in opf_text:
            s["too"] += 1

        if idx["contract"] > -1 and idx["contract"] < len(row):
            val = str(row[idx["contract"]]).lower().strip()
            if val not in ("", "нет", "0", "-", "—"):
                s["contract"] += 1

        if idx["accept"] > -1 and idx["accept"] < len(row):
            val = str(row[idx["accept"]]).lower()
            if len(val) > 1 and ("нет" not in val) and ("отказ" not in val) and ("ошибка" not in val):
                s["accept"] += 1

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
        percent_str = f"{round(percent * 100)}%"  # always string
        result.append([
            m, s["total"], s["ip"], s["too"], s["contract"], s["accept"], percent_str,
            s["nib_sale"], s["nib"], s["zero"], s["empty_tag"], s["other_tag"], s["red"]
        ])

    result.sort(key=lambda x: str(x[0]))
    return result

# ===== write (DATA ONLY) =====
def _pad_or_trim_row(row, width=13):
    row = list(row) if row else []
    if len(row) < width:
        row += [""] * (width - len(row))
    elif len(row) > width:
        row = row[:width]
    return row

def write_data_block(service, sheet_title, data_start_row, max_rows, data_rows):
    """
    Writes ONLY A{data_start_row}:M{data_start_row+max_rows-1}
    NEVER touches header/title rows because data_start_row is computed as title+2 and guarded.
    SAFETY: if data_rows empty -> DO NOTHING (no cleanup).
    """
    width = 13
    clean = [_pad_or_trim_row(r, width) for r in (data_rows or [])]
    clean = clean[:max_rows]

    if len(clean) == 0:
        print(f"[WARN] No data -> skip write/cleanup for {sheet_title} start {data_start_row}")
        return

    end_row = data_start_row + len(clean) - 1
    block_end_row = data_start_row + max_rows - 1

    write_values(service, SUMMARY_SPREADSHEET_ID, f"{sheet_title}!A{data_start_row}:M{end_row}", clean)

    tail_start = end_row + 1
    if tail_start <= block_end_row:
        blanks = [[""] * width for _ in range(block_end_row - tail_start + 1)]
        write_values(service, SUMMARY_SPREADSHEET_ID, f"{sheet_title}!A{tail_start}:M{block_end_row}", blanks)

def run_month_update(service, summary_titles_lower, our_titles_lower, yandex_titles_lower, month_name, our_sheet, yandex_sheet):
    report_sheet_name = f"Сводная - {month_name}"
    real_title = ensure_sheet_exists(service, SUMMARY_SPREADSHEET_ID, report_sheet_name, summary_titles_lower)

    # find blocks in SUMMARY sheet (auto, so rows can "float")
    our_title, our_header, our_data_start = get_block_rows(service, real_title, "НАША СЕТКА")
    y_title, y_header, y_data_start = get_block_rows(service, real_title, "ЯНДЕКС СЕТКА")

    # analyze sources
    our_data = analyze_single_sheet(service, OUR_GRID_ID, our_titles_lower, our_sheet)
    yandex_data = analyze_single_sheet(service, YANDEX_GRID_ID, yandex_titles_lower, yandex_sheet)

    # write ONLY data area
    write_data_block(service, real_title, our_data_start, 16, our_data)           # OUR block: 16 rows safe
    write_data_block(service, real_title, y_data_start, MAX_DATA_ROWS, yandex_data)

    updated = now_local().strftime("%d.%m %H:%M:%S")
    write_values(service, SUMMARY_SPREADSHEET_ID, f"{real_title}!N1", [[f"Обновлено: {updated}"]])
    return report_sheet_name

def run_summary_once():
    dt = now_local()
    if not in_work_window(dt):
        print("[INFO] outside work window -> skip")
        return

    service = get_service()

    summary_titles_lower = get_sheet_titles_lower_cached(service, SUMMARY_SPREADSHEET_ID)
    our_titles_lower = get_sheet_titles_lower_cached(service, OUR_GRID_ID)
    yandex_titles_lower = get_sheet_titles_lower_cached(service, YANDEX_GRID_ID)

    settings = read_values(service, SUMMARY_SPREADSHEET_ID, f"{SUMMARY_SETTINGS_SHEET_NAME}!A2:B")

    pairs = []
    for row in settings:
        a = row[0] if len(row) > 0 else ""
        b = row[1] if len(row) > 1 else ""
        month = (a or b or "").strip()
        if not month:
            continue
        pairs.append((month, str(a).strip(), str(b).strip()))

    if not pairs:
        print("[WARN] Settings empty")
        return

    # HOT
    hot = None
    for month, a, b in pairs:
        if month.lower() == HOT_MONTH.strip().lower():
            hot = (month, a, b)
            break

    if hot:
        month, a, b = hot
        report_sheet_name = f"Сводная - {month}"
        st = read_state(report_sheet_name)

        last_check = float(st.get("last_check_ts", 0))
        if (time.time() - last_check) < HOT_WRITE_INTERVAL_SEC:
            print(f"[INFO] HOT THROTTLE (skip reads): {report_sheet_name}")
        else:
            our_data = analyze_single_sheet(service, OUR_GRID_ID, our_titles_lower, a)
            yandex_data = analyze_single_sheet(service, YANDEX_GRID_ID, yandex_titles_lower, b)
            new_hash = compute_hash([our_data, yandex_data])

            throttled_write = (time.time() - float(st.get("last_write_ts", 0))) < HOT_WRITE_INTERVAL_SEC
            changed = st.get("hash") != new_hash

            if changed and not throttled_write:
                run_month_update(service, summary_titles_lower, our_titles_lower, yandex_titles_lower, month, a, b)
                st["hash"] = new_hash
                st["last_write_ts"] = time.time()
                print(f"[OK] HOT SYNC: {report_sheet_name}")
            else:
                print(f"[INFO] HOT {'THROTTLE' if throttled_write else 'NO-CHANGE'}: {report_sheet_name}")

            st["last_check_ts"] = time.time()
            write_state(report_sheet_name, st)
    else:
        print(f"[WARN] HOT_MONTH '{HOT_MONTH}' not found in Settings")

    # COLD
    for month, a, b in pairs:
        ml = month.lower().strip()
        if ml == HOT_MONTH.strip().lower():
            continue
        if ml not in COLD_MONTHS:
            continue
        if not should_refresh_cold(ml):
            continue
        run_month_update(service, summary_titles_lower, our_titles_lower, yandex_titles_lower, month, a, b)
        mark_refreshed_cold(ml)
        print(f"[OK] COLD SYNC (daily): Сводная - {month}")