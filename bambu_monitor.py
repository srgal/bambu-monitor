#!/usr/bin/env python3
"""
Bambu Lab Bambu Lab printer — Print Monitor with Telegram Bot

Subscribes to the printer's MQTT feed and:
  - sends Telegram notifications on print start / finish / failure
  - tracks AMS filament usage per slot
  - integrates Claude AI for natural-language printer queries
  - records print history and statistics

Requirements:
    pip install paho-mqtt anthropic

Configuration:
  Edit the constants below (SERIAL, CLOUD_HOST, TELEGRAM_TOKEN, etc.)
  Run: python3 bambu_monitor.py

Camera integration is intentionally removed from this public version.
"""

import fcntl
import json
import os
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta

try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.exit("paho-mqtt not found. Install it with: pip install paho-mqtt")

# ── Printer config ────────────────────────────────────────────────────────────
SERIAL       = "YOUR_PRINTER_SERIAL_NUMBER"
TOPIC        = f"device/{SERIAL}/report"
TOPIC_REQ    = f"device/{SERIAL}/request"

# ── Bambu Cloud MQTT config ────────────────────────────────────────────────────
CLOUD_HOST   = "us.mqtt.bambulab.com"   # or eu.mqtt.bambulab.com
CLOUD_PORT   = 8883
# ─────────────────────────────────────────────────────────────────────────────

PRINTING_STATES  = {"RUNNING", "PREPARE"}
RESUMABLE_STATES = {"PAUSE", "FAILED"}

# ── Bambu Cloud token cache ────────────────────────────────────────────────────
BAMBU_TOKENS_FILE  = os.path.join(os.path.expanduser("~"), ".bambu_tokens.json")
BAMBU_TOKEN_TTL    = 3600        # re-check file every hour
BAMBU_REFRESH_DAYS = 7           # try to refresh when < 7 days remain

_bambu_token    = None
_bambu_token_ts = 0.0

_printer_state   = None   # last known gcode_state string
_manual_override = False  # True = manual override mode active
_last_percent    = None   # last known print progress %
_last_remaining  = None   # last known remaining time in minutes
_last_remaining_update = None  # datetime when _last_remaining was last set
_last_layer      = None   # last known layer number
_total_layers    = None   # total layer count

# ── Print session tracking ────────────────────────────────────────────────────
_print_start_time   = None   # datetime when current print began
_last_filament_used = None   # grams reported by printer for current print
_nozzle_alert_sent     = False  # True after a nozzle-temp alert fires; reset on recovery
_hms_alert_sent        = False  # True after an HMS error alert fires; reset when hms list is empty
_nozzle_reached_target = False  # True once nozzle has actually reached its target this print
_nozzle_last_target    = None   # last known nozzle target; used to detect intentional target drops
_current_filename   = None   # subtask_name / gcode_file reported by printer
_subtask_name       = ""     # raw subtask_name field from MQTT
_ams_filament           = None   # current filament type + color from AMS/external spool
_ams_data               = None   # raw ams dict from last MQTT message
_ams_slots_snapshot     = {}     # {slot_key: (type, color_hex)} for change detection
_conversation_state     = None   # str: "waiting_weight_slot_X" / "waiting_price_slot_X" / None
_nozzle_temp        = None   # current nozzle temperature (°C)
_nozzle_target      = None   # nozzle target temperature (°C)
_bed_temp           = None   # current bed temperature (°C)
_bed_target         = None   # bed target temperature (°C)
_milestone_sent     = set()  # progress milestones (25/50/75) already notified this print
_ten_min_alert_sent = False  # True after the ~10-min warning fires; reset on new print
_active_slots_this_print: set = set()  # slot numbers (str "1"-"4") seen active during current print
_slot_active_seconds: dict    = {}     # {slot_str: float} accumulated active-feed seconds per slot this print
_current_slot_str:    str | None = None  # slot currently feeding filament (tray_now)
_current_slot_since:  float | None = None  # time.time() when _current_slot_str became active
_filament_used_snapshot  = 0.0        # last valid mc_weight seen during printing — fallback at finish
_current_task_id        = None        # task_id from MQTT — used to query Cloud API for planned weight
_cloud_slot_weights: dict = {}        # {project_ams_idx: grams} from Cloud API — project-time filament index!
_cloud_slot_types:  dict = {}        # {project_ams_idx: filament_type_str} e.g. {'3': 'PETG'}
_cloud_slot_colors: dict = {}        # {project_ams_idx: rrggbb_hex} for color-matching to physical slots
_cloud_weight_event = threading.Event()  # set() when Cloud API fetch completes (success or failure)
_slot_update_buffer: dict = {}        # temp data during spool replacement flow {slot_num: {type,color,...}}

# ── Thread synchronization ────────────────────────────────────────────────────
_filament_lock = threading.RLock()   # protects filament_data.json I/O (RLock so load→save migration is safe)
_state_lock    = threading.Lock()    # protects _conversation_state and _conversation_state_time
_globals_lock  = threading.Lock()    # protects shared print-session globals across threads
_conversation_state_time: float = 0.0  # epoch seconds when _conversation_state was last set

PROGRESS_MILESTONES = (25, 50, 75)

STATE_FILE   = os.path.join(os.path.expanduser("~"), ".bambu_monitor_state.json")
HISTORY_FILE = os.path.join(os.path.expanduser("~"), ".bambu_history.json")
FILAMENT_FILE          = os.path.join(os.path.expanduser("~"), "filament_data.json")
LOW_FILAMENT_THRESHOLD = 100   # grams — alert below this
DEFAULT_SPOOL_WEIGHT   = 1000  # grams
ANTHROPIC_API_KEY      = os.environ.get("ANTHROPIC_API_KEY", "")

# ── Telegram config ───────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = "YOUR_TELEGRAM_BOT_TOKEN"   # Get from @BotFather
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID"     # Your Telegram chat/user ID


# ── AMS helpers ───────────────────────────────────────────────────────────────

# RGB palette for closest-color matching (squared Euclidean distance)
_COLOR_PALETTE = [
    ("⚪", "לבן",              (255, 255, 255)),
    ("⚫", "שחור",             (0,   0,   0)),
    ("🩶", "אפור-בהיר",        (164, 170, 172)),
    ("🩶", "אפור",             (142, 144, 137)),
    ("🩶", "אפור-כסף",         (192, 192, 192)),
    ("🩶", "אפור-כחלחל",       (91,  101, 121)),
    ("🩶", "אפור-בינוני",      (127, 126, 131)),
    ("🩶", "אפור-כהה",         (84,  84,  84)),
    ("🔴", "אדום",             (214, 0,   28)),
    ("🔴", "אדום-כהה",         (157, 35,  53)),
    ("🔴", "אדום-בהיר",        (222, 67,  67)),
    ("🔴", "בורדו",            (187, 61,  67)),
    ("🔴", "טרקוטה",           (177, 85,  51)),
    ("🩷", "ורוד",             (245, 90,  116)),
    ("🩷", "ורוד-חם",          (245, 84,  124)),
    ("🩷", "ורוד-סקורה",       (232, 175, 207)),
    ("🩷", "מגנטה",            (236, 0,   140)),
    ("🟠", "כתום",             (255, 106, 19)),
    ("🟠", "כתום-דלעת",        (255, 144, 22)),
    ("🟠", "כתום-מנדרינה",     (249, 153, 99)),
    ("🟡", "צהוב",             (252, 227, 0)),
    ("🟡", "צהוב-לימון",       (247, 217, 89)),
    ("🟡", "צהוב-חמנייה",      (254, 198, 0)),
    ("🟡", "זהב",              (228, 189, 104)),
    ("🟢", "ירוק",             (0,   174, 66)),
    ("🟢", "ירוק-בהיר",        (190, 207, 0)),
    ("🟢", "ירוק-עשב",         (97,  198, 128)),
    ("🟢", "ירוק-תפוח",        (194, 225, 137)),
    ("🟢", "ירוק-כהה",         (104, 114, 77)),
    ("🟢", "זית",              (120, 157, 74)),
    ("🟢", "טורקיז",           (0,   177, 183)),
    ("🔵", "כחול",             (0,   133, 214)),
    ("🔵", "כחול-בהיר",        (86,  183, 230)),
    ("🔵", "תכלת",             (163, 216, 225)),
    ("🔵", "כחול-ים",          (0,   120, 191)),
    ("🔵", "כחול-ים-כהה",      (0,   86,  184)),
    ("🔵", "כחול-כהה",         (10,  41,  137)),
    ("🔵", "ציאן",             (0,   134, 214)),
    ("🟣", "סגול",             (94,  67,  183)),
    ("🟣", "סגול-כהה",         (72,  41,  96)),
    ("🟣", "לילך",             (174, 150, 212)),
    ("🟣", "שזיף",             (149, 0,   81)),
    ("🟤", "חום",              (157, 67,  44)),
    ("🟤", "חום-כהה",          (79,  44,  29)),
    ("🟤", "שוקולד",           (77,  51,  36)),
    ("🟤", "קפה",              (111, 80,  52)),
    ("🟤", "קרמל",             (174, 131, 91)),
    ("🟤", "בז׳",              (247, 230, 222)),
    ("🟤", "בז׳-חמאה",         (211, 183, 167)),
    ("🟤", "בז׳-עץ",           (232, 219, 183)),
    ("🟤", "ברונזה",           (132, 125, 72)),
]

# Map Hebrew color names (free-text user input) → display emoji
_COLOR_NAME_TO_EMOJI: dict = {
    "לבן":        "⚪",
    "שחור":       "⚫",
    "אפור":       "🩶",
    "כסוף":       "🩶",
    "אדום":       "🔴",
    "ורוד":       "🩷",
    "כתום":       "🟠",
    "צהוב":       "🟡",
    "זהב":        "🟡",
    "ירוק":       "🟢",
    "ירוק זוהר":  "💚",
    "כחול":       "🔵",
    "כחול זוהר":  "💙",
    "סגול":       "🟣",
    "חום":        "🟤",
    "בז׳":        "🟤",
    "צבע גוף":    "🟤",
    "ברונזה":     "🟫",
}


def _color_name_to_emoji(name: str) -> str:
    """Return emoji for a Hebrew color name, or empty string if not found."""
    return _COLOR_NAME_TO_EMOJI.get(name.strip(), "")


# Slot number display emojis (0-indexed: _SLOT_NUM_EMOJIS[slot_num - 1])
_SLOT_NUM_EMOJIS = ["1️⃣", "2️⃣", "3️⃣", "4️⃣"]


def _set_conversation_state(state: "str | None") -> None:
    """Thread-safe setter for _conversation_state. Always use this instead of direct assignment."""
    global _conversation_state, _conversation_state_time
    with _state_lock:
        _conversation_state = state
        _conversation_state_time = time.time() if state is not None else 0.0


def _hex_to_closest_color(hex_str: str) -> tuple:
    """Return (emoji, hebrew_name) for the closest palette color via RGB distance."""
    hex_str = hex_str.lstrip("#").upper()[:6]
    try:
        r = int(hex_str[0:2], 16)
        g = int(hex_str[2:4], 16)
        b = int(hex_str[4:6], 16)
    except Exception:
        return ("⚪", "לבן")
    best_emoji, best_name = "⚪", "לבן"
    best_dist = float("inf")
    for emoji, name, (pr, pg, pb) in _COLOR_PALETTE:
        dist = (r - pr) ** 2 + (g - pg) ** 2 + (b - pb) ** 2
        if dist < best_dist:
            best_dist = dist
            best_emoji, best_name = emoji, name
    return (best_emoji, best_name)


def _parse_ams_filament(print_data: dict) -> str | None:
    """Extract current filament type+color from AMS or external spool data."""
    try:
        ams_data = print_data.get("ams", {})
        if not ams_data:
            return None

        tray_now_str = str(ams_data.get("tray_now", "255"))
        # 255 = no AMS tray active (external spool or idle)
        if tray_now_str == "255":
            # Try external (virtual) tray
            vt = print_data.get("vt_tray", {})
            if vt:
                ftype = (vt.get("tray_type") or vt.get("tray_sub_brands") or "").strip()
                _, color_name = _hex_to_closest_color(vt.get("tray_color", ""))
                return f"{ftype} - {color_name}" if ftype else None
            return None

        tray_now = int(tray_now_str)
        ams_units = ams_data.get("ams", [])
        ams_idx   = tray_now // 4
        tray_idx  = tray_now % 4
        if ams_idx >= len(ams_units):
            return None
        trays = ams_units[ams_idx].get("tray", [])
        tray  = next((t for t in trays if str(t.get("id")) == str(tray_idx)), None)
        if not tray:
            return None
        ftype = (tray.get("tray_type") or tray.get("tray_sub_brands") or "").strip()
        _, color_name = _hex_to_closest_color(tray.get("tray_color", ""))
        return f"{ftype} - {color_name}" if ftype else None
    except Exception:
        return None


def _get_active_slot_num() -> str | None:
    """Return the 1-based slot number (as string) for the currently active AMS tray, or None if unknown."""
    if _ams_data:
        tray_now_str = str(_ams_data.get("tray_now", "255"))
        if tray_now_str != "255":
            try:
                return str(int(tray_now_str) + 1)
            except (ValueError, TypeError):
                pass
    # Fallback: use the last known active slot (updated in real-time by tray_now tracking)
    if _current_slot_str is not None:
        try:
            return str(int(_current_slot_str) + 1)  # 0-based → 1-based
        except (ValueError, TypeError):
            pass
    return None  # genuinely unknown — do not assume slot 1


def _slot_weight_keyboard(slot_num: int) -> dict:
    """Weight selection keyboard for a specific slot (1-based)."""
    return {
        "inline_keyboard": [[
            {"text": "250ג",  "callback_data": f"sw_{slot_num}_250"},
            {"text": "500ג",  "callback_data": f"sw_{slot_num}_500"},
            {"text": "1000ג", "callback_data": f"sw_{slot_num}_1000"},
            {"text": "✏️ הזן ידנית", "callback_data": f"sw_{slot_num}_manual"},
        ]]
    }


def _slot_confirm_keyboard(slot_num: str) -> dict:
    """Yes/No confirmation for spool replacement."""
    return {
        "inline_keyboard": [[
            {"text": "✅ כן, החלפתי", "callback_data": f"confirm_yes_{slot_num}"},
            {"text": "❌ לא",          "callback_data": f"confirm_no_{slot_num}"},
        ]]
    }


def _slot_type_keyboard(slot_num: str) -> dict:
    """Filament type selection buttons."""
    return {
        "inline_keyboard": [[
            {"text": "PLA",  "callback_data": f"stype_{slot_num}_PLA"},
            {"text": "PETG", "callback_data": f"stype_{slot_num}_PETG"},
            {"text": "TPU",  "callback_data": f"stype_{slot_num}_TPU"},
            {"text": "ABS",  "callback_data": f"stype_{slot_num}_ABS"},
        ]]
    }


def _slot_select_keyboard() -> dict:
    """Slot selection for manual spool replacement."""
    return {
        "inline_keyboard": [[
            {"text": "1️⃣", "callback_data": "replace_slot_1"},
            {"text": "2️⃣", "callback_data": "replace_slot_2"},
            {"text": "3️⃣", "callback_data": "replace_slot_3"},
            {"text": "4️⃣", "callback_data": "replace_slot_4"},
        ]]
    }


def _filament_setup_keyboard() -> dict:
    """Slot setup buttons + manual replacement button."""
    return {
        "inline_keyboard": [
            [
                {"text": "⚙️ 1️⃣", "callback_data": "setup_slot_1"},
                {"text": "⚙️ 2️⃣", "callback_data": "setup_slot_2"},
                {"text": "⚙️ 3️⃣", "callback_data": "setup_slot_3"},
                {"text": "⚙️ 4️⃣", "callback_data": "setup_slot_4"},
            ],
            [{"text": "🔄 החלפתי גליל", "callback_data": "manual_replace"}],
        ]
    }


# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ── Telegram send ─────────────────────────────────────────────────────────────

# Persistent reply keyboard shown at the bottom of the chat
REPLY_KEYBOARD = {
    "keyboard": [
        ["📊 סטטוס",    "🧵 פילמנט"],
        ["❓ עזרה"],
    ],
    "resize_keyboard": True,
    "persistent": True,
}


def _tg_post(endpoint: str, payload: dict) -> None:
    url  = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/{endpoint}"
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(url, data=data,
                                  headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=10)


def send_telegram(message: str, reply_markup=None) -> None:
    """Send a plain notification (no menu).  Used for async printer alerts.
    Retries once after 5 seconds if the first attempt fails."""
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        _tg_post("sendMessage", payload)
        log(f"Telegram sent: {message}")
    except Exception as exc:
        log(f"Telegram error (attempt 1): {exc} — retrying in 5s")
        time.sleep(5)
        try:
            _tg_post("sendMessage", payload)
            log(f"Telegram sent (retry ok): {message}")
        except Exception as exc2:
            log(f"Telegram error (attempt 2, giving up): {exc2}")


def send_menu() -> None:
    """Send the persistent reply keyboard (called once at bot startup)."""
    send_telegram("🤖 בוט מחובר", reply_markup=REPLY_KEYBOARD)


def answer_callback_query(callback_id: str) -> None:
    """Dismiss the loading spinner on the pressed button."""
    try:
        _tg_post("answerCallbackQuery", {"callback_query_id": callback_id})
    except Exception as exc:
        log(f"answerCallbackQuery error: {exc}")


def send_camera_snapshot(silent: bool = False) -> None:
    """Stub — implement your own camera integration here."""
    log("send_camera_snapshot called (not implemented in public version)")
    if not silent:
        send_telegram("📷 מצלמה לא מוגדרת")


# ── Print history ─────────────────────────────────────────────────────────────

def history_load() -> list:
    try:
        with open(HISTORY_FILE) as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return []


def history_append(entry: dict) -> None:
    records = history_load()
    records.append(entry)
    try:
        with open(HISTORY_FILE, "w") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
    except OSError as exc:
        log(f"History write error: {exc}")


def fetch_weight_from_cloud(task_id: str) -> tuple:
    """Query Bambu Cloud API for planned filament weight.
    Returns (total_g, {slot_num: grams}, {slot_num: filament_type_str}) or (0.0, {}, {}) on failure.

    NOTE on amsDetailMapping slot numbers:
      The Cloud API's amsDetailMapping uses the *project extruder index* (amsId/slotId from
      Bambu Studio at slice time), NOT the physical AMS tray index seen in MQTT tray_now.
      When the user rearranges filaments in the AMS, Bambu auto-remaps at print time but the
      Cloud API still reports the original project assignment.  The returned slot_weights dict
      therefore uses project-based keys and must be remapped to physical slots before use.
    """
    try:
        token = bambu_login()
        url = "https://api.bambulab.com/v1/user-service/my/tasks?limit=10"
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {token}",
            "User-Agent": "bambu_network_agent/01.09.05.01",
        })
        resp = json.loads(urllib.request.urlopen(req, timeout=10).read())
        for hit in resp.get("hits", []):
            if str(hit.get("id")) == str(task_id):
                total = float(hit.get("weight") or 0.0)
                # raw_mappings: each entry has fields including:
                #   ams          — 1-based *project* filament/extruder index (unique per entry)
                #   amsId        — AMS unit index (often 0 for all entries on Bambu Lab printer — NOT unique!)
                #   slotId       — slot index within AMS unit (often 0 for all entries — NOT unique!)
                #   filamentType — e.g. "PLA", "PETG"
                #   sourceColor  — RRGGBBAA hex (8 chars); strip alpha to get RRGGBB for AMS matching
                #   weight       — planned grams for this filament
                #
                # CRITICAL: amsId + slotId are NOT reliable keys — multiple entries can share the
                # same (amsId=0, slotId=0), causing the old formula to overwrite previous weights.
                # Use the `ams` field (project filament index) as the unique key instead.
                raw_mappings = hit.get("amsDetailMapping", [])
                log(f"[Cloud API] raw amsDetailMapping: {raw_mappings}")
                slot_weights = {}   # {project_ams_idx_str: grams}
                slot_types   = {}   # {project_ams_idx_str: filament_type_str}
                slot_colors  = {}   # {project_ams_idx_str: rrggbb_hex}  — for physical-slot matching
                for m in raw_mappings:
                    # Use `ams` (project filament index, 1-based) as the unique slot key
                    proj_idx = m.get("ams")
                    if proj_idx is None:
                        # Fallback to old formula if `ams` field absent (future API version change)
                        ams_id  = int(m.get("amsId", 0))
                        slot_id = int(m.get("slotId", 0))
                        proj_idx = ams_id * 4 + slot_id + 1
                    sn = str(proj_idx)
                    w  = float(m.get("weight") or 0.0)
                    ftype = (
                        m.get("filamentType") or
                        m.get("type") or
                        m.get("tray_type") or
                        ""
                    ).strip().upper()
                    # sourceColor is RRGGBBAA (8 chars) — strip alpha to compare with MQTT tray_color (6 chars)
                    raw_color = (m.get("sourceColor") or "").strip().upper()
                    color6 = raw_color[:6] if len(raw_color) >= 6 else raw_color
                    if w > 0:
                        slot_weights[sn] = w
                    if ftype:
                        slot_types[sn] = ftype
                    if color6:
                        slot_colors[sn] = color6
                log(f"[Cloud API] task={task_id} weight={total}g slots={slot_weights} "
                    f"types={slot_types} colors={slot_colors}")
                return (total, slot_weights, slot_types, slot_colors)
        log(f"[Cloud API] task {task_id} not found in last 10 tasks")
        return (0.0, {}, {}, {})
    except Exception as e:
        log(f"[Cloud API] Error fetching weight: {e}")
        return (0.0, {}, {}, {})


def _fetch_cloud_weight_async() -> None:
    """Background thread: fetch planned weight from Cloud API and store in snapshot."""
    global _filament_used_snapshot, _last_filament_used, _cloud_slot_weights, _cloud_slot_types, _cloud_slot_colors
    try:
        if not _current_task_id:
            return
        total, slot_weights, slot_types, slot_colors = fetch_weight_from_cloud(_current_task_id)
        if total > 0:
            with _globals_lock:
                _filament_used_snapshot = total
                _last_filament_used     = total
                _cloud_slot_weights     = slot_weights
                _cloud_slot_types       = slot_types
                _cloud_slot_colors      = slot_colors
            log(f"[Cloud API] Snapshot set: {total}g")
    finally:
        _cloud_weight_event.set()



def _wait_for_weight_then_start() -> None:
    """Wait up to 15 s for Cloud API fetch to complete, then send the print start message."""
    _cloud_weight_event.wait(timeout=15)
    for _ in range(18):
        with _globals_lock:
            r = _last_remaining
        if r and r > 0:
            break
        time.sleep(10)
    _send_print_start_message()


def _send_eta_update() -> None:
    """Wait up to 3 minutes for mc_remaining_time, then send ETA update."""
    for _ in range(18):
        time.sleep(10)
        with _globals_lock:
            r = _last_remaining
        if r and r > 0:
            with _globals_lock:
                start = _print_start_time
            if start:
                eta = start + timedelta(minutes=r)
                eta_str = eta.strftime('%H:%M')
            else:
                eta = datetime.now() + timedelta(minutes=r)
                eta_str = eta.strftime('%H:%M')
            send_telegram(
                f"⏱ זמן משוער: {format_time(r)}\n🏁 סיום משוער: {eta_str}"
            )
            return


def _send_print_start_message() -> None:
    """Send the print start notification, including Cloud API weight if available."""
    d_fil    = filament_load()
    slot_num = _get_active_slot_num()
    slot     = d_fil["slots"].get(slot_num)
    lines    = ["🖨️ הדפסה התחילה!", "─" * 17]
    if _subtask_name:
        lines.append(f"📄 קובץ: {_subtask_name}")
    if _print_start_time:
        lines.append(f"🕐 התחלה: {_print_start_time.strftime('%H:%M')}")
    if _last_remaining and _last_remaining > 0:
        lines.append(f"⏱ זמן משוער: {format_time(_last_remaining)}")
        if _print_start_time:
            eta = _print_start_time + timedelta(minutes=_last_remaining)
            lines.append(f"🏁 סיום משוער: {eta.strftime('%H:%M')}")

    # Show per-slot weights from Cloud API if available.
    # IMPORTANT: _cloud_slot_weights uses *project extruder indices* from amsDetailMapping,
    # not physical AMS slot IDs.  We must NOT display them directly against filament_data slot
    # names (that would show the wrong filament for the weight).
    # At start time we don't yet know the active slots, so we show the Cloud total as a single
    # estimate line and defer the per-slot breakdown to the finish message (where remap runs).
    if _cloud_slot_weights:
        # Only show if total weight is meaningful
        if _filament_used_snapshot > 0:
            line = f"⚖️ פילמנט משוער (Cloud): ~{_filament_used_snapshot:.0f}ג"
            lines.append(line)
        # Additionally show the currently-active slot if known
        if slot:
            ft = slot.get("type", "PLA")
            cn = slot.get("color_name", "")
            ce = slot.get("color_emoji", "")
            sd = _SLOT_NUM_EMOJIS[int(slot_num) - 1] if slot_num and slot_num.isdigit() and 1 <= int(slot_num) <= 4 else slot_num
            lines.append(f"🧵 פילמנט נוכחי: {ft} {cn} {ce} (סלוט {sd})")
    elif slot:
        ft = slot.get("type", "PLA")
        cn = slot.get("color_name", "")
        ce = slot.get("color_emoji", "")
        sd = _SLOT_NUM_EMOJIS[int(slot_num) - 1] if slot_num and slot_num.isdigit() and 1 <= int(slot_num) <= 4 else slot_num
        line = f"🧵 פילמנט: {ft} {cn} {ce} (סלוט {sd})"
        if _filament_used_snapshot > 0:
            line += f" | ~{_filament_used_snapshot:.0f}ג"
        lines.append(line)

    # Cost estimate and low-filament warning
    # At start time, use the total snapshot as a rough estimate against the active slot
    total_cost = 0.0
    low_filament_warnings = []
    if _filament_used_snapshot > 0 and slot:
        price = slot.get("price_per_kg", 0) or 0
        if price > 0:
            total_cost = _filament_used_snapshot * price / 1000.0
        remaining = slot.get("remaining_g", 0) or 0
        if _filament_used_snapshot > remaining:
            low_filament_warnings.append((remaining, _filament_used_snapshot))
    elif slot and _filament_used_snapshot > 0:
        price = slot.get("price_per_kg", 0) or 0
        if price > 0:
            total_cost = _filament_used_snapshot * price / 1000.0
        remaining = slot.get("remaining_g", 0) or 0
        if _filament_used_snapshot > remaining:
            low_filament_warnings.append((remaining, _filament_used_snapshot))

    if total_cost > 0:
        lines.append(f"💰 עלות משוערת: ~{total_cost:.2f}₪")
    for remaining, needed in low_filament_warnings:
        lines.append(f"⚠️ ייתכן שהגליל לא יספיק! נשאר ~{remaining:.0f}ג, נדרש ~{needed:.0f}ג")

    send_telegram("\n".join(lines))

    if not (_last_remaining and _last_remaining > 0):
        threading.Thread(target=_send_eta_update, daemon=True).start()


def _wait_and_finish(result: str, end_time: datetime) -> None:
    """Wait up to 30 s for mc_weight to arrive, then finalize the print."""
    global _last_filament_used
    deadline = time.time() + 30
    while time.time() < deadline:
        with _globals_lock:
            val = _last_filament_used
        if val is not None and val > 0:
            break
        time.sleep(1)
    # Capture the value now — insulated from concurrent reset if a new print starts
    with _globals_lock:
        if _last_filament_used is None or _last_filament_used <= 0:
            _last_filament_used = _filament_used_snapshot if _filament_used_snapshot > 0 else None
    _finish_print(result, end_time)


def _finish_print(result: str, end_time: datetime = None) -> None:
    """Called when a print ends. Records the session to history and sends summary."""
    global _print_start_time, _last_filament_used, _current_filename
    if _print_start_time is None:
        return

    # Snapshot all shared globals under lock — a new print may have reset them by now
    with _globals_lock:
        used_grams              = _last_filament_used
        snap_grams              = _filament_used_snapshot
        active_slots_s          = set(_active_slots_this_print)  # copy
        cloud_slot_weights_raw  = dict(_cloud_slot_weights)   # project-index keys, may not match active slots
        cloud_slot_types_raw    = dict(_cloud_slot_types)
        # Finalize time for whichever slot was active when print ended
        slot_seconds = dict(_slot_active_seconds)
        if _current_slot_str is not None and _current_slot_since is not None:
            delta = time.time() - _current_slot_since
            slot_seconds[_current_slot_str] = slot_seconds.get(_current_slot_str, 0.0) + delta

    if not used_grams or used_grams <= 0:
        used_grams = snap_grams if snap_grams > 0 else None
    log(f"[Finish] using grams={used_grams} (snapshot={snap_grams})")

    if end_time is None:
        end_time = datetime.now()
    duration_min = int((end_time - _print_start_time).total_seconds() / 60)

    entry = {
        "start":            _print_start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "end":              end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_minutes": duration_min,
        "result":           result,
        "filament_g":       used_grams,
        "filename":         _current_filename,
    }
    history_append(entry)
    log(f"Print recorded: {result}, {duration_min} min, filament={used_grams}g, file={_current_filename}")

    # ── Per-slot weight attribution via tray_now time tracking ────────────────
    # Primary method: proportion of time each slot was active during the print.
    # Fallback: equal split across all active slots.
    # Cloud amsDetailMapping is logged for reference only (unreliable for multi-slot).
    active_slots   = sorted(active_slots_s) or [_get_active_slot_num()]
    grams_per_slot = (used_grams / len(active_slots)) if (used_grams and used_grams > 0) else None

    total_time = sum(slot_seconds.get(sn, 0.0) for sn in active_slots)
    if used_grams and used_grams > 0 and total_time > 0:
        slot_weights = {sn: (slot_seconds.get(sn, 0.0) / total_time) * used_grams
                        for sn in active_slots}
        method = "time-proportional"
    else:
        slot_weights = {sn: grams_per_slot for sn in active_slots} if grams_per_slot else {}
        method = "equal-split"

    log(f"  [Slots] active={active_slots}, total_g={used_grams}, method={method}, "
        f"slot_seconds={slot_seconds}, slot_weights={slot_weights}")
    if cloud_slot_weights_raw:
        log(f"  [Slots] cloud_raw={cloud_slot_weights_raw} (for reference only)")

    # Deduct used grams from each active slot
    if used_grams and used_grams > 0:
        d_fil = filament_load()
        for sn in active_slots:
            slot = d_fil["slots"].get(sn)
            if slot and slot.get("remaining_g") is not None:
                deduct = slot_weights.get(sn)
                if deduct:
                    slot["remaining_g"] = max(0.0, slot["remaining_g"] - deduct)
        filament_save(d_fil)

    # Full summary message — FINISH only
    if result == "FINISH":
        d_fil  = filament_load()
        single = (len(active_slots) == 1)
        lines  = ["✅ הדפסה הסתיימה!", "─" * 17]
        if _subtask_name:
            lines.append(f"📄 קובץ: {_subtask_name}")
        lines.append(f"⏱ זמן בפועל: {format_time(duration_min)}")

        if used_grams and used_grams > 0:
            total_cost   = 0.0
            has_cost     = False
            low_alerts   = []
            for sn in active_slots:
                sd   = _SLOT_NUM_EMOJIS[int(sn) - 1] if sn.isdigit() and 1 <= int(sn) <= 4 else sn
                slot = d_fil["slots"].get(sn)
                if not slot:
                    continue
                ft    = slot.get("type", "?")
                cn    = slot.get("color_name", "?")
                ce    = slot.get("color_emoji", "")
                rem   = slot.get("remaining_g")
                pkk   = slot.get("price_per_kg")
                slot_g = slot_weights.get(sn)

                if slot_g:
                    slot_line = f"🧵 סלוט {sd}: {ft} {cn} {ce} | נוצל ~{slot_g:.0f}ג"
                else:
                    slot_line = f"🧵 סלוט {sd}: {ft} {cn} {ce}"
                if rem is not None:
                    slot_line += f" | נשאר ~{rem:.0f}ג"

                if not single and pkk and slot_g:
                    cost = (slot_g / 1000) * pkk
                    total_cost += cost
                    has_cost = True
                    slot_line += f" | ~{cost:.1f}₪"
                lines.append(slot_line)

                if single and pkk and slot_g:
                    cost = (slot_g / 1000) * pkk
                    total_cost += cost
                    has_cost = True
                    lines.append(f"💰 עלות: ~{cost:.1f}₪")

                if rem is not None and rem < LOW_FILAMENT_THRESHOLD:
                    low_alerts.append(
                        f"⚠️ סלוט {sd}: נשאר רק ~{rem:.0f}ג ({ft} {cn} {ce})\n"
                        f"כדאי להחליף גליל בקרוב!"
                    )

            if not single:
                total_line = f"⚖️ סה״כ: ~{used_grams:.0f}ג"
                if has_cost:
                    total_line += f" | 💰 ~{total_cost:.1f}₪"
                lines.append(total_line)

            send_telegram("\n".join(lines))
            for alert in low_alerts:
                send_telegram(alert)
        else:
            send_telegram("\n".join(lines))

    _print_start_time = None
    with _globals_lock:
        _last_filament_used = None
    _current_filename = None


# ── Statistics ────────────────────────────────────────────────────────────────

def compute_stats() -> dict:
    records = history_load()
    now     = datetime.now()
    week_ago  = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)

    week_total = week_ok = week_minutes = 0
    month_total = month_ok = month_minutes = 0

    for r in records:
        try:
            start = datetime.strptime(r["start"], "%Y-%m-%d %H:%M:%S")
        except (KeyError, ValueError):
            continue
        dur = r.get("duration_minutes", 0) or 0
        ok  = r.get("result") == "FINISH"

        if start >= month_ago:
            month_total   += 1
            month_minutes += dur
            if ok:
                month_ok += 1

        if start >= week_ago:
            week_total   += 1
            week_minutes += dur
            if ok:
                week_ok += 1

    return {
        "week_total":   week_total,
        "week_ok":      week_ok,
        "week_hours":   week_minutes / 60,
        "month_total":  month_total,
        "month_ok":     month_ok,
        "month_hours":  month_minutes / 60,
    }


# ── Filament tracking ─────────────────────────────────────────────────────────

_FILAMENT_TYPES = ("PLA", "PETG", "TPU", "ABS")

_FILAMENT_DEFAULT: dict = {
    "slots": {"1": None, "2": None, "3": None, "4": None},
}

# New slot schema:
# {"type": "PLA", "color_hex": "FFFFFF", "color_name": "לבן", "color_emoji": "⚪",
#  "weight_g": 1000, "remaining_g": 1000, "price_per_kg": 65}


def filament_load() -> dict:
    import copy
    with _filament_lock:
        try:
            with open(FILAMENT_FILE) as f:
                d = json.load(f)
        except (OSError, json.JSONDecodeError):
            return copy.deepcopy(_FILAMENT_DEFAULT)

        # Migrate: old single-spool format
        if "spool_weight_g" in d:
            return copy.deepcopy(_FILAMENT_DEFAULT)

        # Migrate: old "0_0" slot keys
        if "slots" in d and any("_" in k for k in d.get("slots", {})):
            new_slots: dict = {"1": None, "2": None, "3": None, "4": None}
            for k, v in d["slots"].items():
                if "_" in k and v:
                    parts = k.split("_")
                    try:
                        sn = str(int(parts[0]) * 4 + int(parts[1]) + 1)
                        if sn in new_slots:
                            rem = max(0.0, v.get("spool_weight_g", DEFAULT_SPOOL_WEIGHT) - v.get("used_g", 0.0))
                            new_slots[sn] = {
                                "type": "PLA", "color_hex": "", "color_name": "?", "color_emoji": "⬜",
                                "weight_g": None, "remaining_g": rem, "price_per_kg": None,
                            }
                    except (ValueError, IndexError):
                        pass
            d = {"slots": new_slots}
            filament_save(d)  # RLock allows re-entry from same thread

        # Migrate: old format with top-level "prices" dict
        if "prices" in d:
            prices = d.pop("prices", {})
            changed = False
            for sn, slot in (d.get("slots") or {}).items():
                if slot and "price_per_kg" not in slot:
                    ftype = slot.get("type", "PLA")
                    p = prices.get(ftype) or None
                    slot["price_per_kg"] = p if p else None
                    changed = True
                if slot and "weight_g" not in slot:
                    slot["weight_g"] = slot.get("remaining_g")
                    changed = True
                if slot and "color_emoji" not in slot:
                    emoji, _ = _hex_to_closest_color(slot.get("color_hex", ""))
                    slot["color_emoji"] = emoji
                    changed = True
            if changed:
                filament_save(d)  # RLock allows re-entry from same thread

        # Ensure structure
        if "slots" not in d:
            d["slots"] = {"1": None, "2": None, "3": None, "4": None}
        for k in ("1", "2", "3", "4"):
            if k not in d["slots"]:
                d["slots"][k] = None
        return d


def filament_save(data: dict) -> None:
    data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with _filament_lock:
        try:
            with open(FILAMENT_FILE, "w") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except OSError as exc:
            log(f"Filament save error: {exc}")


def sync_ams_to_filament_data() -> None:
    """Sync AMS slot type/color from live _ams_data into filament_data.json."""
    if not _ams_data:
        return
    d = filament_load()
    changed = False
    for unit in _ams_data.get("ams", []):
        ams_idx = int(unit.get("id", 0))
        for tray in unit.get("tray", []):
            tray_idx  = int(tray.get("id", 0))
            slot_num  = str(ams_idx * 4 + tray_idx + 1)
            ftype     = (tray.get("tray_type") or tray.get("tray_sub_brands") or "").strip()
            color_hex = tray.get("tray_color", "")[:6]
            if not ftype:
                if d["slots"].get(slot_num) is not None:
                    d["slots"][slot_num] = None
                    changed = True
                continue
            emoji, color_name = _hex_to_closest_color(color_hex)
            current      = d["slots"].get(slot_num)
            type_changed  = current is None or current.get("type") != ftype
            color_changed = current is None or current.get("color_hex") != color_hex
            name_mismatch = current is not None and (current.get("color_name") != color_name or current.get("color_emoji") != emoji)
            if type_changed or color_changed or name_mismatch:
                if current is None:
                    d["slots"][slot_num] = {
                        "type": ftype, "color_hex": color_hex, "color_name": color_name,
                        "color_emoji": emoji,
                        "weight_g": None, "remaining_g": None, "price_per_kg": None,
                    }
                else:
                    current["type"]      = ftype
                    current["color_hex"] = color_hex
                    current["color_name"]  = color_name
                    current["color_emoji"] = emoji
                    # weight_g / remaining_g / price_per_kg are NOT touched — user data preserved
                changed = True
    if changed:
        filament_save(d)


def filament_set_slot_weight(slot_num: str, weight_g: float) -> None:
    """Set the weight and remaining for a slot (spool replacement)."""
    d    = filament_load()
    slot = d["slots"].get(slot_num)
    if slot is None:
        d["slots"][slot_num] = {
            "type": "?", "color_hex": "", "color_name": "?", "color_emoji": "⬜",
            "weight_g": weight_g, "remaining_g": weight_g, "price_per_kg": None,
        }
    else:
        slot["weight_g"]    = weight_g
        slot["remaining_g"] = weight_g
    filament_save(d)
    log(f"Slot {slot_num} weight set to {weight_g}g")


def filament_status_text() -> str:
    """Return formatted filament status for all AMS slots."""
    sync_ams_to_filament_data()
    d     = filament_load()
    lines = ["🧵 מצב גלילים", "─" * 17]

    tray_now_str = str(_ams_data.get("tray_now", "255")) if _ams_data else "255"
    for i, slot_emoji in enumerate(_SLOT_NUM_EMOJIS, 1):
        slot_num    = str(i)
        slot        = d["slots"].get(slot_num)
        active_mark = " ◀" if str(i - 1) == tray_now_str else ""
        if slot is None:
            lines.append(f"{slot_emoji} ריק")
        else:
            ftype        = slot.get("type", "?")
            color_name   = slot.get("color_name", "?")
            color_emoji  = slot.get("color_emoji") or _hex_to_closest_color(slot.get("color_hex", ""))[0]
            remaining    = slot.get("remaining_g")
            price_per_kg = slot.get("price_per_kg")
            rem_str      = f"~{remaining:.0f}ג" if remaining is not None else "לא הוגדר"
            price_str    = f" | {price_per_kg:g}₪/ק״ג" if price_per_kg else ""
            lines.append(
                f"{slot_emoji} {ftype} {color_name} {color_emoji} | {rem_str}{price_str}{active_mark}"
            )

    return "\n".join(lines)


# ── State persistence ─────────────────────────────────────────────────────────

def write_state() -> None:
    printer_label = _printer_state or "unknown"
    data = {
        "printer_state":     printer_label,
        "updated":           datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "manual_override":   _manual_override,
        "percent":           _last_percent,
        "remaining_minutes": _last_remaining,
        "print_start":       _print_start_time.strftime("%Y-%m-%d %H:%M:%S") if _print_start_time else None,
        "filename":          _current_filename,
        "nozzle_temp":       _nozzle_temp,
        "nozzle_target":     _nozzle_target,
        "bed_temp":          _bed_temp,
        "bed_target":        _bed_target,
        "milestone_sent":    sorted(_milestone_sent),
        "last_remaining_update": _last_remaining_update.strftime("%Y-%m-%dT%H:%M:%S") if _last_remaining_update else None,
        "ams_filament":      _ams_filament,
    }
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(data, f)
    except OSError:
        pass


def load_state_from_file() -> None:
    global _printer_state, _last_percent, _last_remaining
    global _print_start_time, _manual_override, _current_filename
    global _milestone_sent
    global _last_remaining_update
    try:
        with open(STATE_FILE) as f:
            data = json.load(f)

        ps = data.get("printer_state")
        _printer_state  = ps if ps and ps != "unknown" else None

        _last_percent    = data.get("percent")
        _last_remaining  = data.get("remaining_minutes")
        _manual_override = bool(data.get("manual_override", False))
        _current_filename = data.get("filename")
        _nozzle_temp      = data.get("nozzle_temp")
        _nozzle_target    = data.get("nozzle_target")
        _bed_temp         = data.get("bed_temp")
        _bed_target       = data.get("bed_target")
        _ams_filament     = data.get("ams_filament")

        saved_milestones = data.get("milestone_sent", [])
        try:
            _milestone_sent = set(int(m) for m in saved_milestones)
        except (TypeError, ValueError):
            _milestone_sent = set()

        raw = data.get("last_remaining_update")
        _last_remaining_update = datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S") if raw else None

        saved_start = data.get("print_start")
        if saved_start and _printer_state in PRINTING_STATES:
            try:
                _print_start_time = datetime.strptime(saved_start, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass

        log(f"State restored from file: printer={_printer_state}, plug={plug_str}, "
            f"percent={_last_percent}, remaining={_last_remaining}, file={_current_filename}")
    except (OSError, json.JSONDecodeError):
        log("No state file found — starting fresh.")


def sync_override() -> bool:
    global _manual_override
    try:
        with open(STATE_FILE) as f:
            data = json.load(f)
        _manual_override = bool(data.get("manual_override", False))
    except (OSError, json.JSONDecodeError):
        pass
    return _manual_override


# ── Bambu Cloud authentication ────────────────────────────────────────────────

def bambu_login() -> str:
    global _bambu_token, _bambu_token_ts

    if _bambu_token and (time.time() - _bambu_token_ts) < BAMBU_TOKEN_TTL:
        return _bambu_token

    try:
        with open(BAMBU_TOKENS_FILE) as f:
            saved = json.load(f)
    except (OSError, json.JSONDecodeError):
        saved = {}

    access_token  = saved.get("accessToken", "")
    refresh_token = saved.get("refreshToken", "")
    saved_at      = float(saved.get("saved_at", 0))
    expires_in    = float(saved.get("expiresIn", 0))
    time_left     = (saved_at + expires_in) - time.time() if saved_at and expires_in else 0

    if access_token and 0 < time_left < BAMBU_REFRESH_DAYS * 86400:
        log(f"Bambu token expiring in {time_left / 86400:.1f} days — refreshing…")
        try:
            url  = "https://api.bambulab.com/v1/user-service/user/refreshtoken"
            body = json.dumps({"refreshToken": refresh_token}).encode()
            req  = urllib.request.Request(url, data=body, headers={
                "Content-Type": "application/json",
                "User-Agent": "bambu_network_agent/01.09.05.01",
            })
            resp     = json.loads(urllib.request.urlopen(req, timeout=15).read())
            new_tok  = resp.get("accessToken") or resp.get("token")
            if new_tok:
                saved.update({
                    "accessToken":  new_tok,
                    "refreshToken": resp.get("refreshToken", refresh_token),
                    "expiresIn":    resp.get("expiresIn", expires_in),
                    "saved_at":     time.time(),
                })
                with open(BAMBU_TOKENS_FILE, "w") as f:
                    json.dump(saved, f, indent=2)
                access_token = new_tok
                log("Bambu token refreshed OK.")
        except Exception as exc:
            log(f"Bambu token refresh failed: {exc}")

    if not access_token:
        raise RuntimeError(f"No Bambu token found. Run: python3 ~/bambu_auth.py")
    if saved_at and expires_in and time.time() > saved_at + expires_in:
        raise RuntimeError(f"Bambu token expired. Run: python3 ~/bambu_auth.py")

    _bambu_token    = access_token
    _bambu_token_ts = time.time()
    log("Bambu Cloud token loaded.")
    return _bambu_token



# ── MQTT callbacks ────────────────────────────────────────────────────────────

def _rc_value(reason_code) -> int:
    if isinstance(reason_code, int):
        return reason_code
    return getattr(reason_code, "value", 0)


def on_connect(client, userdata, *args):
    reason_code = args[1] if len(args) >= 2 else args[0]
    rc = _rc_value(reason_code)
    if rc == 0:
        log(f"Connected to Bambu Cloud MQTT ({CLOUD_HOST}:{CLOUD_PORT}).")
        client.subscribe(TOPIC)
        log(f"Subscribed to {TOPIC}")
        # Request a full status dump so we get AMS/filament data immediately
        pushall = json.dumps({"pushing": {"sequence_id": "0", "command": "pushall"}})
        client.publish(TOPIC_REQ, pushall)
        log("Sent pushall request to printer.")
    else:
        log(f"MQTT connection failed (rc={rc}).")


def on_disconnect(client, userdata, *args):
    reason_code = args[1] if len(args) >= 2 else args[0]
    rc = _rc_value(reason_code)
    log(f"Disconnected from MQTT broker (rc={rc}). Reconnecting…")


def format_time(minutes: int) -> str:
    if minutes <= 0:
        return "0m"
    h, m = divmod(minutes, 60)
    return f"{h}h {m:02d}m" if h else f"{m}m"


def on_message(client, userdata, msg):
    global _printer_state, _last_percent, _last_remaining
    global _last_layer, _total_layers
    global _print_start_time, _last_filament_used, _nozzle_alert_sent, _current_filename
    global _nozzle_temp, _nozzle_target, _bed_temp, _bed_target
    global _nozzle_reached_target, _nozzle_last_target, _ams_data, _ams_slots_snapshot
    global _conversation_state, _active_slots_this_print, _filament_used_snapshot
    global _slot_active_seconds, _current_slot_str, _current_slot_since
    global _current_task_id, _cloud_slot_weights, _cloud_slot_types, _ams_filament
    global _hms_alert_sent, _subtask_name, _ten_min_alert_sent, _slot_update_buffer

    try:
        payload = json.loads(msg.payload.decode())
    except (json.JSONDecodeError, UnicodeDecodeError):
        return

    print_data = payload.get("print", {})

    # Capture task_id whenever it appears — may arrive in PREPARE or RUNNING (service restart mid-print)
    task_id = print_data.get("task_id")
    if task_id and str(task_id) != _current_task_id:
        _current_task_id = str(task_id)
        _cloud_slot_weights = {}
        _cloud_slot_types   = {}
        _cloud_weight_event.clear()
        _filament_used_snapshot = 0.0
        threading.Thread(target=_fetch_cloud_weight_async, daemon=True).start()

    # ── Always capture AMS data immediately, even before state is known ───────
    ams_raw = print_data.get("ams")
    if ams_raw:
        _ams_data = ams_raw
        tray_now  = ams_raw.get("tray_now", "?")
        slot_parts = []
        for unit in ams_raw.get("ams", []):
            for tray in unit.get("tray", []):
                ftype = (tray.get("tray_type") or "").strip()
                color = (tray.get("tray_color") or "")[:6]
                slot_parts.append(f"id{tray.get('id')}:{ftype}/{color}")
        log(f"[AMS] tray_now={tray_now} | {', '.join(slot_parts) if slot_parts else 'no trays'}")

    reported_state = print_data.get("gcode_state")
    # Bambu often sends partial MQTT messages (no gcode_state) with progress data.
    # Use the last known state as fallback so progress updates are not dropped.
    state = reported_state or _printer_state
    if not state:
        return

    sync_override()

    # Parse mc_weight unconditionally — Bambu often sends it in the FINISH message
    # (state is already FINISH at that point, so it must be parsed before PRINTING_STATES check)
    filament = print_data.get("mc_weight") or print_data.get("filament_used")
    if filament is not None:
        try:
            val = float(filament)
            with _globals_lock:
                _last_filament_used = val
                if val > 0:
                    _filament_used_snapshot = val  # keep last valid value as fallback for finish
        except (TypeError, ValueError):
            pass

    # Update _subtask_name whenever the field appears in the payload
    raw_subtask = (print_data.get("subtask_name") or "").strip()
    if raw_subtask and raw_subtask != _subtask_name:
        _subtask_name = raw_subtask
        log(f"  Subtask name: {_subtask_name}")

    if reported_state and reported_state != _printer_state:
        log(f"Printer state: {state}")
        prev_state     = _printer_state
        _printer_state = state
        write_state()

        if state in PRINTING_STATES and prev_state not in PRINTING_STATES:
            # Clear stale confirm_slot_ dialog atomically — printer started printing before user responded
            with _state_lock:
                if _conversation_state and _conversation_state.startswith("confirm_slot_"):
                    _slot_update_buffer.pop(_conversation_state[len("confirm_slot_"):], None)
                    _conversation_state = None
                    _conversation_state_time = 0.0
            is_resume = prev_state in RESUMABLE_STATES
            if prev_state is not None and not is_resume:
                _print_start_time        = datetime.now()
                with _globals_lock:
                    _last_filament_used      = None
                    _filament_used_snapshot  = 0.0   # reset here (IDLE→PREPARE), not at PREPARE→RUNNING
                    _cloud_slot_weights      = {}
                    _cloud_slot_types        = {}
                    _cloud_weight_event.clear()
                    _milestone_sent.clear()
                    _active_slots_this_print.clear()
                    _slot_active_seconds.clear()
                    _current_slot_str   = None
                    _current_slot_since = None
                _nozzle_alert_sent       = False
                _nozzle_reached_target   = False
                _nozzle_last_target      = None
                _last_percent = 0
                _ten_min_alert_sent = False
                # Fetch planned weight from Bambu Cloud API in background
                threading.Thread(target=_fetch_cloud_weight_async, daemon=True).start()
            elif is_resume:
                _nozzle_alert_sent     = False
                _nozzle_reached_target = False
                _nozzle_last_target    = None
                log(f"Print resumed from {prev_state}.")
                send_telegram("▶️ ההדפסה חודשה")
            elif _print_start_time is None:
                _print_start_time = datetime.now()

        # reset progress when transitioning PREPARE → RUNNING
        elif state == "RUNNING" and prev_state == "PREPARE":
            _last_percent = 0
            with _globals_lock:
                _milestone_sent.clear()
            _nozzle_alert_sent     = False
            _nozzle_reached_target = False
            _nozzle_last_target    = None
            # _filament_used_snapshot intentionally NOT reset here — Cloud API may have filled it at PREPARE
            log("PREPARE → RUNNING: _last_percent reset to 0, _milestone_sent cleared.")
            threading.Thread(target=_wait_for_weight_then_start, daemon=True).start()

        elif state == "FINISH":
            _last_percent   = None
            _last_remaining = None
            _last_layer     = None
            _total_layers   = None
            finish_end = datetime.now()
            threading.Thread(target=_wait_and_finish, args=("FINISH", finish_end), daemon=True).start()
            _nozzle_alert_sent     = False
            _nozzle_reached_target = False
            _nozzle_last_target    = None
            log("Print FINISH.")

        elif state == "FAILED":
            _last_percent   = None
            _last_remaining = None
            _last_layer     = None
            _total_layers   = None
            fail_end = datetime.now()
            threading.Thread(target=_wait_and_finish, args=("FAILED", fail_end), daemon=True).start()
            _nozzle_alert_sent     = False
            _nozzle_reached_target = False
            _nozzle_last_target    = None
            send_telegram("❌ ההדפסה נכשלה!")
            log("Print FAILED.")

    if state in PRINTING_STATES:
        # Track active AMS slot with time accumulation — runs AFTER state-clear so no stale data
        if ams_raw:
            try:
                tn_int = int(ams_raw.get("tray_now", "255"))
                if 0 <= tn_int <= 3:
                    slot_str = str(tn_int + 1)
                    now = time.time()
                    with _globals_lock:
                        if slot_str not in _active_slots_this_print:
                            log(f"  [Slot tracking] New active slot: {slot_str} (total now: {sorted(_active_slots_this_print | {slot_str})})")
                        _active_slots_this_print.add(slot_str)
                        # Accumulate time for the outgoing slot if it changed
                        if _current_slot_str is not None and _current_slot_since is not None:
                            if _current_slot_str != slot_str:
                                delta = now - _current_slot_since
                                _slot_active_seconds[_current_slot_str] = \
                                    _slot_active_seconds.get(_current_slot_str, 0.0) + delta
                                log(f"  [Slot time] Slot {_current_slot_str} +{delta:.1f}s "
                                    f"(total {_slot_active_seconds[_current_slot_str]:.1f}s) → switching to {slot_str}")
                                _current_slot_str   = slot_str
                                _current_slot_since = now
                        else:
                            # First observation — start the clock
                            _current_slot_str   = slot_str
                            _current_slot_since = now
                        _slot_active_seconds.setdefault(slot_str, 0.0)
            except (ValueError, TypeError):
                pass

        remaining = print_data.get("mc_remaining_time")
        percent   = print_data.get("mc_percent")
        layer = print_data.get("layer_num")
        total = print_data.get("total_layer_num")
        if layer is not None:
            _last_layer = layer
        if total is not None:
            _total_layers = total

        if remaining is not None and percent is not None:
            prev_percent    = _last_percent or 0
            _last_percent   = percent
            _last_remaining = remaining
            _last_remaining_update = datetime.now()
            layer_info = f", layer {_last_layer}/{_total_layers}" if _last_layer is not None and _total_layers else ""
            log(f"  Progress: {percent}% complete, {format_time(remaining)} remaining{layer_info}")
            write_state()

            if percent > 0 and state == "RUNNING":
                for milestone in PROGRESS_MILESTONES:
                    crosses = percent >= milestone and prev_percent < milestone
                    with _globals_lock:
                        already = milestone in _milestone_sent
                        if crosses and not already:
                            _milestone_sent.add(milestone)
                    if crosses and not already:
                        def _send_milestone(m=milestone):
                            send_telegram(f"🖨️ ההדפסה הגיעה ל-{m}%")
                        threading.Thread(target=_send_milestone, daemon=True).start()

                if 0 < remaining <= 10 and not _ten_min_alert_sent:
                    _ten_min_alert_sent = True
                    eta_str = (datetime.now() + timedelta(minutes=remaining)).strftime("%H:%M")
                    msg_lines = [f"⏰ ההדפסה מסתיימת בעוד ~10 דקות!"]
                    if _subtask_name:
                        msg_lines.append(f"📄 {_subtask_name}")
                    msg_lines.append(f"🏁 סיום משוער: {eta_str}")
                    send_telegram("\n".join(msg_lines))

        name = (print_data.get("subtask_name") or "").strip()
        if not name:
            name = os.path.basename((print_data.get("gcode_file") or "").strip())
        if name and name != _current_filename:
            _current_filename = name
            log(f"  Print file: {_current_filename}")
            write_state()

    if print_data.get("nozzle_temper")        is not None: _nozzle_temp   = print_data["nozzle_temper"]
    if print_data.get("nozzle_target_temper") is not None: _nozzle_target = print_data["nozzle_target_temper"]
    if print_data.get("bed_temper")           is not None: _bed_temp      = print_data["bed_temper"]
    if print_data.get("bed_target_temper")    is not None: _bed_target    = print_data["bed_target_temper"]

    # ── AMS filament sync and per-slot change detection ──────────────────────
    if ams_raw:
        # Keep _ams_filament in sync (used by write_state / status command)
        parsed = _parse_ams_filament(print_data)
        if parsed and parsed != _ams_filament:
            _ams_filament = parsed
            log(f"  AMS active filament: {_ams_filament}")
            write_state()

        # Detect per-slot changes while printer is idle → ask for spool weight
        if state not in PRINTING_STATES:
            sync_ams_to_filament_data()
            for unit in ams_raw.get("ams", []):
                ams_idx = int(unit.get("id", 0))
                for tray in unit.get("tray", []):
                    tray_idx  = int(tray.get("id", 0))
                    slot_num  = str(ams_idx * 4 + tray_idx + 1)
                    ftype     = (tray.get("tray_type") or tray.get("tray_sub_brands") or "").strip()
                    color_hex = tray.get("tray_color", "")[:6]  # normalize to 6-char hex

                    if not ftype:
                        _ams_slots_snapshot[slot_num] = ("", "")  # sentinel: slot was empty
                        continue

                    current_sig = (ftype, color_hex)
                    prev_sig    = _ams_slots_snapshot.get(slot_num)

                    if prev_sig is not None and prev_sig != current_sig:
                        emoji, color_name = _hex_to_closest_color(color_hex)
                        log(f"  AMS slot {slot_num} changed: {prev_sig} → {current_sig}")
                        slot_disp = _SLOT_NUM_EMOJIS[int(slot_num) - 1]
                        # Store candidate data; ask user to confirm before resetting filament data
                        _slot_update_buffer[slot_num] = {
                            "type": ftype, "color_hex": color_hex,
                            "color_name": color_name, "color_emoji": emoji,
                        }
                        _set_conversation_state(f"confirm_slot_{slot_num}")
                        send_telegram(
                            f"🔄 סלוט {slot_disp}: זוהה שינוי ({ftype} {color_name} {emoji})\n"
                            f"החלפת גליל?",
                            reply_markup=_slot_confirm_keyboard(slot_num),
                        )

                    _ams_slots_snapshot[slot_num] = current_sig

    if state == "RUNNING":
        nozzle_actual = print_data.get("nozzle_temper")
        nozzle_target = print_data.get("nozzle_target_temper")
        if (nozzle_actual is not None and nozzle_target is not None
                and nozzle_target > 0):
            drop = nozzle_target - nozzle_actual
            # If the target changed significantly (up or down), reset _nozzle_reached_target
            # so we wait for the nozzle to reach the new target before alerting again.
            # Threshold: 10°C change in either direction.
            if _nozzle_last_target is not None and abs(nozzle_target - _nozzle_last_target) > 10:
                log(f"  [NOZZLE] Target changed significantly: {_nozzle_last_target:.1f} → {nozzle_target:.1f} — resetting reached_target")
                _nozzle_reached_target = False
                _nozzle_alert_sent     = False
            _nozzle_last_target = nozzle_target
            # Mark that the nozzle has reached its target (within 5°C tolerance)
            if not _nozzle_reached_target and drop <= 5:
                _nozzle_reached_target = True
                log(f"  [NOZZLE] Reached target: actual={nozzle_actual:.1f}, target={nozzle_target:.1f}")
            # Only alert on unexpected temperature drop after nozzle already reached target once
            if drop > 20 and not _nozzle_alert_sent and _nozzle_reached_target:
                log(f"  [NOZZLE] ALERT: target={nozzle_target:.1f}, actual={nozzle_actual:.1f}, drop={drop:.1f}")
                send_telegram("⚠️ טמפרטורת ה-Nozzle ירדה! בדוק את המדפסת.")
                _nozzle_alert_sent = True
            elif drop <= 20 and _nozzle_alert_sent:
                _nozzle_alert_sent = False

    # ── HMS error detection ───────────────────────────────────────────────────
    hms_list = print_data.get("hms")
    if hms_list is not None:
        if hms_list:
            if not _hms_alert_sent:
                first_code = hms_list[0].get("code", "?")
                if first_code == 196609:
                    send_telegram("🧵 גליל הפילמנט נגמר! החלף גליל והמשך את ההדפסה.")
                else:
                    send_telegram(f"⚠️ המדפסת דיווחה על שגיאה (קוד: {first_code}). בדוק את המדפסת.")
                _hms_alert_sent = True
                log(f"[HMS] Error alert sent: {hms_list}")
        else:
            if _hms_alert_sent:
                send_telegram("✅ השגיאה במדפסת נפתרה.")
                _hms_alert_sent = False
                log("[HMS] Error cleared — recovery message sent.")



# ── Telegram bot listener ─────────────────────────────────────────────────────

_tg_last_update_id = 0
TELEGRAM_POLL_INTERVAL = 5

STATE_LABELS = {
    "RUNNING": "מדפיס",
    "PREPARE": "מתכונן",
    "FINISH":  "הסתיים",
    "FAILED":  "נכשל",
    "PAUSE":   "מושהה",
    "IDLE":    "מחכה",
}

def _filament_context_text() -> str:
    try:
        data = filament_load()
        lines = []
        for slot_num, slot in data.get("slots", {}).items():
            if slot is None:
                lines.append(f"סלוט {slot_num}: ריק")
                continue
            parts = [f"סלוט {slot_num}:"]
            if slot.get("type"):        parts.append(slot["type"])
            if slot.get("color_name"):  parts.append(slot["color_name"])
            if slot.get("color_emoji"): parts.append(slot["color_emoji"])
            if slot.get("remaining_g") is not None:
                parts.append(f"נשאר {slot['remaining_g']:.0f}ג")
            else:
                parts.append("כמות לא הוגדרה")
            if slot.get("price_per_kg") is not None:
                parts.append(f"{slot['price_per_kg']}₪/ק״ג")
            lines.append(" | ".join(parts))
        return "\n".join(lines) if lines else "אין נתוני פילמנט"
    except Exception:
        return "אין נתוני פילמנט"


def _recent_history_text() -> str:
    try:
        history = history_load()
        recent  = history[-5:] if len(history) >= 5 else history
        lines   = []
        for h in reversed(recent):
            name     = h.get("filename") or h.get("file", "לא ידוע")
            duration = h.get("duration_minutes") or h.get("duration_min", 0)
            result   = RESULT_LABELS.get(h.get("result", ""), h.get("result", ""))
            lines.append(f"• {name} | {duration} דקות | {result}")
        return "\n".join(lines) if lines else "אין היסטוריה"
    except Exception:
        return "אין היסטוריה"


def _build_printer_context() -> str:
    state_map = {
        "RUNNING": "מדפיסה כעת",
        "PAUSE":   "מושהית",
        "FAILED":  "נכשלה",
        "FINISH":  "סיימה",
        "IDLE":    "במנוחה",
        "PREPARE": "מתכוננת להדפסה",
    }
    state_text     = state_map.get(_printer_state or "", _printer_state or "לא ידוע")
    remaining_text = f"{_last_remaining} דקות" if _last_remaining is not None else "לא ידוע"

    # Statistics + filament used this month
    stats = compute_stats()
    records = history_load()
    month_ago    = datetime.now() - timedelta(days=30)
    month_grams  = 0.0
    for r in records:
        try:
            if datetime.strptime(r["start"], "%Y-%m-%d %H:%M:%S") >= month_ago:
                month_grams += r.get("filament_g", 0) or 0
        except (KeyError, ValueError):
            continue
    stats_text = (
        f"שבוע אחרון: {stats['week_total']} הדפסות ({stats['week_ok']} הצליחו), "
        f"{stats['week_hours']:.1f} שעות\n"
        f"30 יום אחרונים: {stats['month_total']} הדפסות ({stats['month_ok']} הצליחו), "
        f"{stats['month_hours']:.1f} שעות, {month_grams:.0f}ג פילמנט"
    )

    return (
        "אתה עוזר חכם לניהול מדפסת תלת-ממד של Bambu Lab.\n"
        "ענה תמיד בעברית, בתמציתיות ובידידותיות.\n"
        "אם אינך יודע משהו בוודאות — אמור זאת במפורש.\n\n"
        "=== מצב נוכחי ===\n"
        f"סטטוס: {state_text}\n"
        f"קובץ מודפס: {_current_filename or 'אין'}\n"
        f"התקדמות: {_last_percent}%\n"
        f"זמן נשאר: {remaining_text}\n\n"
        "=== גלילים ===\n"
        f"{_filament_context_text()}\n\n"
        "=== סטטיסטיקות ===\n"
        f"{stats_text}\n\n"
        "=== 5 הדפסות אחרונות ===\n"
        f"{_recent_history_text()}\n\n"
        "=== מה אתה יכול לעזור בו ===\n"
        "- שאלות על מצב המדפסת והסטטוס שלה\n"
        "- סטטיסטיקות הדפסה (כמות הדפסות, שעות, גרמים שנוצלו)\n"
        "- חישובי עלות וכמות פילמנט\n"
        "- המלצות על סוגי פילמנט (PLA, PETG, TPU, ABS וכו׳)\n"
        "- טיפים לשיפור איכות הדפסה\n"
        "- פתרון בעיות נפוצות במדפסות Bambu Lab\n"
        "- שאלות כלליות על הדפסת תלת-ממד\n"
        "אל תמציא נתונים שלא סופקו לך."
    )


def ask_claude(user_message: str) -> str:
    try:
        import anthropic
        client   = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=_build_printer_context(),
            messages=[{"role": "user", "content": user_message}],
        )
        return response.content[0].text
    except Exception as e:
        log(f"[Claude API] error: {e}")
        return "מצטער, לא הצלחתי לעבד את השאלה כרגע 🤖"


HELP_TEXT = (
    "כפתורים זמינים:\n"
    "📊 סטטוס          — מצב מלא\n"
    "🧵 פילמנט         — מצב כל הסלוטים ב-AMS\n"
    "  ⚙️ 1️⃣ 2️⃣ 3️⃣ 4️⃣ — הגדרת גליל לסלוט\n"
    "❓ עזרה           — הצגת רשימה זו\n"
    "─────────────────\n"
    "🤖 צ׳אט חופשי — שאל כל שאלה:\n"
    "   כמה הדפסות עשיתי? כמה גרם השתמשתי?\n"
    "   היסטוריה, סטטיסטיקות, טיפים לתלת-ממד ועוד"
)

RESULT_LABELS = {"FINISH": "✅ הצליחה", "FAILED": "❌ נכשלה"}


def _tg_get_updates(offset: int):
    url = (
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
        f"?offset={offset}&timeout=4"
        f"&allowed_updates=%5B%22message%22%2C%22callback_query%22%5D"
    )
    req  = urllib.request.Request(url)
    resp = urllib.request.urlopen(req, timeout=10).read()
    return json.loads(resp)


def _handle_tg_command(text: str) -> None:
    global _slot_update_buffer
    cmd = text.strip()

    if "סטטוס" in cmd:
        printer = _printer_state or "unknown"
        label   = STATE_LABELS.get(printer, printer)
        lines   = ["📊 סטטוס מדפסת", "─" * 17, f"🖨️ סטטוס: {label}"]
        if _subtask_name:
            lines.append(f"📄 קובץ: {_subtask_name}")
        if _last_percent is not None:
            display_pct = _last_percent
            if _last_percent == 0 and _print_start_time is not None and _last_remaining is not None and _last_remaining > 0 and _last_remaining_update is not None:
                now = datetime.now()
                elapsed = (now - _print_start_time).total_seconds() / 60
                elapsed_since_update = (now - _last_remaining_update).total_seconds() / 60
                remaining_now = max(0, _last_remaining - elapsed_since_update)
                total_estimated = elapsed + remaining_now
                if total_estimated > 0:
                    display_pct = int(elapsed / total_estimated * 100)
            lines.append(f"📈 התקדמות: {display_pct}%")
        if _last_remaining is not None and _last_remaining_update is not None:
            elapsed_since_update = (datetime.now() - _last_remaining_update).total_seconds() / 60
            remaining_now = max(0, _last_remaining - elapsed_since_update)
        else:
            remaining_now = _last_remaining or 0
        if _print_start_time is not None:
            lines.append(f"🕐 התחלה: {_print_start_time.strftime('%H:%M')}")
        if _last_remaining is not None:
            lines.append(f"⏱ זמן נשאר: {format_time(int(remaining_now))}")
            if remaining_now > 0:
                eta = datetime.now() + timedelta(minutes=remaining_now)
                lines.append(f"🏁 סיום משוער: {eta.strftime('%H:%M')}")
        # שכבות — רק בזמן הדפסה פעילה וכשהנתונים קיימים
        if _printer_state in PRINTING_STATES and _last_layer is not None and _total_layers:
            lines.append(f"🗂 שכבה: {_last_layer} / {_total_layers}")
        # Nozzle + מיטה — שורה אחת
        if _nozzle_temp is not None or _bed_temp is not None:
            nozzle_str = f"Nozzle: {_nozzle_temp:.0f}°C" if _nozzle_temp is not None else ""
            bed_str    = f"מיטה: {_bed_temp:.0f}°C" if _bed_temp is not None else ""
            lines.append(f"🌡️ {' | '.join(x for x in [nozzle_str, bed_str] if x)}")
        # פילמנט: בזמן הדפסה — סלוטים פעילים; במנוחה — כל הסלוטים הטעונים
        d_fil = filament_load()
        if _printer_state in PRINTING_STATES:
            active = sorted(_active_slots_this_print) if _active_slots_this_print else [_get_active_slot_num()]
            if len(active) == 1:
                sn   = active[0]
                sd   = _SLOT_NUM_EMOJIS[int(sn) - 1] if sn.isdigit() and 1 <= int(sn) <= 4 else sn
                slot = d_fil["slots"].get(sn)
                if slot:
                    ft = slot.get("type", "PLA")
                    cn = slot.get("color_name", "")
                    ce = slot.get("color_emoji", "")
                    lines.append(f"🧵 פילמנט: {ft} {cn} {ce} (סלוט {sd})")
                elif _ams_filament:
                    lines.append(f"🧵 פילמנט: {_ams_filament}")
            else:
                for sn in active:
                    sd   = _SLOT_NUM_EMOJIS[int(sn) - 1] if sn.isdigit() and 1 <= int(sn) <= 4 else sn
                    slot = d_fil["slots"].get(sn)
                    if slot:
                        ft = slot.get("type", "PLA")
                        cn = slot.get("color_name", "")
                        ce = slot.get("color_emoji", "")
                        lines.append(f"🧵 סלוט {sd}: {ft} {cn} {ce}")
        else:
            for i, slot_emoji in enumerate(_SLOT_NUM_EMOJIS, 1):
                slot = d_fil["slots"].get(str(i))
                if slot:
                    ft = slot.get("type", "?")
                    cn = slot.get("color_name", "")
                    ce = slot.get("color_emoji", "")
                    lines.append(f"🧵 סלוט {slot_emoji}: {ft} {cn} {ce}")
        send_telegram("\n".join(lines))

    elif "כמה זמן נשאר" in cmd:
        if _printer_state not in PRINTING_STATES:
            send_telegram("המדפסת לא מדפיסה כרגע.")
        elif _last_remaining is None:
            send_telegram("אין מידע על הזמן שנותר עדיין.")
        else:
            # Apply elapsed-time correction (same as סטטוס command)
            if _last_remaining_update is not None:
                elapsed_since_update = (datetime.now() - _last_remaining_update).total_seconds() / 60
                remaining_now = max(0, _last_remaining - elapsed_since_update)
            else:
                remaining_now = _last_remaining
            pct = f"{_last_percent}% הושלם, " if _last_percent is not None else ""
            send_telegram(f"⏱️ {pct}נותר עוד {format_time(int(remaining_now))}.")

    elif "היסטוריה" in cmd:
        records = history_load()
        if not records:
            send_telegram("אין היסטוריית הדפסות עדיין.")
            return
        last5 = records[-5:][::-1]
        lines = ["📋 5 ההדפסות האחרונות:"]
        for r in last5:
            date   = r.get("start", "?")[:10]
            dur    = format_time(r.get("duration_minutes", 0))
            result = RESULT_LABELS.get(r.get("result"), r.get("result", "?"))
            fil    = f", {r['filament_g']:.1f}g" if r.get("filament_g") else ""
            fname  = f"\n  📄 {r['filename']}" if r.get("filename") else ""
            lines.append(f"• {date} | {dur} | {result}{fil}{fname}")
        send_telegram("\n".join(lines))

    elif "סטטיסטיקות" in cmd:
        s = compute_stats()
        def pct_str(ok, total):
            return f"{round(ok/total*100)}%" if total else "—"

        # חישוב גרמים ועלות ב-30 יום אחרונים
        records    = history_load()
        month_ago  = datetime.now() - timedelta(days=30)
        month_grams = 0.0
        month_cost  = 0.0
        d_fil = filament_load()
        for r in records:
            try:
                if datetime.strptime(r["start"], "%Y-%m-%d %H:%M:%S") >= month_ago:
                    g = r.get("filament_g") or 0.0
                    month_grams += g
            except (KeyError, ValueError):
                continue
        # עלות — ממוצע מחיר/ק"ג מכל הסלוטים הידועים
        prices = [
            slot["price_per_kg"]
            for slot in d_fil.get("slots", {}).values()
            if slot and slot.get("price_per_kg")
        ]
        avg_price = sum(prices) / len(prices) if prices else None
        if avg_price:
            month_cost = (month_grams / 1000) * avg_price

        lines = [
            "📊 סטטיסטיקות הדפסה:",
            "",
            "🗓️ שבוע אחרון:",
            f"  סך הכל: {s['week_total']} הדפסות",
            f"  הצלחה: {s['week_ok']} ({pct_str(s['week_ok'], s['week_total'])})",
            f"  שעות הדפסה: {s['week_hours']:.1f}h",
            "",
            "📅 30 יום אחרונים:",
            f"  סך הכל: {s['month_total']} הדפסות",
            f"  הצלחה: {s['month_ok']} ({pct_str(s['month_ok'], s['month_total'])})",
            f"  שעות הדפסה: {s['month_hours']:.1f}h",
        ]
        if month_grams > 0:
            filament_line = f"🧵 פילמנט החודש: {month_grams:.0f} גרם"
            if avg_price:
                filament_line += f" (₪{month_cost:.2f})"
            lines.append(f"  {filament_line}")
        send_telegram("\n".join(lines))

    elif "פילמנט" in cmd and not cmd.startswith("setup_slot_") and not cmd.startswith("sw_"):
        send_telegram(filament_status_text(), reply_markup=_filament_setup_keyboard())

    elif cmd.startswith("setup_slot_"):
        # "setup_slot_X" — show current values then weight keyboard for slot X
        try:
            slot_num  = int(cmd.split("_")[-1])
            slot_disp = _SLOT_NUM_EMOJIS[slot_num - 1]
            d         = filament_load()
            slot      = d["slots"].get(str(slot_num))
            if slot:
                ftype       = slot.get("type", "?")
                color_name  = slot.get("color_name", "?")
                color_emoji = slot.get("color_emoji", "")
                rem         = slot.get("remaining_g")
                price       = slot.get("price_per_kg")
                rem_str     = f"~{rem:.0f}ג" if rem is not None else "לא הוגדר"
                price_str   = f"{price:g}₪/ק״ג" if price else "לא הוגדר"
                header = (f"⚙️ סלוט {slot_disp}: {ftype} {color_name} {color_emoji}\n"
                          f"נשאר: {rem_str} | מחיר: {price_str}\n"
                          f"בחר משקל גליל חדש:")
            else:
                header = f"⚙️ סלוט {slot_disp} — ריק\nבחר משקל גליל:"
            send_telegram(header, reply_markup=_slot_weight_keyboard(slot_num))
        except (ValueError, IndexError):
            pass

    elif cmd.startswith("confirm_yes_"):
        slot_num  = cmd[len("confirm_yes_"):]
        slot_disp = _SLOT_NUM_EMOJIS[int(slot_num) - 1] if slot_num.isdigit() and 1 <= int(slot_num) <= 4 else slot_num
        buf = _slot_update_buffer.get(slot_num, {})
        # Reset slot data now that user confirmed replacement
        d_fil = filament_load()
        s = d_fil["slots"].get(slot_num)
        if s:
            s["type"]         = buf.get("type", s.get("type", "PLA"))
            s["color_hex"]    = buf.get("color_hex", "")
            s["color_name"]   = buf.get("color_name", "")
            s["color_emoji"]  = buf.get("color_emoji", "")
            s["weight_g"]     = None
            s["remaining_g"]  = None
            s["price_per_kg"] = None
            s["manually_set"] = False
            filament_save(d_fil)
        else:
            # Slot was None — create it
            ftype = buf.get("type", "PLA")
            d_fil["slots"][slot_num] = {
                "type": ftype, "color_hex": buf.get("color_hex", ""),
                "color_name": buf.get("color_name", ""), "color_emoji": buf.get("color_emoji", ""),
                "weight_g": None, "remaining_g": None, "price_per_kg": None,
            }
            filament_save(d_fil)
        _set_conversation_state(f"type_slot_{slot_num}")
        send_telegram(f"🔄 סלוט {slot_disp}: בחר סוג פילמנט:", reply_markup=_slot_type_keyboard(slot_num))

    elif cmd.startswith("confirm_no_"):
        slot_num = cmd[len("confirm_no_"):]
        _slot_update_buffer.pop(slot_num, None)
        _set_conversation_state(None)
        send_telegram("👍 לא בוצע שינוי")

    elif cmd.startswith("stype_"):
        # "stype_{slot_num}_{TYPE}"
        parts = cmd.split("_", 2)
        if len(parts) == 3:
            slot_num  = parts[1]
            ftype     = parts[2]
            slot_disp = _SLOT_NUM_EMOJIS[int(slot_num) - 1] if slot_num.isdigit() and 1 <= int(slot_num) <= 4 else slot_num
            d_fil = filament_load()
            s = d_fil["slots"].get(slot_num)
            if s:
                s["type"] = ftype
                filament_save(d_fil)
            _slot_update_buffer.setdefault(slot_num, {})["type"] = ftype
            _set_conversation_state(f"color_slot_{slot_num}")
            send_telegram(f"🎨 סלוט {slot_disp}: הקלד שם הצבע (לדוגמה: אדום, כחול, לבן):")

    elif cmd == "manual_replace":
        send_telegram("🔄 בחר את הסלוט שהחלפת:", reply_markup=_slot_select_keyboard())

    elif cmd.startswith("replace_slot_"):
        slot_num  = cmd[len("replace_slot_"):]
        slot_disp = _SLOT_NUM_EMOJIS[int(slot_num) - 1] if slot_num.isdigit() and 1 <= int(slot_num) <= 4 else slot_num
        _slot_update_buffer[slot_num] = {}
        d_fil = filament_load()
        s = d_fil["slots"].get(slot_num)
        if s:
            s["weight_g"]     = None
            s["remaining_g"]  = None
            s["price_per_kg"] = None
            s["manually_set"] = True
            filament_save(d_fil)
        else:
            d_fil["slots"][slot_num] = {
                "type": "?", "color_hex": "", "color_name": "?", "color_emoji": "⬜",
                "weight_g": None, "remaining_g": None, "price_per_kg": None,
                "manually_set": True,
            }
            filament_save(d_fil)
        _set_conversation_state(f"type_slot_{slot_num}")
        send_telegram(f"🔄 סלוט {slot_disp}: בחר סוג פילמנט:", reply_markup=_slot_type_keyboard(slot_num))

    elif cmd.startswith("sw_"):
        # "sw_{slot_num}_{weight|manual}" — set weight or enter manual mode
        parts = cmd.split("_")
        if len(parts) == 3:
            try:
                slot_num   = parts[1]
                weight_str = parts[2]
                slot_disp  = _SLOT_NUM_EMOJIS[int(slot_num) - 1] if slot_num.isdigit() and 1 <= int(slot_num) <= 4 else slot_num
                if weight_str == "manual":
                    _set_conversation_state(f"waiting_weight_slot_{slot_num}")
                    send_telegram(f"✏️ סלוט {slot_disp}: הזן משקל בגרמים (לדוגמה: 750):")
                else:
                    weight = float(weight_str)
                    filament_set_slot_weight(slot_num, weight)
                    d    = filament_load()
                    slot = d["slots"].get(slot_num)
                    ftype = slot.get("type", "?") if slot else "?"
                    _set_conversation_state(f"waiting_price_slot_{slot_num}")
                    send_telegram(
                        f"✅ סלוט {slot_disp}: {weight:.0f}ג נרשם\n"
                        f"מה המחיר של {ftype} ל-₪/ק״ג? (שלח מספר, או 0 לדילוג)"
                    )
            except (ValueError, IndexError):
                pass

    elif "עזרה" in cmd:
        send_telegram(HELP_TEXT)

    else:
        if ANTHROPIC_API_KEY:
            send_telegram("🤖 חושב...")
            send_telegram(ask_claude(cmd))
        # if no API key — silently ignore unknown commands


def poll_telegram() -> None:
    global _tg_last_update_id, _conversation_state, _conversation_state_time
    log("Telegram bot listener started.")
    send_menu()  # send persistent reply keyboard once at startup
    _409_backoff = 0  # exponential backoff counter for 409 Conflict
    while True:
        try:
            result = _tg_get_updates(_tg_last_update_id + 1)
            _409_backoff = 0  # reset backoff on success
            for update in result.get("result", []):
                uid = update["update_id"]
                _tg_last_update_id = max(_tg_last_update_id, uid)

                if "callback_query" in update:
                    cq      = update["callback_query"]
                    chat    = str(cq.get("from", {}).get("id", ""))
                    cmd     = cq.get("data", "").strip()
                    cq_id   = cq["id"]
                    if chat != TELEGRAM_CHAT_ID:
                        continue
                    log(f"Telegram callback from {chat}: {cmd!r}")
                    answer_callback_query(cq_id)
                    _handle_tg_command(cmd)

                elif "message" in update:
                    msg  = update["message"]
                    chat = str(msg.get("chat", {}).get("id", ""))
                    text = msg.get("text", "").strip()
                    if not text or chat != TELEGRAM_CHAT_ID:
                        continue
                    log(f"Telegram message from {chat}: {text!r}")

                    # ── Timeout / cancel ──────────────────────────────────────
                    _timed_out = False
                    _cancelled = False
                    with _state_lock:
                        if _conversation_state is not None:
                            if time.time() - _conversation_state_time > 300:
                                _conversation_state = None
                                _conversation_state_time = 0.0
                                _timed_out = True
                            elif text in ("ביטול", "בטל"):
                                _conversation_state = None
                                _conversation_state_time = 0.0
                                _cancelled = True
                    if _timed_out:
                        send_telegram("⏱ תם הזמן — השיחה בוטלה.")
                        # Don't skip — let the text fall through as a regular command
                    if _cancelled:
                        send_telegram("בוטל ✅")
                        continue  # discard this message entirely

                    # Snapshot state for consistent processing (MQTT thread may write concurrently)
                    with _state_lock:
                        cs = _conversation_state
                    # ─────────────────────────────────────────────────────────

                    if cs and cs.startswith("color_slot_"):
                        slot_num   = cs[len("color_slot_"):]
                        color_name = text.strip()
                        emoji      = _color_name_to_emoji(color_name)
                        slot_disp  = _SLOT_NUM_EMOJIS[int(slot_num) - 1] if slot_num.isdigit() and 1 <= int(slot_num) <= 4 else slot_num
                        d_fil = filament_load()
                        s = d_fil["slots"].get(slot_num)
                        if s is None:
                            # Slot missing on disk — create it before writing color
                            d_fil["slots"][slot_num] = {
                                "type": "PLA", "color_hex": "", "color_name": color_name,
                                "color_emoji": emoji if emoji else "🎨",
                                "weight_g": None, "remaining_g": None, "price_per_kg": None,
                            }
                            s = d_fil["slots"][slot_num]
                        s["color_name"]  = color_name
                        s["color_emoji"] = emoji if emoji else "🎨"
                        s["color_hex"]   = ""
                        filament_save(d_fil)
                        _slot_update_buffer.setdefault(slot_num, {}).update({"color_name": color_name, "color_emoji": emoji})
                        _set_conversation_state(f"waiting_weight_slot_{slot_num}")
                        confirm_line = f"{color_name} {emoji}" if emoji else color_name
                        send_telegram(
                            f"✅ {confirm_line}\nסלוט {slot_disp}: כמה גרם בגליל?",
                            reply_markup=_slot_weight_keyboard(int(slot_num))
                        )
                    elif cs and cs.startswith("type_slot_"):
                        # User typed instead of clicking button — re-prompt
                        slot_num  = cs[len("type_slot_"):]
                        send_telegram("בחר סוג מהכפתורים:", reply_markup=_slot_type_keyboard(slot_num))
                    elif cs and cs.startswith("confirm_slot_"):
                        slot_num  = cs[len("confirm_slot_"):]
                        send_telegram("לחץ על אחד הכפתורים למעלה:", reply_markup=_slot_confirm_keyboard(slot_num))
                    elif cs and cs.startswith("waiting_weight_slot_"):
                        slot_num = cs[len("waiting_weight_slot_"):]
                        try:
                            weight = float(text)
                            filament_set_slot_weight(slot_num, weight)
                            _set_conversation_state(f"waiting_price_slot_{slot_num}")
                            slot_disp = _SLOT_NUM_EMOJIS[int(slot_num) - 1] if slot_num.isdigit() and 1 <= int(slot_num) <= 4 else slot_num
                            d    = filament_load()
                            slot = d["slots"].get(slot_num)
                            ftype = slot.get("type", "?") if slot else "?"
                            send_telegram(
                                f"✅ סלוט {slot_disp}: {weight:.0f}ג נרשם\n"
                                f"מה המחיר של {ftype} ל-₪/ק״ג? (שלח מספר, או 0 לדילוג)"
                            )
                        except ValueError:
                            send_telegram("❌ נא לשלוח מספר בלבד, לדוגמה: 750")
                    elif cs and cs.startswith("waiting_price_slot_"):
                        slot_num = cs[len("waiting_price_slot_"):]
                        try:
                            price = float(text)
                            d = filament_load()
                            slot = d["slots"].get(slot_num)
                            if slot is not None:
                                slot["price_per_kg"] = price if price > 0 else None
                                filament_save(d)
                            _set_conversation_state(None)
                            slot_disp = _SLOT_NUM_EMOJIS[int(slot_num) - 1] if slot_num.isdigit() and 1 <= int(slot_num) <= 4 else slot_num
                            if slot:
                                ftype       = slot.get("type", "?")
                                color_name  = slot.get("color_name", "?")
                                color_emoji = slot.get("color_emoji", "")
                                weight_g    = slot.get("weight_g") or slot.get("remaining_g")
                                w_str       = f"{weight_g:.0f}ג" if weight_g else "?"
                                p_str       = f"{price:.0f}₪/ק״ג" if price > 0 else "לא הוגדר"
                                send_telegram(f"✅ סלוט {slot_disp} עודכן: {ftype} {color_name} {color_emoji} | {w_str} | {p_str}")
                            else:
                                send_telegram(f"✅ מחיר עודכן לסלוט {slot_disp}")
                        except ValueError:
                            send_telegram("❌ נא לשלוח מספר בלבד, לדוגמה: 80")
                    else:
                        _handle_tg_command(text)

        except urllib.error.HTTPError as exc:
            if exc.code == 409:
                _409_backoff = min(_409_backoff + 1, 6)  # cap at 2^6 = 64 s
                wait = 2 ** _409_backoff
                log(f"[Telegram poll] 409 Conflict — another instance is polling. "
                    f"Backing off {wait}s. "
                    f"Run: sudo systemctl status bambu-monitor (check for duplicate processes)")
                time.sleep(wait)
                continue
            log(f"[Telegram poll] Error: {exc}")
        except Exception as exc:
            log(f"[Telegram poll] Error: {exc}")
        time.sleep(TELEGRAM_POLL_INTERVAL)


# ── Status command (CLI) ──────────────────────────────────────────────────────

def cmd_status() -> None:
    import platform
    monitor_status = "not running"

    if platform.system() == "Darwin":
        try:
            out = subprocess.check_output(
                ["launchctl", "list", "com.yourname.bambu-monitor"],
                stderr=subprocess.DEVNULL,
            ).decode()
            pid = None
            for line in out.splitlines():
                if '"PID"' in line:
                    pid = line.strip().split()[-1].rstrip(";")
                    break
            monitor_status = f"running (PID {pid})" if pid and pid != "0" else "loaded but not running"
        except subprocess.CalledProcessError:
            monitor_status = "not running"
    else:
        try:
            out = subprocess.check_output(
                ["ps", "aux"],
                stderr=subprocess.DEVNULL,
            ).decode()
            for line in out.splitlines():
                if "bambu_monitor" in line and "grep" not in line:
                    parts = line.split()
                    pid = parts[1] if len(parts) > 1 else "?"
                    monitor_status = f"running (PID {pid})"
                    break
        except Exception:
            monitor_status = "unknown"

    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            state = json.load(f)
        printer = state.get("printer_state", "unknown")
        plug    = state.get("plug_state",    "unknown")
        updated = state.get("updated",       "unknown")
    else:
        printer = updated = "unknown (monitor has not reported yet)"
        plug    = "unknown"

    print(f"Monitor : {monitor_status}")
    print(f"Printer : {printer}")
    print(f"Plug    : {plug}")
    print(f"Updated : {updated}")


# ── Main ──────────────────────────────────────────────────────────────────────

def _bambu_uid() -> str:
    token = bambu_login()
    url   = "https://api.bambulab.com/v1/user-service/my/profile"
    req   = urllib.request.Request(url, headers={
        "Authorization": "Bearer " + token,
        "User-Agent":    "bambu_network_agent/01.09.05.01",
    })
    resp = json.loads(urllib.request.urlopen(req, timeout=15).read())
    uid  = str(resp["uidStr"])
    log(f"Bambu uid: {uid}")
    return uid


def build_client():
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    except AttributeError:
        client = mqtt.Client()

    token = bambu_login()
    uid   = _bambu_uid()
    client.username_pw_set(f"u_{uid}", token)
    ctx = ssl.create_default_context()
    client.tls_set_context(ctx)
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message
    return client


_PID_LOCK_FILE = "/tmp/bambu_monitor.lock"
_pid_lock_fh   = None  # keep file handle open so lock is held for process lifetime


def _acquire_pid_lock() -> None:
    """Acquire an exclusive flock on the PID lock file.

    If another process already holds the lock (i.e. bambu_monitor is already
    running), log an error and exit immediately.  This prevents two instances
    from polling the same Telegram bot token simultaneously (409 Conflict).
    """
    global _pid_lock_fh
    _pid_lock_fh = open(_PID_LOCK_FILE, "w")
    try:
        fcntl.flock(_pid_lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _pid_lock_fh.write(str(os.getpid()))
        _pid_lock_fh.flush()
    except OSError:
        _pid_lock_fh.close()
        sys.exit(
            f"ERROR: bambu_monitor is already running (lock held by another process). "
            f"Check: sudo systemctl status bambu-monitor  or  cat {_PID_LOCK_FILE}"
        )


def main():
    _acquire_pid_lock()
    log("Bambu Monitor starting.")
    log(f"Printer serial : {SERIAL}")
    log(f"Cloud MQTT     : {CLOUD_HOST}:{CLOUD_PORT}")

    load_state_from_file()

    try:
        bambu_login()
    except Exception as exc:
        sys.exit(f"Bambu Cloud login failed: {exc}")

    threading.Thread(target=poll_telegram, daemon=True, name="tg-listener").start()

    # Periodic pushall: ask the printer for a full status dump every 5 minutes.
    # This ensures progress data stays fresh even when the printer sends infrequent
    # MQTT messages (sometimes gaps of 30+ minutes between mc_percent updates).
    PUSHALL_INTERVAL = 5 * 60  # seconds

    def _periodic_pushall(mqtt_client_ref: list) -> None:
        while True:
            time.sleep(PUSHALL_INTERVAL)
            c = mqtt_client_ref[0]
            if c is None:
                continue
            try:
                pushall = json.dumps({"pushing": {"sequence_id": "0", "command": "pushall"}})
                c.publish(TOPIC_REQ, pushall)
                log("Periodic pushall sent to printer.")
            except Exception as exc:
                log(f"Periodic pushall error: {exc}")

    _mqtt_client_ref = [None]
    threading.Thread(target=_periodic_pushall, args=(_mqtt_client_ref,),
                     daemon=True, name="pushall-timer").start()
    log(f"Periodic pushall thread started (every {PUSHALL_INTERVAL // 60} min).")

    while True:
        try:
            client = build_client()
            _mqtt_client_ref[0] = client
            log(f"Connecting to Bambu Cloud MQTT ({CLOUD_HOST}:{CLOUD_PORT})…")
            client.connect(CLOUD_HOST, CLOUD_PORT, keepalive=60)
            client.reconnect_delay_set(min_delay=1, max_delay=1)
            client.loop_forever(retry_first_connection=False)
        except KeyboardInterrupt:
            log("Stopped by user.")
            _mqtt_client_ref[0] = None
            break
        except Exception as exc:
            log(f"Error: {exc}. Retrying in 15 s…")
        finally:
            _mqtt_client_ref[0] = None
        log("MQTT loop ended. Retrying in 15 s…")
        time.sleep(15)


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] == "status":
        cmd_status()
    else:
        main()
