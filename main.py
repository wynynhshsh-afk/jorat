import asyncio
import os
import json
import logging
import random
import sqlite3
import string
import time
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import quote

import re
import secrets
from datetime import datetime, timedelta, timezone

import aiohttp
from aiohttp import web

# ============================== CONFIG ===================================
# این مقادیر را خودت اینجا تنظیم کن (طبق درخواست صریح کاربر، نه .env)

# روی Render این‌ها رو در Environment Variables بگذار (نه داخل کد)؛ اگه ست نشن، مقدار پیش‌فرض همین‌جا استفاده می‌شه
BOT_TOKEN = os.environ.get("BOT_TOKEN", "PUT_YOUR_TOKEN")
OWNER_ID = os.environ.get("OWNER_ID", "PUT_YOUR_ID")      # شناسهٔ کاربری خودت در سروش

# توجه: توکن دیگه مستقیم توی API_BASE چسبونده نمی‌شه (مثل نمونه‌ای که
# کاربر داد)، بلکه هر فراخوانی جدا quote و اضافه می‌شه — همون الگوی
# SoroushAdapter._call در نمونه.
API_BASE = "https://api.splus.ir/bot"
POLL_TIMEOUT = 30
DB_PATH = os.environ.get("DB_PATH", "truth_or_dare.db")   # روی Render: /data/truth_or_dare.db (دیسک دائمی)

TURN_TIMEOUT_SECONDS = 60
MIN_PLAYERS_TO_START = 2
MAX_ROOM_NAME_LEN = 40
MAX_SKIPS_PER_TURN = 1          # تعداد دفعات مجاز «رد کردن سؤال» در هر نوبت
DEFAULT_ROOM_CAPACITY = 10

POINTS_TRUTH = 10
POINTS_DARE = 15
POINTS_PARTICIPATION = 20

# ---------------------------- سیستم سکه ------------------------------------
DAILY_GIFT_COINS = 15            # هدیهٔ روزانه
COIN_PRICE_TOMAN = 500           # قیمت هر سکه (تومان)
MIN_COIN_PURCHASE = 10           # حداقل تعداد سکه در هر خرید
MAX_COIN_PURCHASE = 100000
# هزینهٔ «ساخت روم» (ورود به روم دیگران و روم رندوم رایگان است)
ROOM_COSTS = {"HOKM": 20, "LUDO": 30, "SNL": 25, "NAF": 20, "TOD": 30}
# متنی که بعد از محاسبهٔ مبلغ به کاربر نشان داده می‌شود؛ شمارهٔ کارت خودت را بگذار
PAYMENT_CARD_TEXT = "💳  شماره کارت: 5022-2915-4965-\n👤 به نام:غفاری"

# ---------------------------- بازی‌ها --------------------------------------
# برای بازی گرافیکی، آدرس عمومی سرورت (دامنه/HTTPS) را اینجا بگذار؛ خالی = فقط بازی متنی
WEB_BASE_URL = (os.environ.get("WEB_BASE_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "").rstrip("/")
WEB_HOST = "0.0.0.0"
WEB_PORT = int(os.environ.get("PORT", "8080"))   # Render خودش PORT رو ست می‌کنه
KEEPALIVE_URL = os.environ.get("RENDER_EXTERNAL_URL", "") if os.environ.get("KEEPALIVE", "1") == "1" else ""
AFK_SECONDS = 75                 # مهلت هر نوبت در حکم/منچ/مار و پله
AFK_KICK_AFTER = 3               # بعد از چند بار تایم‌اوت پشت‌سرهم، بازیکن حذف می‌شود
AUTOSTART_DELAY = 15             # شروع خودکار روم‌های رندوم
HOKM_TARGET_POINTS = 5
NAF_ROUNDS = 5
NAF_ROUND_SECONDS = 90
NAF_GRACE_SECONDS = 10
NAF_BETWEEN_SECONDS = 8

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("tod_bot")

# ============================== DATABASE ==================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id TEXT PRIMARY KEY,
    display_name TEXT,
    first_seen INTEGER,
    last_active INTEGER,
    games_played INTEGER DEFAULT 0,
    truths_answered INTEGER DEFAULT 0,
    dares_done INTEGER DEFAULT 0,
    turns_missed INTEGER DEFAULT 0,
    score INTEGER DEFAULT 0,
    state TEXT,              -- وضعیت گفتگوی جاری (FSM) به صورت JSON
    current_room_id TEXT
);

CREATE TABLE IF NOT EXISTS rooms (
    room_id TEXT PRIMARY KEY,
    name TEXT,
    room_type TEXT,           -- PUBLIC / PRIVATE
    owner_id TEXT,
    status TEXT,              -- OPEN / IN_PROGRESS / CLOSED
    capacity INTEGER,
    invite_code TEXT,
    accepting_new INTEGER DEFAULT 1,
    created_at INTEGER,
    current_turn_order INTEGER DEFAULT 0,
    current_game_id TEXT
);

CREATE TABLE IF NOT EXISTS room_members (
    room_id TEXT,
    user_id TEXT,
    join_order INTEGER,
    joined_at INTEGER,
    turns_taken INTEGER DEFAULT 0,
    turns_missed INTEGER DEFAULT 0,
    PRIMARY KEY (room_id, user_id)
);

CREATE TABLE IF NOT EXISTS questions (
    question_id TEXT PRIMARY KEY,
    qtype TEXT,                -- TRUTH / DARE
    text TEXT,
    category TEXT,
    difficulty TEXT,
    active INTEGER DEFAULT 1,
    created_by TEXT,
    created_at INTEGER
);

CREATE TABLE IF NOT EXISTS games (
    game_id TEXT PRIMARY KEY,
    room_id TEXT,
    started_at INTEGER,
    ended_at INTEGER,
    turns_done INTEGER DEFAULT 0,
    turns_skipped INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS turns (
    turn_id TEXT PRIMARY KEY,
    game_id TEXT,
    room_id TEXT,
    user_id TEXT,
    qtype TEXT,
    question_id TEXT,
    status TEXT,         -- WAITING_CHOICE / WAITING_ANSWER / DONE / EXPIRED
    started_at INTEGER,
    deadline INTEGER,
    skips_used INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS answers (
    turn_id TEXT PRIMARY KEY,
    user_id TEXT,
    answer_text TEXT,
    media_file_id TEXT,
    media_type TEXT,
    submitted_at INTEGER
);

CREATE TABLE IF NOT EXISTS friendships (
    user_a TEXT,
    user_b TEXT,
    status TEXT,          -- PENDING / ACCEPTED
    requested_by TEXT,
    created_at INTEGER,
    PRIMARY KEY (user_a, user_b)
);

CREATE TABLE IF NOT EXISTS reports (
    report_id TEXT PRIMARY KEY,
    reporter_id TEXT,
    room_id TEXT,
    report_type TEXT,
    notes TEXT,
    created_at INTEGER,
    handled INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS admin_logs (
    log_id TEXT PRIMARY KEY,
    actor_id TEXT,
    action TEXT,
    details TEXT,
    created_at INTEGER
);

-- برای هر نوبت، شناسهٔ پیامی که برای هر عضو روم ارسال شده ذخیره می‌شود
-- تا به‌جای ارسال پیام تازه، همان پیام ویرایش (edit) شود و صفحهٔ چت
-- شلوغ نشود.
CREATE TABLE IF NOT EXISTS turn_messages (
    turn_id TEXT,
    user_id TEXT,
    message_id TEXT,
    PRIMARY KEY (turn_id, user_id)
);
"""


def now() -> int:
    return int(time.time())


def new_id(prefix: str) -> str:
    return f"{prefix}_{int(time.time()*1000)}_{random.randint(1000,9999)}"


def gen_invite_code() -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=8))


class DB:
    """لایهٔ دسترسی به دیتابیس. از یک کانکشن با WAL استفاده می‌شود؛
    عملیات حساس (کم‌کردن ظرفیت، تغییر نوبت) داخل تراکنش صریح انجام
    می‌شوند تا اتمیک باشند."""

    def __init__(self, path: str):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.executescript(SCHEMA)
        self.conn.commit()
        self.lock = asyncio.Lock()  # قفل سطح‌برنامه برای عملیات نوشتنی حساس

    def execute(self, sql, params=()):
        cur = self.conn.execute(sql, params)
        self.conn.commit()
        return cur

    def fetchone(self, sql, params=()):
        return self.conn.execute(sql, params).fetchone()

    def fetchall(self, sql, params=()):
        return self.conn.execute(sql, params).fetchall()


db = DB(DB_PATH)

# ---------------- مهاجرت دیتابیس: ستون‌ها و جدول‌های سکه/بازی ----------------
SCHEMA_V2 = """
CREATE TABLE IF NOT EXISTS purchases (
    purchase_id TEXT PRIMARY KEY,
    user_id TEXT,
    coins INTEGER,
    price INTEGER,
    receipt_file_id TEXT,
    receipt_type TEXT,          -- photo / document
    status TEXT,                -- PENDING / APPROVED / REJECTED
    created_at INTEGER,
    decided_at INTEGER,
    decided_by TEXT,
    owner_msg_id TEXT,
    owner_msg_kind TEXT         -- media / text
);
CREATE TABLE IF NOT EXISTS coin_log (
    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT,
    delta INTEGER,
    reason TEXT,
    created_at INTEGER
);
CREATE TABLE IF NOT EXISTS web_tokens (
    token TEXT PRIMARY KEY,
    room_id TEXT,
    user_id TEXT,
    created_at INTEGER
);
"""


def _migrate(conn):
    for table, col in (
        ("users", "coins INTEGER DEFAULT 0"),
        ("users", "last_daily TEXT"),
        ("rooms", "game_type TEXT DEFAULT 'TOD'"),
        ("rooms", "ui_mode TEXT DEFAULT 'TEXT'"),
        ("rooms", "is_random INTEGER DEFAULT 0"),
        ("rooms", "game_state TEXT"),
        ("rooms", "game_version INTEGER DEFAULT 0"),
        ("rooms", "last_result TEXT"),
    ):
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass  # ستون از قبل وجود دارد
    conn.executescript(SCHEMA_V2)
    conn.commit()


_migrate(db.conn)

# ============================== BOT API WRAPPER ============================
#
# این بخش دقیقاً با همون الگوی SoroushAdapter در نمونه‌ای که کاربر داد
# بازنویسی شده: یک aiohttp.ClientSession پایدار (نه یک اتصال تازه به
# ازای هر فراخوانی)، آدرس به‌صورت {API_BASE}{token}/{method}، GET برای
# getUpdates (با پارامترهای flatten‌شده) و POST JSON برای بقیهٔ متدها،
# و deleteWebhook در شروع کار تا اگه وبهوکی قبلاً ست شده بود، جلوی
# دریافت آپدیت از طریق polling رو نگیره.

_http_session: Optional[aiohttp.ClientSession] = None


class ApiError(Exception):
    def __init__(self, message: str, retryable: bool = False):
        super().__init__(message)
        self.retryable = retryable


async def get_http_session() -> aiohttp.ClientSession:
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=POLL_TIMEOUT + 20)
        )
    return _http_session


async def api_call(method: str, payload: dict, timeout: int = 20, get: bool = False) -> dict:
    """فراخوانی یک متد از Bot API سروش. در خطای شبکه‌ای/موقتی، دیکشنری
    {"ok": False, "error": ...} برمی‌گردونه تا صدازننده تصمیم بگیره
    (بازی رو خراب نکنه)."""
    session = await get_http_session()
    url = f"{API_BASE}{quote(BOT_TOKEN, safe=':')}/{method}"
    try:
        if get:
            flat = {
                k: (json.dumps(v, ensure_ascii=False) if isinstance(v, (list, dict))
                    else ("true" if v is True else "false" if v is False else str(v)))
                for k, v in payload.items()
            }
            ctx = session.get(url, params=flat)
        else:
            ctx = session.post(url, json=payload)
        async with ctx as resp:
            data = await resp.json(content_type=None)
            status = resp.status
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.warning("خطای شبکه در فراخوانی %s: %s", method, type(e).__name__)
        return {"ok": False, "error": f"network error ({type(e).__name__})"}
    except Exception as e:
        log.error("خطای غیرمنتظره در فراخوانی %s: %s", method, e)
        return {"ok": False, "error": str(e)}

    if not isinstance(data, dict) or not data.get("ok", False):
        log.warning("پاسخ ناموفق از %s (status=%s): %s", method, status, data)
        return {"ok": False, "error": data, "status": status}
    return data


async def delete_webhook():
    """اگه قبلاً روی این توکن webhook تنظیم شده باشه، getUpdates هیچ‌وقت
    آپدیتی نمی‌گیره. برای همین دقیقاً مثل نمونه، همون اول حذفش می‌کنیم."""
    res = await api_call("deleteWebhook", {})
    if not res.get("ok", True):
        log.warning("deleteWebhook ناموفق بود (مهم نیست اگه از اول وبهوکی نبوده): %s", res.get("error"))


async def api_get_me() -> dict:
    return await api_call("getMe", {})


def build_keyboard(rows: list) -> dict:
    """rows: لیستی از لیست رشته‌ها -> ReplyKeyboardMarkup"""
    return {
        "keyboard": [[{"text": t} for t in row] for row in rows],
        "resize_keyboard": True,
        "one_time_keyboard": False,
    }


async def send_message(chat_id: str, text: str, keyboard: Optional[list] = None):
    payload = {"chat_id": chat_id, "text": text}
    if keyboard is not None:
        payload["reply_markup"] = build_keyboard(keyboard)
    res = await api_call("sendMessage", payload)
    if not res.get("ok", True) and "error" in res:
        log.warning("sendMessage to %s failed: %s", chat_id, res.get("error"))
    return res


async def edit_message_text(chat_id: str, message_id: str, text: str) -> dict:
    """ویرایش متن یک پیام قبلی. توجه: در Bot APIهای سازگار با تلگرام،
    ReplyKeyboardMarkup به یک پیام خاص متصل نیست (روی کل چت اعمال
    می‌شه)، پس editMessageText فقط متنِ همون حباب رو عوض می‌کنه و
    دکمه‌های پایین صفحه رو دست نمی‌زنه؛ برای همین هر جا لازم بود
    دکمه‌ها عوض بشن (نه فقط متن)، همچنان یک پیام جدید و کوچیک جدا
    فرستاده می‌شه."""
    return await api_call(
        "editMessageText", {"chat_id": chat_id, "message_id": message_id, "text": text}
    )


def _extract_message_id(res: dict) -> Optional[str]:
    if not res.get("ok", False):
        return None
    result = res.get("result") or {}
    mid = result.get("message_id") if isinstance(result, dict) else None
    return str(mid) if mid is not None else None


async def broadcast_turn_status(turn_id: str, room_id: str, text: str, exclude: Optional[str] = None):
    """متن وضعیت یک نوبت رو برای همهٔ اعضای روم می‌فرسته/آپدیت می‌کنه.
    بار اول: پیام جدید می‌فرسته و message_id رو ذخیره می‌کنه.
    بارهای بعد (برای همین turn_id): همون پیام رو ویرایش می‌کنه؛ اگه
    ویرایش به هر دلیلی (مثلاً نسخهٔ API که editMessageText نداره)
    شکست خورد، به‌صورت خودکار یک پیام جدید می‌فرسته تا کاربر چیزی رو
    از دست نده."""
    for m in room_members(room_id):
        if exclude and m["user_id"] == exclude:
            continue
        chat_id = m["user_id"]
        existing = db.fetchone(
            "SELECT message_id FROM turn_messages WHERE turn_id=? AND user_id=?",
            (turn_id, chat_id),
        )
        if existing and existing["message_id"]:
            res = await edit_message_text(chat_id, existing["message_id"], text)
            if res.get("ok", False):
                continue
            log.info(
                "ویرایش پیام نوبت برای %s ناموفق بود، پیام جدید فرستاده می‌شه: %s",
                chat_id, res.get("error"),
            )
        res = await send_message(chat_id, text)
        mid = _extract_message_id(res)
        if mid:
            db.execute(
                "INSERT OR REPLACE INTO turn_messages(turn_id,user_id,message_id) VALUES (?,?,?)",
                (turn_id, chat_id, mid),
            )


def forget_turn_messages(turn_id: str):
    db.execute("DELETE FROM turn_messages WHERE turn_id=?", (turn_id,))


async def send_media(chat_id: str, media_type: str, file_id: str, caption: str = ""):
    """بازارسال رسانه با file_id (مطابق الگوی copyMessage/sendPhoto و..
    در Bot APIهای سازگار با تلگرام). اگر این متد خاص در سروش در دسترس
    نبود، خطا لاگ می‌شود و بازی متوقف نمی‌شود — طبق الزام «خطای ارسال
    رسانه نباید وضعیت بازی را خراب کند»."""
    method_map = {
        "photo": "sendPhoto",
        "video": "sendVideo",
        "voice": "sendVoice",
        "video_note": "sendVideoNote",
    }
    method = method_map.get(media_type)
    if not method:
        return {"ok": False, "error": "unsupported media type"}
    field_map = {
        "photo": "photo",
        "video": "video",
        "voice": "voice",
        "video_note": "video_note",
    }
    payload = {"chat_id": chat_id, field_map[media_type]: file_id}
    if caption:
        payload["caption"] = caption
    return await api_call(method, payload)


# ============================== STATE HELPERS ==============================


def get_or_create_user(user_id: str, display_name: str):
    row = db.fetchone("SELECT * FROM users WHERE user_id=?", (user_id,))
    if row is None:
        db.execute(
            "INSERT INTO users(user_id, display_name, first_seen, last_active, state) "
            "VALUES (?,?,?,?,?)",
            (user_id, display_name, now(), now(), None),
        )
        row = db.fetchone("SELECT * FROM users WHERE user_id=?", (user_id,))
    else:
        db.execute(
            "UPDATE users SET last_active=?, display_name=? WHERE user_id=?",
            (now(), display_name, user_id),
        )
    return row


def set_state(user_id: str, state: Optional[dict]):
    db.execute(
        "UPDATE users SET state=? WHERE user_id=?",
        (json.dumps(state, ensure_ascii=False) if state else None, user_id),
    )


def get_state(user_id: str) -> Optional[dict]:
    row = db.fetchone("SELECT state FROM users WHERE user_id=?", (user_id,))
    if row and row["state"]:
        return json.loads(row["state"])
    return None


def is_owner(user_id: str) -> bool:
    # بررسی سمت سرور؛ به هیچ دکمه‌ای برای مخفی‌کردن متکی نیست.
    return str(user_id) == str(OWNER_ID)


MAIN_MENU = [
    ["🎮 بازی", "💰 خرید سکه"],
    ["🎁 هدیه روزانه", "🙍 پروفایل من"],
    ["📘 راهنما"],
]

GAME_MENU = [
    ["🎯 ساخت روم", "🔑 ورود به روم"],
    ["🎲 روم رندوم (رایگان)", "👥 دوستان"],
    ["🔙 منوی اصلی"],
]

TRUTH_DARE_MENU = [["حقیقت", "جرئت"], ["رد کردن سؤال"]]

WELCOME = (
    "🎉 به دنیای بازی‌ها خوش اومدی! (حکم، منچ، مار و پله، اسم و فامیل، جرئت یا حقیقت)\n\n"
    "اینجا می‌تونی با دوستات بازی کنی، روم اختصاصی بسازی یا وارد بازی‌های عمومی بشی.\n\n"
    "آماده‌ای؟ 🚀"
)

HELP_TEXT = (
    "📘 راهنمای بازی\n\n"
    "۱. با «ساخت روم» یک روم بساز یا با «ورود به روم» وارد روم دوستت شو.\n"
    "۲. گاد روم (سازنده) با دکمهٔ «شروع بازی» بازی رو شروع می‌کنه.\n"
    "۳. وقتی نوبت توئه، «حقیقت» یا «جرئت» رو انتخاب کن.\n"
    "۴. برای هر نوبت ۶۰ ثانیه وقت داری؛ اگه پاسخ ندی، نوبتت رد می‌شه.\n"
    "۵. حقیقت رو با پیام متنی جواب بده؛ جرئت رو با عکس/ویدیو/ویس/ویدیو مسیج انجام بده.\n"
    "۶. هر نوبت یک‌بار می‌تونی سؤال رو رد کنی و سؤال جدید بگیری.\n"
    "۷. امتیازها به‌صورت خودکار و سرور-ساید ثبت می‌شن، نمی‌شه تقلب کرد 😉"
)


# ============================== ROOM / GAME LOGIC ===========================


def room_members(room_id: str):
    return db.fetchall(
        "SELECT rm.*, u.display_name FROM room_members rm "
        "JOIN users u ON u.user_id = rm.user_id "
        "WHERE rm.room_id=? ORDER BY rm.join_order ASC",
        (room_id,),
    )


def room_member_count(room_id: str) -> int:
    row = db.fetchone(
        "SELECT COUNT(*) c FROM room_members WHERE room_id=?", (room_id,)
    )
    return row["c"]


async def broadcast(room_id: str, text: str, exclude: Optional[str] = None):
    for m in room_members(room_id):
        if exclude and m["user_id"] == exclude:
            continue
        await send_message(m["user_id"], text)


async def broadcast_media(room_id: str, media_type: str, file_id: str, caption: str, exclude: Optional[str] = None):
    for m in room_members(room_id):
        if exclude and m["user_id"] == exclude:
            continue
        res = await send_media(m["user_id"], media_type, file_id, caption)
        if not res.get("ok", True):
            log.warning(
                "پخش رسانه برای کاربر %s در روم %s ناموفق بود: %s",
                m["user_id"], room_id, res.get("error"),
            )


def build_room_summary(room_id: str) -> str:
    room = db.fetchone("SELECT * FROM rooms WHERE room_id=?", (room_id,))
    owner = db.fetchone("SELECT display_name FROM users WHERE user_id=?", (room["owner_id"],))
    cnt = room_member_count(room_id)
    type_fa = "عمومی" if room["room_type"] == "PUBLIC" else "خصوصی"
    return (
        f"🎉 رومت با موفقیت ساخته شد!\n\n"
        f"🎮 بازی: {GAME_TYPES[room['game_type'] or 'TOD']['title']}\n"
        f"📛 نام روم: {room['name']}\n"
        f"👑 گاد بازی: {owner['display_name']}\n"
        f"👥 بازیکنان: {cnt} نفر\n"
        f"🔒 نوع روم: {type_fa}\n"
        f"🔑 کد ورود: {room['invite_code']}\n\n"
        f"هر وقت آماده بودی، بازی رو شروع کن!"
    )


ROOM_MENU = [
    ["▶️ شروع بازی", "➕ دعوت از دوستان"],
    ["👥 بازیکنان روم", "⚙️ تنظیمات روم"],
    ["🚪 خروج از روم"],
]


async def enter_room_flow(user_id: str, display_name: str, room_id: str):
    db.execute("UPDATE users SET current_room_id=? WHERE user_id=?", (room_id, user_id))
    await send_message(
        user_id,
        f"✅ با موفقیت وارد روم شدی!\n\n"
        f"📛 نام روم: {db.fetchone('SELECT name FROM rooms WHERE room_id=?',(room_id,))['name']}\n"
        f"👥 تعداد بازیکنان: {room_member_count(room_id)}",
        keyboard=ROOM_MENU,
    )
    await broadcast(room_id, f"👋 {display_name} وارد روم شد!", exclude=user_id)


async def try_join_room(user_id: str, display_name: str, room_id: str) -> Optional[str]:
    """تلاش اتمیک برای ورود به روم. خروجی: پیام خطا یا None اگر موفق بود."""
    async with db.lock:
        room = db.fetchone("SELECT * FROM rooms WHERE room_id=?", (room_id,))
        if room is None:
            return "این روم وجود نداره."
        if room["status"] == "CLOSED":
            return "این روم بسته شده."
        if room["status"] == "IN_PROGRESS":
            return "بازی این روم شروع شده و فعلاً ورود جدید امکان‌پذیر نیست."
        if not room["accepting_new"]:
            return "گاد این روم پذیرش بازیکن جدید رو غیرفعال کرده."
        existing = db.fetchone(
            "SELECT 1 FROM room_members WHERE room_id=? AND user_id=?", (room_id, user_id)
        )
        if existing:
            return "تو از قبل عضو همین روم هستی."
        cur_u = db.fetchone("SELECT current_room_id FROM users WHERE user_id=?", (user_id,))
        if cur_u and cur_u["current_room_id"] and cur_u["current_room_id"] != room_id:
            r2 = db.fetchone("SELECT status FROM rooms WHERE room_id=?", (cur_u["current_room_id"],))
            still = db.fetchone("SELECT 1 FROM room_members WHERE room_id=? AND user_id=?",
                                (cur_u["current_room_id"], user_id))
            if r2 and r2["status"] != "CLOSED" and still:
                return "اول باید از روم فعلی‌ت خارج بشی."
        cnt = room_member_count(room_id)
        if cnt >= room["capacity"]:
            return "ظرفیت این روم تکمیله."
        db.execute(
            "INSERT INTO room_members(room_id,user_id,join_order,joined_at) VALUES (?,?,?,?)",
            (room_id, user_id, cnt, now()),
        )
    await enter_room_flow(user_id, display_name, room_id)
    await after_join_hook(room_id)
    return None


async def leave_room(user_id: str, display_name: str):
    row = db.fetchone("SELECT current_room_id FROM users WHERE user_id=?", (user_id,))
    room_id = row["current_room_id"] if row else None
    if not room_id:
        await send_message(user_id, "تو الان توی هیچ رومی نیستی.", keyboard=MAIN_MENU)
        return
    # کاربرها هر وقت بخوان می‌تونن از روم خارج بشن، به‌جز دقیقاً وقتی که
    # نوبت خودشونه (چون وسط جواب‌دادن یا ضبط جرئته و خروج باعث گیرکردن
    # نوبت تا پایان تایمر می‌شه). این بررسی سمت سروره، نه فقط مخفی‌کردن دکمه.
    active_turn = db.fetchone(
        "SELECT 1 FROM turns WHERE room_id=? AND user_id=? AND status IN ('WAITING_CHOICE','WAITING_ANSWER')",
        (room_id, user_id),
    )
    if active_turn:
        await send_message(
            user_id,
            "⛔️ الان نوبت خودته! تا پایان نوبت (جواب‌دادن یا تمام‌شدن تایمر) نمی‌تونی از روم خارج بشی.",
        )
        return
    room = db.fetchone("SELECT * FROM rooms WHERE room_id=?", (room_id,))
    if room and room["status"] == "IN_PROGRESS" and (room["game_type"] or "TOD") != "TOD":
        await generic_player_left(room_id, user_id)
        room = db.fetchone("SELECT * FROM rooms WHERE room_id=?", (room_id,))
    db.execute("DELETE FROM room_members WHERE room_id=? AND user_id=?", (room_id, user_id))
    db.execute("UPDATE users SET current_room_id=NULL WHERE user_id=?", (user_id,))
    remaining = room_members(room_id)
    if not remaining:
        db.execute("UPDATE rooms SET status='CLOSED' WHERE room_id=?", (room_id,))
    elif room and room["owner_id"] == user_id:
        new_owner = remaining[0]
        db.execute("UPDATE rooms SET owner_id=? WHERE room_id=?", (new_owner["user_id"], room_id))
        await send_message(new_owner["user_id"], "👑 چون گاد قبلی خارج شد، مالکیت روم به تو منتقل شد!")
        await broadcast(room_id, f"👑 مالکیت روم به {new_owner['display_name']} منتقل شد.")
    await broadcast(room_id, f"🚪 {display_name} از روم خارج شد.")
    await send_message(user_id, "از روم خارج شدی.", keyboard=MAIN_MENU)


def pick_random_question(qtype: str, exclude_ids: list) -> Optional[sqlite3.Row]:
    placeholders = ",".join("?" for _ in exclude_ids) if exclude_ids else None
    if placeholders:
        rows = db.fetchall(
            f"SELECT * FROM questions WHERE qtype=? AND active=1 AND question_id NOT IN ({placeholders})",
            (qtype, *exclude_ids),
        )
    else:
        rows = db.fetchall("SELECT * FROM questions WHERE qtype=? AND active=1", (qtype,))
    if not rows:
        return None
    return random.choice(rows)


async def start_game(room_id: str, requester_id: str):
    room = db.fetchone("SELECT * FROM rooms WHERE room_id=?", (room_id,))
    if (room["game_type"] or "TOD") != "TOD":
        await start_generic_game(room_id, requester_id)
        return
    if room["owner_id"] != requester_id:
        await send_message(requester_id, "❌ فقط گاد روم می‌تونه بازی رو شروع کنه.")
        return
    if room["status"] == "IN_PROGRESS":
        await send_message(requester_id, "❌ یک بازی در همین روم در حال اجراست.")
        return
    cnt = room_member_count(room_id)
    if cnt < MIN_PLAYERS_TO_START:
        await send_message(
            requester_id,
            f"❌ برای شروع بازی حداقل {MIN_PLAYERS_TO_START} بازیکن لازمه (الان {cnt} نفرید).",
        )
        return
    game_id = new_id("game")
    db.execute(
        "INSERT INTO games(game_id, room_id, started_at) VALUES (?,?,?)",
        (game_id, room_id, now()),
    )
    db.execute(
        "UPDATE rooms SET status='IN_PROGRESS', accepting_new=0, current_turn_order=0, "
        "current_game_id=? WHERE room_id=?",
        (game_id, room_id),
    )
    await broadcast(
        room_id,
        f"🚀 بازی شروع شد!\n\n🏷 روم: {room['name']}\n👥 تعداد بازیکنان: {cnt}\n"
        f"آماده باشین؛ نوبت‌ها شروع می‌شن!",
    )
    await begin_next_turn(room_id)


async def begin_next_turn(room_id: str):
    room = db.fetchone("SELECT * FROM rooms WHERE room_id=?", (room_id,))
    if room is None or room["status"] != "IN_PROGRESS":
        return
    members = room_members(room_id)
    if not members:
        db.execute("UPDATE rooms SET status='CLOSED' WHERE room_id=?", (room_id,))
        return
    if len(members) < MIN_PLAYERS_TO_START:
        await end_game(room_id, reason="تعداد بازیکنان فعال کمتر از حد مجاز شد.")
        return

    order = room["current_turn_order"] % len(members)
    member = members[order]
    db.execute("UPDATE rooms SET current_turn_order=? WHERE room_id=?", (order + 1, room_id))

    turn_id = new_id("turn")
    deadline = now() + TURN_TIMEOUT_SECONDS
    db.execute(
        "INSERT INTO turns(turn_id, game_id, room_id, user_id, status, started_at, deadline) "
        "VALUES (?,?,?,?,?,?,?)",
        (turn_id, room["current_game_id"], room_id, member["user_id"], "WAITING_CHOICE", now(), deadline),
    )

    await broadcast_turn_status(
        turn_id, room_id,
        f"🎯 نوبت: {member['display_name']}\n⏳ منتظر انتخابت هستیم! نوع سؤال خودت رو انتخاب کن.",
    )
    await send_message(member["user_id"], "نوبت توئه! یکی رو انتخاب کن:", keyboard=TRUTH_DARE_MENU)

    schedule_turn_timeout(turn_id, TURN_TIMEOUT_SECONDS)


# نگهداری تایمرهای فعال در حافظه (برای لغو سریع). منبع حقیقت واقعی
# برای بازیابی بعد از ری‌استارت، ستون deadline در جدول turns است.
ACTIVE_TIMERS: dict = {}


def schedule_turn_timeout(turn_id: str, delay_seconds: int):
    old = ACTIVE_TIMERS.pop(turn_id, None)
    if old and not old.done():
        old.cancel()
    task = asyncio.ensure_future(_turn_timeout_watcher(turn_id, delay_seconds))
    ACTIVE_TIMERS[turn_id] = task


async def _turn_timeout_watcher(turn_id: str, delay_seconds: int):
    try:
        await asyncio.sleep(max(0, delay_seconds))
    except asyncio.CancelledError:
        return
    await expire_turn_if_still_pending(turn_id)


async def expire_turn_if_still_pending(turn_id: str):
    async with db.lock:
        turn = db.fetchone("SELECT * FROM turns WHERE turn_id=?", (turn_id,))
        if turn is None or turn["status"] in ("DONE", "EXPIRED"):
            return  # قبلاً تمام شده؛ از ثبت دوباره جلوگیری می‌شود
        if now() < turn["deadline"] - 1:
            return  # هنوز زمان نرسیده (محافظت در برابر فراخوانی زودهنگام)
        db.execute("UPDATE turns SET status='EXPIRED' WHERE turn_id=?", (turn_id,))
        room_id = turn["room_id"]
        db.execute(
            "UPDATE room_members SET turns_missed = turns_missed + 1 WHERE room_id=? AND user_id=?",
            (room_id, turn["user_id"]),
        )
        db.execute(
            "UPDATE users SET turns_missed = turns_missed + 1 WHERE user_id=?", (turn["user_id"],)
        )
        db.execute(
            "UPDATE games SET turns_skipped = turns_skipped + 1 WHERE game_id=?", (turn["game_id"],)
        )
    user = db.fetchone("SELECT display_name FROM users WHERE user_id=?", (turn["user_id"],))
    await broadcast_turn_status(
        turn["turn_id"], turn["room_id"],
        f"⏰ زمان {user['display_name']} تموم شد!\nچون در {TURN_TIMEOUT_SECONDS} ثانیه پاسخی ثبت نشد، نوبتش رد شد.\n➡️ نوبت بازیکن بعدی...",
    )
    await send_message(turn["user_id"], "زمانت تموم شد و نوبتت رد شد.", keyboard=ROOM_MENU)
    await begin_next_turn(turn["room_id"])


async def handle_truth_dare_choice(user_id: str, display_name: str, choice: str):
    turn = db.fetchone(
        "SELECT * FROM turns WHERE user_id=? AND status='WAITING_CHOICE' ORDER BY started_at DESC LIMIT 1",
        (user_id,),
    )
    if not turn:
        await send_message(user_id, "الان نوبت فعالی برای تو ثبت نشده.")
        return
    qtype = "TRUTH" if choice == "حقیقت" else "DARE"
    q = pick_random_question(qtype, exclude_ids=[])
    if q is None:
        await send_message(
            user_id,
            "⚠️ بانک سؤال‌های این نوع فعلاً خالیه. لطفاً به گاد یا مالک ربات اطلاع بده.",
        )
        # بازی را در وضعیت قابل‌بازیابی نگه می‌داریم؛ نوبت را منقضی نمی‌کنیم،
        # اما برای جلوگیری از گیرکردن بازی، به نوبت بعد می‌رویم.
        db.execute("UPDATE turns SET status='EXPIRED' WHERE turn_id=?", (turn["turn_id"],))
        await begin_next_turn(turn["room_id"])
        return

    new_deadline = now() + TURN_TIMEOUT_SECONDS
    db.execute(
        "UPDATE turns SET qtype=?, question_id=?, status='WAITING_ANSWER', deadline=? WHERE turn_id=?",
        (qtype, q["question_id"], new_deadline, turn["turn_id"]),
    )
    schedule_turn_timeout(turn["turn_id"], TURN_TIMEOUT_SECONDS)

    if qtype == "TRUTH":
        await broadcast_turn_status(
            turn["turn_id"], turn["room_id"],
            f"🎯 نوبت: {display_name}\n❓ نوع سؤال: حقیقت\n💬 سؤال:\n«{q['text']}»\n\n"
            f"⏳ منتظر پاسخت هستیم... ({TURN_TIMEOUT_SECONDS} ثانیه)",
        )
    else:
        await broadcast_turn_status(
            turn["turn_id"], turn["room_id"],
            f"🎯 نوبت: {display_name}\n🔥 نوع سؤال: جرئت\n💪 چالش:\n«{q['text']}»\n\n"
            f"منتظر انجام جرئت هستیم (عکس/ویدیو/ویس/ویدیو مسیج). ⏳ {TURN_TIMEOUT_SECONDS} ثانیه",
        )
    await send_message(
        user_id,
        "می‌تونی «رد کردن سؤال» رو هم بزنی (محدود به یک‌بار در هر نوبت).",
        keyboard=[["رد کردن سؤال"]],
    )


async def handle_skip_question(user_id: str, display_name: str):
    turn = db.fetchone(
        "SELECT * FROM turns WHERE user_id=? AND status='WAITING_ANSWER' ORDER BY started_at DESC LIMIT 1",
        (user_id,),
    )
    if not turn:
        await send_message(user_id, "الان سؤال فعالی برای رد کردن نداری.")
        return
    if turn["skips_used"] >= MAX_SKIPS_PER_TURN:
        await send_message(user_id, "❌ تعداد دفعات مجاز رد کردن سؤال در این نوبت تموم شده.")
        return
    q = pick_random_question(turn["qtype"], exclude_ids=[turn["question_id"]])
    if q is None:
        await send_message(user_id, "⚠️ سؤال جایگزینی در این دسته موجود نیست.")
        return
    db.execute(
        "UPDATE turns SET question_id=?, skips_used=skips_used+1 WHERE turn_id=?",
        (q["question_id"], turn["turn_id"]),
    )
    label = "سؤال" if turn["qtype"] == "TRUTH" else "جرئت"
    emoji = "❓" if turn["qtype"] == "TRUTH" else "🔥"
    tail = (
        f"⏳ منتظر پاسخت هستیم... ({TURN_TIMEOUT_SECONDS} ثانیه)"
        if turn["qtype"] == "TRUTH"
        else f"منتظر انجام جرئت هستیم (عکس/ویدیو/ویس/ویدیو مسیج). ⏳ {TURN_TIMEOUT_SECONDS} ثانیه"
    )
    await broadcast_turn_status(
        turn["turn_id"], turn["room_id"],
        f"🎯 نوبت: {display_name}\n{emoji} نوع سؤال: {'حقیقت' if turn['qtype']=='TRUTH' else 'جرئت'}\n"
        f"💬 سؤال:\n«{q['text']}» (رد و جایگزین شد)\n\n{tail}",
    )


async def handle_truth_answer(user_id: str, display_name: str, text: str):
    turn = db.fetchone(
        "SELECT * FROM turns WHERE user_id=? AND status='WAITING_ANSWER' AND qtype='TRUTH' "
        "ORDER BY started_at DESC LIMIT 1",
        (user_id,),
    )
    if not turn:
        return False  # این پیام متعلق به یک پاسخ حقیقت فعال نیست
    async with db.lock:
        fresh = db.fetchone("SELECT status FROM turns WHERE turn_id=?", (turn["turn_id"],))
        if fresh["status"] != "WAITING_ANSWER":
            return True  # دیر رسید؛ تایمر زودتر منقضی کرده
        db.execute("UPDATE turns SET status='DONE' WHERE turn_id=?", (turn["turn_id"],))
        db.execute(
            "INSERT OR REPLACE INTO answers(turn_id,user_id,answer_text,submitted_at) VALUES (?,?,?,?)",
            (turn["turn_id"], user_id, text, now()),
        )
        db.execute(
            "UPDATE room_members SET turns_taken = turns_taken + 1 WHERE room_id=? AND user_id=?",
            (turn["room_id"], user_id),
        )
        db.execute(
            "UPDATE users SET truths_answered = truths_answered + 1, score = score + ? WHERE user_id=?",
            (POINTS_TRUTH, user_id),
        )
        db.execute(
            "UPDATE games SET turns_done = turns_done + 1 WHERE game_id=?", (turn["game_id"],)
        )
    old_timer = ACTIVE_TIMERS.pop(turn["turn_id"], None)
    if old_timer and not old_timer.done():
        old_timer.cancel()
    q = db.fetchone("SELECT text FROM questions WHERE question_id=?", (turn["question_id"],))
    await broadcast_turn_status(
        turn["turn_id"], turn["room_id"],
        f"🎯 نوبت: {display_name}\n❓ نوع سؤال: حقیقت\n💬 سؤال:\n{q['text']}\n\n"
        f"✅ پاسخ:\n{text}\n\n🎉 نوبت با موفقیت به پایان رسید!",
    )
    await begin_next_turn(turn["room_id"])
    return True


MEDIA_TYPE_FA = {"photo": "عکس", "video": "ویدیو", "voice": "ویس", "video_note": "ویدیو مسیج"}


async def handle_dare_media(user_id: str, display_name: str, media_type: str, file_id: str):
    turn = db.fetchone(
        "SELECT * FROM turns WHERE user_id=? AND status='WAITING_ANSWER' AND qtype='DARE' "
        "ORDER BY started_at DESC LIMIT 1",
        (user_id,),
    )
    if not turn:
        return False
    async with db.lock:
        fresh = db.fetchone("SELECT status FROM turns WHERE turn_id=?", (turn["turn_id"],))
        if fresh["status"] != "WAITING_ANSWER":
            return True
        db.execute("UPDATE turns SET status='DONE' WHERE turn_id=?", (turn["turn_id"],))
        db.execute(
            "INSERT OR REPLACE INTO answers(turn_id,user_id,media_file_id,media_type,submitted_at) "
            "VALUES (?,?,?,?,?)",
            (turn["turn_id"], user_id, file_id, media_type, now()),
        )
        db.execute(
            "UPDATE room_members SET turns_taken = turns_taken + 1 WHERE room_id=? AND user_id=?",
            (turn["room_id"], user_id),
        )
        db.execute(
            "UPDATE users SET dares_done = dares_done + 1, score = score + ? WHERE user_id=?",
            (POINTS_DARE, user_id),
        )
        db.execute(
            "UPDATE games SET turns_done = turns_done + 1 WHERE game_id=?", (turn["game_id"],)
        )
    old_timer = ACTIVE_TIMERS.pop(turn["turn_id"], None)
    if old_timer and not old_timer.done():
        old_timer.cancel()
    q = db.fetchone("SELECT text FROM questions WHERE question_id=?", (turn["question_id"],))
    # رسانه نمی‌تونه داخل یک پیام متنی ویرایش بشه (محدودیت پلتفرم)، پس
    # خودش به‌صورت یک پیام جدا می‌ره؛ ولی متن وضعیت نوبت (که قبلاً برای
    # همه فرستاده شده بود) رو ویرایش می‌کنیم تا نتیجه در همون حباب دیده
    # بشه و صفحه شلوغ نشه.
    await broadcast_turn_status(
        turn["turn_id"], turn["room_id"],
        f"🎯 نوبت: {display_name}\n🔥 جرئت:\n«{q['text']}»\n\n"
        f"📎 نتیجه ({MEDIA_TYPE_FA.get(media_type,'رسانه')}) در پیام بعدی 👇\n\n"
        f"🎉 نوبت با موفقیت به پایان رسید!",
    )
    await broadcast_media(turn["room_id"], media_type, file_id, caption=f"از طرف {display_name}")
    await begin_next_turn(turn["room_id"])
    return True


async def end_game(room_id: str, reason: str, by_user: Optional[str] = None):
    room = db.fetchone("SELECT * FROM rooms WHERE room_id=?", (room_id,))
    if room is None or room["status"] != "IN_PROGRESS":
        return
    if (room["game_type"] or "TOD") != "TOD":
        await end_generic_game(room_id, reason)
        return
    game = db.fetchone("SELECT * FROM games WHERE game_id=?", (room["current_game_id"],))
    db.execute(
        "UPDATE games SET ended_at=? WHERE game_id=?", (now(), game["game_id"])
    )
    members = room_members(room_id)
    for m in members:
        db.execute(
            "UPDATE users SET games_played = games_played + 1, score = score + ? WHERE user_id=?",
            (POINTS_PARTICIPATION, m["user_id"]),
        )
    db.execute(
        "UPDATE rooms SET status='OPEN', accepting_new=1, current_game_id=NULL WHERE room_id=?",
        (room_id,),
    )
    # رتبه‌بندی براساس امتیاز فعلی کاربران (کل امتیاز حساب، نه فقط این بازی —
    # چون امتیاز فی‌الواقع تجمعیه؛ برای این بازی، نوبت‌های گرفته‌شده را مبنا می‌گیریم)
    scored = sorted(members, key=lambda m: m["turns_taken"], reverse=True)
    top_score = scored[0]["turns_taken"] if scored else 0
    winners = [m["display_name"] for m in scored if m["turns_taken"] == top_score and top_score > 0]
    winner_text = "، ".join(winners) if winners else "—"
    await broadcast(
        room_id,
        f"🏁 بازی به پایان رسید!\n\n"
        f"🏷 روم: {room['name']}\n"
        f"👥 تعداد بازیکنان: {len(members)}\n"
        f"✅ تعداد نوبت‌های انجام‌شده: {game['turns_done']}\n"
        f"⏰ تعداد نوبت‌های ردشده: {game['turns_skipped']}\n"
        f"🏆 بازیکن برتر: {winner_text}\n\n"
        f"ممنون که بازی کردین! 🎉\n\n(دلیل پایان: {reason})",
    )
    for m in members:
        await send_message(m["user_id"], "برگشتی به منوی روم:", keyboard=ROOM_MENU)
    # لغو تایمرهای باقیمانده این روم
    for turn in db.fetchall(
        "SELECT turn_id FROM turns WHERE room_id=? AND status IN ('WAITING_CHOICE','WAITING_ANSWER')",
        (room_id,),
    ):
        t = ACTIVE_TIMERS.pop(turn["turn_id"], None)
        if t and not t.done():
            t.cancel()
        db.execute("UPDATE turns SET status='EXPIRED' WHERE turn_id=?", (turn["turn_id"],))


# ============================== RECOVERY ON STARTUP =========================


async def recover_pending_turns():
    """پس از ری‌استارت، نوبت‌های نیمه‌کاره را بازیابی می‌کند: اگر مهلت
    گذشته، همان لحظه منقضی می‌شوند؛ وگرنه با زمان باقیمانده واقعی دوباره
    زمان‌بندی می‌شوند."""
    rows = db.fetchall(
        "SELECT * FROM turns WHERE status IN ('WAITING_CHOICE','WAITING_ANSWER')"
    )
    for t in rows:
        remaining = t["deadline"] - now()
        schedule_turn_timeout(t["turn_id"], max(0, remaining))
    if rows:
        log.info("بازیابی %d نوبت فعال بعد از ری‌استارت انجام شد.", len(rows))


# ============================== ADMIN PANEL =================================

ADMIN_MENU = [
    ["➕ افزودن جرئت", "➕ افزودن حقیقت"],
    ["📋 لیست کاربران", "📈 آمار کلی ربات"],
    ["🗂 مدیریت سؤالات", "🏠 مدیریت روم‌ها"],
    ["❌ بستن پنل"],
]


def log_admin_action(actor_id: str, action: str, details: str = ""):
    db.execute(
        "INSERT INTO admin_logs(log_id, actor_id, action, details, created_at) VALUES (?,?,?,?,?)",
        (new_id("log"), actor_id, action, details, now()),
    )


async def admin_open_panel(user_id: str):
    if not is_owner(user_id):
        # هیچ اطلاعاتی دربارهٔ وجود پنل به کاربر عادی داده نمی‌شود
        await send_message(user_id, "این دستور شناخته‌شده نیست.")
        return
    await send_message(
        user_id,
        "🛠 پنل مدیریت ربات\n\nبه پنل مدیریت خوش آمدی. یکی از گزینه‌ها رو انتخاب کن.",
        keyboard=ADMIN_MENU,
    )


async def admin_stats(user_id: str):
    users_cnt = db.fetchone("SELECT COUNT(*) c FROM users")["c"]
    rooms_cnt = db.fetchone("SELECT COUNT(*) c FROM rooms WHERE status!='CLOSED'")["c"]
    games_cnt = db.fetchone("SELECT COUNT(*) c FROM games")["c"]
    truths_cnt = db.fetchone("SELECT COUNT(*) c FROM questions WHERE qtype='TRUTH' AND active=1")["c"]
    dares_cnt = db.fetchone("SELECT COUNT(*) c FROM questions WHERE qtype='DARE' AND active=1")["c"]
    await send_message(
        user_id,
        f"📈 آمار کلی ربات\n\n"
        f"👤 کاربران ثبت‌شده: {users_cnt}\n"
        f"🏠 روم‌های فعال/باز: {rooms_cnt}\n"
        f"🎮 بازی‌های انجام‌شده: {games_cnt}\n"
        f"💬 سؤالات حقیقت فعال: {truths_cnt}\n"
        f"🔥 جرئت‌های فعال: {dares_cnt}",
        keyboard=ADMIN_MENU,
    )


USERS_PAGE_SIZE = 20


async def admin_list_users(user_id: str, page: int = 0):
    offset = page * USERS_PAGE_SIZE
    rows = db.fetchall(
        "SELECT * FROM users ORDER BY first_seen DESC LIMIT ? OFFSET ?",
        (USERS_PAGE_SIZE, offset),
    )
    total = db.fetchone("SELECT COUNT(*) c FROM users")["c"]
    if not rows:
        await send_message(user_id, "کاربری در این صفحه نیست.", keyboard=ADMIN_MENU)
        return
    lines = [f"📋 لیست کاربران (صفحه {page+1} — کل: {total})\n"]
    for r in rows:
        status = "آزاد"
        if r["current_room_id"]:
            room = db.fetchone("SELECT status FROM rooms WHERE room_id=?", (r["current_room_id"],))
            status = "در حال بازی" if room and room["status"] == "IN_PROGRESS" else "داخل روم"
        lines.append(
            f"• {r['display_name']} | {r['user_id']} | بازی‌ها:{r['games_played']} "
            f"| نوبت‌های ازدست‌رفته:{r['turns_missed']} | وضعیت: {status}"
        )
    set_state(user_id, {"flow": "admin_users_page", "page": page})
    await send_message(
        user_id,
        "\n".join(lines),
        keyboard=[["⬅️ صفحه قبل", "صفحه بعد ➡️"], ["❌ بستن پنل"]],
    )


async def admin_manage_rooms(user_id: str):
    rows = db.fetchall(
        "SELECT r.*, (SELECT COUNT(*) FROM room_members rm WHERE rm.room_id=r.room_id) cnt "
        "FROM rooms r WHERE status != 'CLOSED' ORDER BY created_at DESC LIMIT 30"
    )
    if not rows:
        await send_message(user_id, "در حال حاضر رومی فعال نیست.", keyboard=ADMIN_MENU)
        return
    lines = ["🏠 روم‌های فعال:\n"]
    for r in rows:
        type_fa = "عمومی" if r["room_type"] == "PUBLIC" else "خصوصی"
        lines.append(f"• {r['name']} | {type_fa} | وضعیت:{r['status']} | بازیکنان:{r['cnt']} | کد:{r['invite_code']}")
    lines.append("\nبرای پایان‌دادن اجباری به یک بازی بنویس:\nپایان_اجباری <کد_روم>")
    await send_message(user_id, "\n".join(lines), keyboard=ADMIN_MENU)


# ============================== TEXT COMMAND ROUTER =========================


async def handle_update(update: dict):
    msg = update.get("message")
    if not msg:
        return
    chat = msg.get("chat", {}) or {}
    user_id = str(chat.get("id") or msg.get("from", {}).get("id"))
    if not user_id:
        return
    from_info = msg.get("from", {}) or {}
    display_name = from_info.get("first_name") or from_info.get("username") or f"کاربر {user_id}"

    get_or_create_user(user_id, display_name)

    text = msg.get("text")
    photo = msg.get("photo")
    video = msg.get("video")
    voice = msg.get("voice")
    video_note = msg.get("video_note")

    # --- رسانه (برای جرئت) ---
    if photo or video or voice or video_note:
        if photo:
            file_id = photo[-1]["file_id"] if isinstance(photo, list) else photo.get("file_id")
            handled = await handle_dare_media(user_id, display_name, "photo", file_id)
        elif video:
            handled = await handle_dare_media(user_id, display_name, "video", video.get("file_id"))
        elif voice:
            handled = await handle_dare_media(user_id, display_name, "voice", voice.get("file_id"))
        else:
            handled = await handle_dare_media(user_id, display_name, "video_note", video_note.get("file_id"))
        if not handled:
            await send_message(
                user_id,
                "الان نوبت فعالی برای ارسال رسانه نداری یا نوع سؤالت جرئت نیست.",
            )
        return

    if not text:
        return
    text = text.strip()

    # --- دستور پنل مالک ---
    if text == "پنل":
        await admin_open_panel(user_id)
        return

    state = get_state(user_id)

    # --- جریان‌های مرحله‌ای (ساخت روم، ورود، افزودن سؤال و ...) ---
    if state:
        handled = await handle_stateful_flow(user_id, display_name, text, state)
        if handled:
            return

    # --- دکمه‌های پنل مدیریت (فقط برای مالک معتبرند؛ بررسی داخل هر تابع) ---
    if text in (
        "➕ افزودن جرئت", "➕ افزودن حقیقت", "📋 لیست کاربران", "📈 آمار کلی ربات",
        "🗂 مدیریت سؤالات", "🏠 مدیریت روم‌ها", "❌ بستن پنل",
        "⬅️ صفحه قبل", "صفحه بعد ➡️",
    ):
        if not is_owner(user_id):
            await send_message(user_id, "دسترسی نداری.")
            return
        await route_admin_menu(user_id, text)
        return

    if text.startswith("پایان_اجباری"):
        if not is_owner(user_id):
            await send_message(user_id, "دسترسی نداری.")
            return
        parts = text.split()
        if len(parts) < 2:
            await send_message(user_id, "فرمت درست: پایان_اجباری <کد_روم>")
            return
        room = db.fetchone("SELECT * FROM rooms WHERE invite_code=?", (parts[1],))
        if not room:
            await send_message(user_id, "رومی با این کد پیدا نشد.")
            return
        log_admin_action(user_id, "FORCE_END_GAME", room["room_id"])
        await end_game(room["room_id"], reason="پایان اجباری توسط مالک ربات.")
        await send_message(user_id, "✅ انجام شد.", keyboard=ADMIN_MENU)
        return

    # --- منوی اصلی ---
    if text in ("/start", "start", "شروع"):
        await send_message(user_id, WELCOME, keyboard=MAIN_MENU)
        return

    if text == "📘 راهنمای بازی":
        await send_message(user_id, HELP_TEXT, keyboard=MAIN_MENU)
        return

    if text == "🙍 پروفایل من" or text == "📊 آمار بازی‌ها":
        u = db.fetchone("SELECT * FROM users WHERE user_id=?", (user_id,))
        rank_row = db.fetchone(
            "SELECT COUNT(*)+1 r FROM users WHERE score > ?", (u["score"],)
        )
        await send_message(
            user_id,
            f"🙍 پروفایل {u['display_name']}\n\n"
            f"🎮 بازی‌های شرکت‌کرده: {u['games_played']}\n"
            f"💬 پاسخ‌های حقیقت: {u['truths_answered']}\n"
            f"🔥 جرئت‌های انجام‌شده: {u['dares_done']}\n"
            f"⏰ نوبت‌های ردشده: {u['turns_missed']}\n"
            f"⭐ امتیاز کل: {u['score']}\n"
            f"🏅 رتبه: {rank_row['r']}",
            keyboard=MAIN_MENU,
        )
        return

    if text == "🎯 ساخت روم":
        set_state(user_id, {"flow": "create_room_name"})
        await send_message(user_id, "✏️ اسم روم خودت رو وارد کن:\nیه اسم جذاب برای بازی انتخاب کن!")
        return

    if text == "🔑 ورود به روم":
        set_state(user_id, {"flow": "join_room_code"})
        await send_message(user_id, "🔑 کد دعوت روم رو بفرست تا وارد بازی بشی.")
        return

    if text == "🎲 روم رندوم":
        await handle_random_room(user_id, display_name)
        return

    if text == "👥 دوستان":
        await show_friends_menu(user_id)
        return

    # --- داخل روم ---
    current_room = db.fetchone("SELECT current_room_id FROM users WHERE user_id=?", (user_id,))
    room_id = current_room["current_room_id"] if current_room else None

    if text == "▶️ شروع بازی" and room_id:
        await start_game(room_id, user_id)
        return
    if text == "👥 بازیکنان روم" and room_id:
        await show_room_players(user_id, room_id)
        return
    if text == "🚪 خروج از روم" and room_id:
        await leave_room(user_id, display_name)
        return
    if text == "⚙️ تنظیمات روم" and room_id:
        await show_room_settings(user_id, room_id)
        return
    if text == "➕ دعوت از دوستان" and room_id:
        room = db.fetchone("SELECT invite_code, name FROM rooms WHERE room_id=?", (room_id,))
        await send_message(
            user_id,
            f"📨 این کد رو به دوستات بده تا وارد روم «{room['name']}» بشن:\n\n🔑 {room['invite_code']}",
        )
        return

    if text == "حقیقت" or text == "جرئت":
        await handle_truth_dare_choice(user_id, display_name, text)
        return
    if text == "رد کردن سؤال":
        await handle_skip_question(user_id, display_name)
        return

    # --- اگر نوبت حقیقت فعالی برای این کاربر هست، این متن را پاسخ در نظر بگیر ---
    handled = await handle_truth_answer(user_id, display_name, text)
    if handled:
        return

    # هیچ‌کدام از موارد بالا نبود
    await send_message(user_id, "متوجه نشدم 🤔 از منو استفاده کن:", keyboard=MAIN_MENU)


async def route_admin_menu(user_id: str, text: str):
    if text == "➕ افزودن جرئت":
        set_state(user_id, {"flow": "admin_add_question", "qtype": "DARE"})
        await send_message(user_id, "✏️ متن جرئت جدید رو ارسال کن:\nاین سؤال به بانک جرئت‌ها اضافه می‌شه.")
    elif text == "➕ افزودن حقیقت":
        set_state(user_id, {"flow": "admin_add_question", "qtype": "TRUTH"})
        await send_message(user_id, "✏️ سؤال حقیقت جدید رو ارسال کن:\nسؤال باید واضح و مناسب بازی باشه.")
    elif text == "📋 لیست کاربران":
        await admin_list_users(user_id, 0)
    elif text == "📈 آمار کلی ربات":
        await admin_stats(user_id)
    elif text == "🗂 مدیریت سؤالات":
        t_cnt = db.fetchone("SELECT COUNT(*) c FROM questions WHERE qtype='TRUTH' AND active=1")["c"]
        d_cnt = db.fetchone("SELECT COUNT(*) c FROM questions WHERE qtype='DARE' AND active=1")["c"]
        await send_message(
            user_id,
            f"🗂 سؤالات فعال:\n💬 حقیقت: {t_cnt}\n🔥 جرئت: {d_cnt}\n\n"
            f"برای غیرفعال‌کردن یک سؤال با شناسه مشخص، به مالک پروژه (توسعه‌دهنده) اطلاع بده تا "
            f"دستور متنی حذف نرم تکمیل بشه.",
            keyboard=ADMIN_MENU,
        )
    elif text == "🏠 مدیریت روم‌ها":
        await admin_manage_rooms(user_id)
    elif text == "❌ بستن پنل":
        set_state(user_id, None)
        await send_message(user_id, "پنل بسته شد.", keyboard=MAIN_MENU)
    elif text in ("⬅️ صفحه قبل", "صفحه بعد ➡️"):
        st = get_state(user_id) or {"page": 0}
        page = st.get("page", 0)
        page = max(0, page - 1) if text.startswith("⬅️") else page + 1
        await admin_list_users(user_id, page)


async def handle_stateful_flow(user_id: str, display_name: str, text: str, state: dict) -> bool:
    flow = state.get("flow")

    if flow == "create_room_name":
        name = text.strip()
        if not name:
            await send_message(user_id, "اسم روم نمی‌تونه خالی باشه. دوباره بفرست:")
            return True
        if len(name) > MAX_ROOM_NAME_LEN:
            await send_message(user_id, f"اسم روم حداکثر {MAX_ROOM_NAME_LEN} کاراکتر می‌تونه باشه. دوباره بفرست:")
            return True
        set_state(user_id, {"flow": "create_room_type", "name": name})
        await send_message(
            user_id,
            "🔒 دوست داری رومت چه نوعی باشه؟",
            keyboard=[["🌍 روم عمومی", "🔐 روم خصوصی"], ["لغو ساخت روم"]],
        )
        return True

    if flow == "create_room_type":
        if text == "لغو ساخت روم":
            set_state(user_id, None)
            await send_message(user_id, "ساخت روم لغو شد.", keyboard=MAIN_MENU)
            return True
        if text not in ("🌍 روم عمومی", "🔐 روم خصوصی"):
            await send_message(user_id, "یکی از گزینه‌ها رو انتخاب کن.")
            return True
        room_type = "PUBLIC" if "عمومی" in text else "PRIVATE"
        room_id = new_id("room")
        invite_code = gen_invite_code()
        db.execute(
            "INSERT INTO rooms(room_id,name,room_type,owner_id,status,capacity,invite_code,"
            "accepting_new,created_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (room_id, state["name"], room_type, user_id, "OPEN", DEFAULT_ROOM_CAPACITY,
             invite_code, 1, now()),
        )
        db.execute(
            "INSERT INTO room_members(room_id,user_id,join_order,joined_at) VALUES (?,?,?,?)",
            (room_id, user_id, 0, now()),
        )
        db.execute("UPDATE users SET current_room_id=? WHERE user_id=?", (room_id, user_id))
        set_state(user_id, None)
        await send_message(user_id, build_room_summary(room_id), keyboard=ROOM_MENU)
        return True

    if flow == "join_room_code":
        code = text.strip().upper()
        room = db.fetchone("SELECT * FROM rooms WHERE invite_code=?", (code,))
        set_state(user_id, None)
        if not room:
            await send_message(user_id, "❌ کد دعوت نامعتبره.", keyboard=MAIN_MENU)
            return True
        err = await try_join_room(user_id, display_name, room["room_id"])
        if err:
            await send_message(user_id, f"❌ {err}", keyboard=MAIN_MENU)
        return True

    if flow == "admin_add_question":
        if not is_owner(user_id):
            set_state(user_id, None)
            return True
        qtype = state["qtype"]
        qid = new_id("q")
        db.execute(
            "INSERT INTO questions(question_id,qtype,text,category,difficulty,active,created_by,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (qid, qtype, text.strip(), "عمومی", "متوسط", 1, user_id, now()),
        )
        log_admin_action(user_id, f"ADD_{qtype}", qid)
        set_state(user_id, None)
        label = "جرئت" if qtype == "DARE" else "سؤال حقیقت"
        await send_message(
            user_id,
            f"✅ {label} جدید با موفقیت اضافه شد!",
            keyboard=[[f"➕ افزودن {'جرئت' if qtype=='DARE' else 'حقیقت'} دیگر", "🗂 مدیریت سؤالات"], ["❌ بستن پنل"]],
        )
        return True

    if flow == "room_settings_rename":
        room_id = state["room_id"]
        new_name = text.strip()
        if not new_name or len(new_name) > MAX_ROOM_NAME_LEN:
            await send_message(user_id, f"اسم نامعتبره (حداکثر {MAX_ROOM_NAME_LEN} کاراکتر).")
            return True
        db.execute("UPDATE rooms SET name=? WHERE room_id=?", (new_name, room_id))
        set_state(user_id, None)
        await send_message(user_id, "✅ نام روم تغییر کرد.", keyboard=ROOM_MENU)
        return True

    if flow == "room_settings_capacity":
        room_id = state["room_id"]
        _r = db.fetchone("SELECT game_type FROM rooms WHERE room_id=?", (room_id,))
        _cfg = GAME_TYPES[(_r["game_type"] or "TOD")]
        if (not text.isdigit() or int(text) < room_member_count(room_id)
                or int(text) > _cfg["max"] or int(text) < _cfg["min"]):
            await send_message(user_id, f"عدد نامعتبره. برای این بازی ظرفیت باید بین {_cfg['min']} تا {_cfg['max']} و "
                                        f"نه کمتر از تعداد فعلی بازیکنان باشه.")
            return True
        db.execute("UPDATE rooms SET capacity=? WHERE room_id=?", (int(text), room_id))
        set_state(user_id, None)
        await send_message(user_id, "✅ ظرفیت روم به‌روزرسانی شد.", keyboard=ROOM_MENU)
        return True

    if flow == "friend_add":
        target_id = text.strip()
        set_state(user_id, None)
        if target_id == user_id:
            await send_message(user_id, "نمی‌تونی خودت رو دوست اضافه کنی 🙂", keyboard=MAIN_MENU)
            return True
        target = db.fetchone("SELECT * FROM users WHERE user_id=?", (target_id,))
        if not target:
            await send_message(user_id, "کاربری با این شناسه پیدا نشد (باید قبلاً با ربات شروع کرده باشه).", keyboard=MAIN_MENU)
            return True
        a, b = sorted([user_id, target_id])
        existing = db.fetchone("SELECT * FROM friendships WHERE user_a=? AND user_b=?", (a, b))
        if existing and existing["status"] == "ACCEPTED":
            await send_message(user_id, "قبلاً با این کاربر دوست هستی.", keyboard=MAIN_MENU)
            return True
        if existing and existing["status"] == "PENDING":
            if existing["requested_by"] != user_id:
                db.execute(
                    "UPDATE friendships SET status='ACCEPTED' WHERE user_a=? AND user_b=?", (a, b)
                )
                await send_message(user_id, "✅ درخواست دوستی تأیید شد!", keyboard=MAIN_MENU)
                await send_message(target_id, f"✅ {display_name} درخواست دوستی‌ت رو تأیید کرد!")
            else:
                await send_message(user_id, "درخواست قبلاً ارسال شده؛ منتظر تأیید طرف مقابل باش.", keyboard=MAIN_MENU)
            return True
        db.execute(
            "INSERT INTO friendships(user_a,user_b,status,requested_by,created_at) VALUES (?,?,?,?,?)",
            (a, b, "PENDING", user_id, now()),
        )
        await send_message(user_id, "📨 درخواست دوستی ارسال شد؛ منتظر تأیید باش.", keyboard=MAIN_MENU)
        await send_message(
            target_id,
            f"📨 {display_name} ({user_id}) درخواست دوستی فرستاد.\nبرای تأیید، از منوی «دوستان» → «افزودن دوست» همین شناسه رو بفرست: {user_id}",
        )
        return True

    return False


async def handle_random_room(user_id: str, display_name: str):
    rows = db.fetchall(
        "SELECT r.*, (SELECT COUNT(*) FROM room_members rm WHERE rm.room_id=r.room_id) cnt "
        "FROM rooms r WHERE room_type='PUBLIC' AND status='OPEN' AND accepting_new=1"
    )
    candidates = [r for r in rows if r["cnt"] < r["capacity"]]
    if not candidates:
        await send_message(
            user_id,
            "😕 فعلاً روم عمومی مناسبی پیدا نشد!\nمی‌تونی یه روم بسازی یا چند لحظه دیگه دوباره امتحان کنی.",
            keyboard=[["🔁 جست‌وجوی دوباره", "🎯 ساخت روم"], ["بازگشت"]],
        )
        return
    choice = random.choice(candidates)
    err = await try_join_room(user_id, display_name, choice["room_id"])
    if err:
        await send_message(user_id, f"❌ {err}", keyboard=MAIN_MENU)


async def show_room_players(user_id: str, room_id: str):
    members = room_members(room_id)
    lines = [f"👥 بازیکنان روم\n"]
    for i, m in enumerate(members, start=1):
        lines.append(f"{i}. {m['display_name']}")
    lines.append(f"\nتعداد کل: {len(members)} نفر")
    await send_message(user_id, "\n".join(lines), keyboard=ROOM_MENU)


async def show_room_settings(user_id: str, room_id: str):
    room = db.fetchone("SELECT * FROM rooms WHERE room_id=?", (room_id,))
    if room["owner_id"] != user_id:
        await send_message(user_id, "فقط گاد روم می‌تونه تنظیمات رو تغییر بده.", keyboard=ROOM_MENU)
        return
    await send_message(
        user_id,
        f"⚙️ تنظیمات روم «{room['name']}»\n\n"
        f"ظرفیت: {room['capacity']} | پذیرش بازیکن جدید: {'بله' if room['accepting_new'] else 'خیر'}\n"
        f"کد دعوت: {room['invite_code']}",
        keyboard=[
            ["✏️ تغییر نام روم", "🔢 تغییر ظرفیت"],
            ["🔄 لینک دعوت جدید", "⛔️ قفل/باز کردن ورود"],
            ["🏁 پایان بازی", "🗑 بستن روم"],
            ["بازگشت به روم"],
        ],
    )
    set_state(user_id, {"flow": "room_settings_menu", "room_id": room_id})


async def handle_room_settings_buttons(user_id: str, text: str, room_id: str) -> bool:
    room = db.fetchone("SELECT * FROM rooms WHERE room_id=?", (room_id,))
    if room is None or room["owner_id"] != user_id:
        return False
    if text == "✏️ تغییر نام روم":
        set_state(user_id, {"flow": "room_settings_rename", "room_id": room_id})
        await send_message(user_id, "نام جدید روم رو بفرست:")
        return True
    if text == "🔢 تغییر ظرفیت":
        set_state(user_id, {"flow": "room_settings_capacity", "room_id": room_id})
        await send_message(user_id, "ظرفیت جدید (عدد) رو بفرست:")
        return True
    if text == "🔄 لینک دعوت جدید":
        new_code = gen_invite_code()
        db.execute("UPDATE rooms SET invite_code=? WHERE room_id=?", (new_code, room_id))
        await send_message(user_id, f"✅ کد دعوت جدید: {new_code}\n(کد قبلی دیگه معتبر نیست.)", keyboard=ROOM_MENU)
        return True
    if text == "⛔️ قفل/باز کردن ورود":
        new_val = 0 if room["accepting_new"] else 1
        db.execute("UPDATE rooms SET accepting_new=? WHERE room_id=?", (new_val, room_id))
        await send_message(
            user_id,
            "✅ ورود بازیکن جدید الان " + ("باز" if new_val else "بسته") + " است.",
            keyboard=ROOM_MENU,
        )
        return True
    if text == "🏁 پایان بازی":
        if room["status"] == "IN_PROGRESS":
            await end_game(room_id, reason="گاد بازی رو متوقف کرد.")
        else:
            await send_message(user_id, "بازی‌ای در حال اجرا نیست.", keyboard=ROOM_MENU)
        return True
    if text == "🗑 بستن روم":
        for m in room_members(room_id):
            db.execute("UPDATE users SET current_room_id=NULL WHERE user_id=?", (m["user_id"],))
        db.execute("UPDATE rooms SET status='CLOSED' WHERE room_id=?", (room_id,))
        await broadcast(room_id, "🚫 این روم توسط گاد بسته شد.")
        set_state(user_id, None)
        await send_message(user_id, "روم بسته شد.", keyboard=MAIN_MENU)
        return True
    if text == "بازگشت به روم":
        set_state(user_id, None)
        await send_message(user_id, "برگشتی به منوی روم:", keyboard=ROOM_MENU)
        return True
    return False


async def show_friends_menu(user_id: str):
    friends = db.fetchall(
        "SELECT * FROM friendships WHERE (user_a=? OR user_b=?) AND status='ACCEPTED'",
        (user_id, user_id),
    )
    lines = ["👥 دوستان من\n"]
    if not friends:
        lines.append("هنوز دوستی اضافه نکردی.")
    for f in friends:
        other = f["user_b"] if f["user_a"] == user_id else f["user_a"]
        u = db.fetchone("SELECT display_name FROM users WHERE user_id=?", (other,))
        if u:
            lines.append(f"• {u['display_name']} ({other})")
    await send_message(
        user_id,
        "\n".join(lines),
        keyboard=[["➕ افزودن دوست", "🗑 حذف دوست"], ["📨 دعوت دوست به روم"], ["بازگشت"]],
    )
    set_state(user_id, {"flow": "friends_menu"})


# اتصال دکمه‌های تنظیمات روم و دوستان به روتر اصلی (چون بعضی دکمه‌ها
# نیاز به state دارند، این دو تابع را داخل handle_update فراخوانی می‌کنیم)
_original_handle_update = handle_update


async def handle_update(update: dict):  # noqa: F811  (override عمدی برای افزودن روتینگ state)
    msg = update.get("message")
    if not msg:
        return
    chat = msg.get("chat", {}) or {}
    user_id = str(chat.get("id") or msg.get("from", {}).get("id"))
    if not user_id or not msg.get("text"):
        await _original_handle_update(update)
        return
    text = msg["text"].strip()
    state = get_state(user_id)
    if state and state.get("flow") == "room_settings_menu":
        done = await handle_room_settings_buttons(user_id, text, state["room_id"])
        if done:
            return
    if state and state.get("flow") == "friends_menu":
        if text == "➕ افزودن دوست":
            set_state(user_id, {"flow": "friend_add"})
            await send_message(user_id, "شناسهٔ کاربری دوستت رو بفرست (باید قبلاً ربات رو استارت کرده باشه):")
            return
        if text == "🗑 حذف دوست":
            await send_message(user_id, "برای حذف، شناسهٔ کاربری دوست رو با پیشوند حذف_دوست بفرست.\nمثال: حذف_دوست 12345")
            set_state(user_id, None)
            return
        if text == "📨 دعوت دوست به روم":
            cur = db.fetchone("SELECT current_room_id FROM users WHERE user_id=?", (user_id,))
            if not cur or not cur["current_room_id"]:
                await send_message(user_id, "اول باید عضو یه روم باشی.", keyboard=MAIN_MENU)
                set_state(user_id, None)
                return
            room = db.fetchone("SELECT invite_code, name FROM rooms WHERE room_id=?", (cur["current_room_id"],))
            await send_message(user_id, f"این کد رو برای دوستت بفرست:\n🔑 {room['invite_code']} (روم: {room['name']})")
            set_state(user_id, None)
            return
        if text == "بازگشت":
            set_state(user_id, None)
            await send_message(user_id, WELCOME, keyboard=MAIN_MENU)
            return
    if text.startswith("حذف_دوست"):
        parts = text.split()
        if len(parts) == 2:
            target = parts[1]
            a, b = sorted([user_id, target])
            db.execute("DELETE FROM friendships WHERE user_a=? AND user_b=?", (a, b))
            await send_message(user_id, "دوست حذف شد.", keyboard=MAIN_MENU)
            return

    await _original_handle_update(update)


# ============================================================================
#                         ===  افزونهٔ سکه و بازی‌ها  ===
# ============================================================================

# ============================== GAME ENGINES ================================
# هر بازی یک موتور خالص (pure) است که روی یک دیکشنری JSON کار می‌کند.
# همهٔ اعتبارسنجی‌ها سمت سرور است؛ کلاینت وب فقط درخواست می‌فرستد.

WAIT_KB = [["📍 وضعیت"], ["🏳️ خروج از بازی"]]
LEAVE_ROW = ["🏳️ خروج از بازی"]


def _log(state: dict, events: list):
    state.setdefault("log", []).extend(events)
    del state["log"][:-25]


def _idx(state: dict, pid: str) -> int:
    for i, p in enumerate(state["players"]):
        if p["id"] == pid:
            return i
    return -1


def _active(state: dict) -> list:
    return [i for i, p in enumerate(state["players"]) if not p.get("left")]


def _pname(state: dict, i: int) -> str:
    return state["players"][i]["name"]


class BaseEngine:
    key = ""

    @classmethod
    def touch(cls, st: dict, seconds: int = None):
        st["deadline"] = now() + (seconds or AFK_SECONDS)

    @classmethod
    def deadline(cls, st):
        return st.get("deadline")

    @classmethod
    def is_over(cls, st) -> bool:
        return bool(st.get("over"))

    @classmethod
    def public_text(cls, st) -> str:
        return ""

    @classmethod
    def parse_common(cls, text: str):
        if text in ("📍 وضعیت", "🃏 دست من"):
            return {"type": "show"}
        return None

    @classmethod
    def timeout(cls, st) -> list:
        events = []
        afk = st.setdefault("afk", {})
        for pid in list(cls.current(st)):
            i = _idx(st, pid)
            if i < 0 or st["players"][i].get("left"):
                continue
            afk[pid] = afk.get(pid, 0) + 1
            if afk[pid] >= AFK_KICK_AFTER:
                events.append(f"🚫 {_pname(st, i)} به‌خاطر عدم فعالیت از بازی حذف شد.")
                events += cls.player_left(st, pid)
            else:
                events.append(f"⏰ زمان {_pname(st, i)} تموم شد؛ حرکت خودکار انجام شد.")
                err, ev = cls.apply(st, pid, cls.auto_action(st, pid))
                events += ev
        return events


# ------------------------------ مار و پله ----------------------------------

class SnakesLadders(BaseEngine):
    key = "SNL"
    SNAKES = {16: 6, 47: 26, 49: 11, 56: 53, 62: 19, 64: 60, 87: 24, 93: 73, 95: 75, 98: 78}
    LADDERS = {1: 38, 4: 14, 9: 31, 21: 42, 28: 84, 36: 44, 51: 67, 71: 91, 80: 100}
    COLORS = ["🔴", "🟢", "🟡", "🔵"]

    @classmethod
    def new(cls, players):
        st = {"players": [dict(p) for p in players], "pos": [0] * len(players), "turn": 0,
              "sixes": 0, "last_die": None, "over": False, "winner": None, "log": [], "afk": {}}
        cls.touch(st)
        ev = ["🐍 مار و پله شروع شد! همه از خونهٔ ۰ شروع می‌کنن؛ اولین نفری که به ۱۰۰ برسه می‌بره."]
        _log(st, ev)
        return st, ev

    @classmethod
    def current(cls, st):
        return [] if st["over"] else [st["players"][st["turn"]]["id"]]

    @classmethod
    def _advance(cls, st):
        st["sixes"] = 0
        n = len(st["players"])
        for _ in range(n):
            st["turn"] = (st["turn"] + 1) % n
            if not st["players"][st["turn"]].get("left"):
                return

    @classmethod
    def apply(cls, st, pid, action):
        if st["over"]:
            return "بازی تموم شده.", []
        if action.get("type") != "roll":
            return "حرکت نامعتبر.", []
        i = _idx(st, pid)
        if i != st["turn"]:
            return "نوبت تو نیست.", []
        die = random.randint(1, 6)
        st["last_die"] = die
        c, name = cls.COLORS[i], _pname(st, i)
        target = st["pos"][i] + die
        ev = []
        if target > 100:
            ev.append(f"🎲 {c} {name} عدد {die} آورد ولی از ۱۰۰ رد می‌شه؛ سر جاش موند.")
        else:
            msg = f"🎲 {c} {name} عدد {die} آورد → خونهٔ {target}"
            final = target
            if target in cls.SNAKES:
                final = cls.SNAKES[target]
                msg += f" 🐍 مار! افتاد به {final}"
            elif target in cls.LADDERS:
                final = cls.LADDERS[target]
                msg += f" 🪜 نردبون! رفت به {final}"
            st["pos"][i] = final
            ev.append(msg)
            if final == 100:
                st["over"] = True
                st["winner"] = i
                ev.append(f"🏆 {c} {name} برنده شد!")
                _log(st, ev)
                return None, ev
        if die == 6:
            st["sixes"] += 1
            if st["sixes"] >= 3:
                ev.append("⚠️ سه تا ۶ پشت‌هم! نوبت از دست رفت.")
                cls._advance(st)
            else:
                ev.append("🔁 ۶ آورد؛ یک بار دیگه تاس می‌ندازه.")
        else:
            cls._advance(st)
        cls.touch(st)
        _log(st, ev)
        return None, ev

    @classmethod
    def auto_action(cls, st, pid):
        return {"type": "roll"}

    @classmethod
    def player_left(cls, st, pid):
        i = _idx(st, pid)
        if i < 0 or st["players"][i].get("left"):
            return []
        st["players"][i]["left"] = True
        ev = [f"🚪 {_pname(st, i)} از بازی خارج شد."]
        if len(_active(st)) >= 2 and st["turn"] == i:
            cls._advance(st)
            cls.touch(st)
        _log(st, ev)
        return ev

    @classmethod
    def parse_text(cls, st, pid, text):
        if text == "🎲 تاس بنداز":
            return {"type": "roll"}
        return cls.parse_common(text)

    @classmethod
    def public_text(cls, st):
        parts = [f"{cls.COLORS[i]} {p['name']}: {st['pos'][i]}"
                 for i, p in enumerate(st["players"]) if not p.get("left")]
        return "📍 " + " | ".join(parts)

    @classmethod
    def text_view(cls, st, pid):
        i = _idx(st, pid)
        if st["turn"] == i and not st["over"]:
            return (cls.public_text(st) + f"\n\n🎲 نوبت توئه! (جایگاه تو: {st['pos'][i]})",
                    [["🎲 تاس بنداز"], LEAVE_ROW])
        return (cls.public_text(st) + f"\n\n⏳ نوبت {_pname(st, st['turn'])} است.", WAIT_KB)

    @classmethod
    def view(cls, st, pid):
        i = _idx(st, pid)
        return {"game": "SNL", "me": i, "turn": st["turn"], "my_turn": st["turn"] == i and not st["over"],
                "players": [{"name": p["name"], "color": k, "pos": st["pos"][k], "left": bool(p.get("left"))}
                            for k, p in enumerate(st["players"])],
                "last_die": st["last_die"], "snakes": cls.SNAKES, "ladders": cls.LADDERS,
                "over": st["over"], "winner": st["winner"], "log": st["log"][-12:]}

    @classmethod
    def result_text(cls, st):
        order = sorted(range(len(st["players"])), key=lambda k: -st["pos"][k])
        lines = ["🏁 نتیجهٔ مار و پله:"]
        for r, k in enumerate(order, 1):
            lines.append(f"{r}. {cls.COLORS[k]} {_pname(st, k)} — خونهٔ {st['pos'][k]}")
        return "\n".join(lines)


# ------------------------------- منچ --------------------------------------

class LudoEngine(BaseEngine):
    key = "LUDO"
    COLORS = ["🔴", "🟢", "🟡", "🔵"]
    SEATS = {2: [0, 2], 3: [0, 1, 2], 4: [0, 1, 2, 3]}
    FINISH = 56          # 0..50 روی حلقه، 51..56 مسیر نهایی
    SAFE_MOD = (0, 8)    # خانهٔ شروع و ستاره (abs % 13)

    @classmethod
    def new(cls, players):
        n = len(players)
        ps = [dict(p) for p in players]
        random.shuffle(ps)
        for i, p in enumerate(ps):
            p["color"] = cls.SEATS[n][i]
        st = {"players": ps, "pieces": [[-1] * 4 for _ in ps], "turn": 0, "phase": "roll",
              "die": None, "sixes": 0, "over": False, "winner": None, "log": [], "afk": {}}
        cls.touch(st)
        ev = ["🎲 منچ شروع شد! با ۶ مهره از پایگاه میاد بیرون؛ هر ۴ مهره رو زودتر به مقصد برسونی می‌بری."]
        _log(st, ev)
        return st, ev

    @classmethod
    def current(cls, st):
        return [] if st["over"] else [st["players"][st["turn"]]["id"]]

    @classmethod
    def _abs(cls, color, rel):
        return (13 * color + rel) % 52

    @classmethod
    def movable(cls, st, pi, die):
        res = []
        for k, rel in enumerate(st["pieces"][pi]):
            if rel == -1:
                if die == 6:
                    res.append(k)
            elif rel < cls.FINISH and rel + die <= cls.FINISH:
                res.append(k)
        return res

    @classmethod
    def _advance(cls, st):
        st["sixes"] = 0
        st["phase"] = "roll"
        st["die"] = None
        n = len(st["players"])
        for _ in range(n):
            st["turn"] = (st["turn"] + 1) % n
            if not st["players"][st["turn"]].get("left"):
                return

    @classmethod
    def _do_move(cls, st, pi, k, die):
        p = st["players"][pi]
        c = cls.COLORS[p["color"]]
        rel = st["pieces"][pi][k]
        new = 0 if rel == -1 else rel + die
        st["pieces"][pi][k] = new
        ev, extra = [], die == 6
        if rel == -1:
            ev.append(f"{c} {p['name']} مهرهٔ {k+1} رو آورد بیرون.")
        else:
            ev.append(f"{c} {p['name']} مهرهٔ {k+1} رو {die} خونه جلو برد.")
        if new <= 50:
            a = cls._abs(p["color"], new)
            if a % 13 not in cls.SAFE_MOD:
                for qi, q in enumerate(st["players"]):
                    if qi == pi:
                        continue
                    for qk, qrel in enumerate(st["pieces"][qi]):
                        if 0 <= qrel <= 50 and cls._abs(q["color"], qrel) == a:
                            st["pieces"][qi][qk] = -1
                            ev.append(f"💥 {c} {p['name']} مهرهٔ {cls.COLORS[q['color']]} {q['name']} رو زد و برگشت به پایگاه!")
                            extra = True
        elif new == cls.FINISH:
            ev.append(f"🏁 مهرهٔ {k+1} {p['name']} به مقصد رسید!")
            extra = True
        if all(x == cls.FINISH for x in st["pieces"][pi]):
            st["over"] = True
            st["winner"] = pi
            ev.append(f"🏆 {c} {p['name']} برنده شد!")
            return ev
        if extra:
            st["phase"] = "roll"
            st["die"] = None
            ev.append("🔁 یک بار دیگه نوبتشه.")
        else:
            cls._advance(st)
        return ev

    @classmethod
    def apply(cls, st, pid, action):
        if st["over"]:
            return "بازی تموم شده.", []
        pi = _idx(st, pid)
        if pi != st["turn"]:
            return "نوبت تو نیست.", []
        t = action.get("type")
        p = st["players"][pi]
        c = cls.COLORS[p["color"]]
        ev = []
        if t == "roll":
            if st["phase"] != "roll":
                return "اول باید یک مهره رو حرکت بدی.", []
            die = random.randint(1, 6)
            st["die"] = die
            ev.append(f"🎲 {c} {p['name']} عدد {die} آورد.")
            if die == 6:
                st["sixes"] += 1
                if st["sixes"] >= 3:
                    ev.append("⚠️ سه تا ۶ پشت‌هم! نوبت از دست رفت.")
                    cls._advance(st)
                    cls.touch(st)
                    _log(st, ev)
                    return None, ev
            mv = cls.movable(st, pi, die)
            if not mv:
                ev.append("هیچ حرکتی ممکن نیست؛ نوبت رد شد.")
                cls._advance(st)
            elif len(mv) == 1:
                ev += cls._do_move(st, pi, mv[0], die)
            else:
                st["phase"] = "move"
        elif t == "move":
            if st["phase"] != "move":
                return "اول تاس بنداز.", []
            k = action.get("piece")
            if not isinstance(k, int) or k not in cls.movable(st, pi, st["die"]):
                return "این مهره قابل حرکت نیست.", []
            ev += cls._do_move(st, pi, k, st["die"])
        else:
            return "حرکت نامعتبر.", []
        cls.touch(st)
        _log(st, ev)
        return None, ev

    @classmethod
    def auto_action(cls, st, pid):
        if st["phase"] == "roll":
            return {"type": "roll"}
        pi = _idx(st, pid)
        mv = cls.movable(st, pi, st["die"])
        return {"type": "move", "piece": max(mv, key=lambda k: st["pieces"][pi][k])}

    @classmethod
    def player_left(cls, st, pid):
        i = _idx(st, pid)
        if i < 0 or st["players"][i].get("left"):
            return []
        st["players"][i]["left"] = True
        st["pieces"][i] = [-1] * 4
        ev = [f"🚪 {_pname(st, i)} از بازی خارج شد و مهره‌هاش برداشته شد."]
        if len(_active(st)) >= 2 and st["turn"] == i:
            cls._advance(st)
            cls.touch(st)
        _log(st, ev)
        return ev

    @classmethod
    def _desc(cls, rel):
        if rel == -1:
            return "🏠 پایگاه"
        if rel == cls.FINISH:
            return "🏁 رسیده"
        if rel > 50:
            return f"⭐ مسیر نهایی ({rel - 50}/6)"
        return f"قدم {rel + 1}"

    @classmethod
    def public_text(cls, st):
        lines = []
        for i, p in enumerate(st["players"]):
            if p.get("left"):
                continue
            done = sum(1 for x in st["pieces"][i] if x == cls.FINISH)
            out = sum(1 for x in st["pieces"][i] if 0 <= x < cls.FINISH)
            lines.append(f"{cls.COLORS[p['color']]} {p['name']}: رسیده {done} | در مسیر {out} | پایگاه {4 - done - out}")
        return "📍 " + "\n".join(lines)

    @classmethod
    def parse_text(cls, st, pid, text):
        if text == "🎲 تاس بنداز":
            return {"type": "roll"}
        m = re.match(r"^مهره\s*(\d)", text)
        if m:
            return {"type": "move", "piece": int(m.group(1)) - 1}
        return cls.parse_common(text)

    @classmethod
    def text_view(cls, st, pid):
        i = _idx(st, pid)
        pub = cls.public_text(st)
        if st["turn"] != i or st["over"]:
            return pub + f"\n\n⏳ نوبت {_pname(st, st['turn'])} است.", WAIT_KB
        mine = "\n".join(f"مهرهٔ {k+1}: {cls._desc(r)}" for k, r in enumerate(st["pieces"][i]))
        if st["phase"] == "roll":
            return f"{pub}\n\n{mine}\n\n🎲 نوبت توئه!", [["🎲 تاس بنداز"], LEAVE_ROW]
        mv = cls.movable(st, i, st["die"])
        rows = [[f"مهره {k+1}" for k in mv], LEAVE_ROW]
        return f"{pub}\n\n{mine}\n\n🎲 عدد {st['die']} اومد؛ کدوم مهره رو حرکت بدم؟", rows

    @classmethod
    def view(cls, st, pid):
        i = _idx(st, pid)
        my = st["turn"] == i and not st["over"]
        return {"game": "LUDO", "me": i, "turn": st["turn"], "phase": st["phase"], "die": st["die"],
                "my_turn": my,
                "movable": cls.movable(st, i, st["die"]) if my and st["phase"] == "move" else [],
                "players": [{"name": p["name"], "color": p["color"], "pieces": st["pieces"][k],
                             "left": bool(p.get("left"))} for k, p in enumerate(st["players"])],
                "over": st["over"], "winner": st["winner"], "log": st["log"][-12:]}

    @classmethod
    def result_text(cls, st):
        order = sorted(range(len(st["players"])),
                       key=lambda k: -sum(max(0, x) + (60 if x == cls.FINISH else 0) for x in st["pieces"][k]))
        if st["winner"] is not None:
            order.remove(st["winner"])
            order.insert(0, st["winner"])
        lines = ["🏁 نتیجهٔ منچ:"]
        for r, k in enumerate(order, 1):
            fin = sum(1 for x in st["pieces"][k] if x == cls.FINISH)
            lines.append(f"{r}. {cls.COLORS[st['players'][k]['color']]} {_pname(st, k)} — {fin} مهره رسیده")
        return "\n".join(lines)


# ------------------------------- حکم --------------------------------------

class HokmEngine(BaseEngine):
    key = "HOKM"
    SUITS = ["S", "H", "D", "C"]
    SYM = {"S": "♠", "H": "♥", "D": "♦", "C": "♣"}
    FA = {"S": "پیک", "H": "دل", "D": "خشت", "C": "گشنیز"}
    RANKS = {11: "J", 12: "Q", 13: "K", 14: "A"}

    @classmethod
    def label(cls, c):
        return cls.SYM[c[0]] + cls.RANKS.get(int(c[1:]), c[1:])

    @classmethod
    def sort_hand(cls, hand):
        order = {s: i for i, s in enumerate(cls.SUITS)}
        return sorted(hand, key=lambda c: (order[c[0]], -int(c[1:])))

    @classmethod
    def new(cls, players):
        ps = [dict(p) for p in players]
        random.shuffle(ps)
        st = {"players": ps, "points": [0, 0], "round": 0, "hakem": random.randrange(4),
              "over": False, "winner": None, "log": [], "afk": {}}
        ev = [f"🃏 حکم شروع شد! تیم ۱: {ps[0]['name']} و {ps[2]['name']} — تیم ۲: {ps[1]['name']} و {ps[3]['name']}",
              f"🎯 تیمی که اول به {HOKM_TARGET_POINTS} امتیاز برسه برنده است."]
        ev += cls._start_round(st)
        _log(st, ev)
        return st, ev

    @classmethod
    def _start_round(cls, st):
        deck = [f"{s}{r}" for s in cls.SUITS for r in range(2, 15)]
        random.shuffle(deck)
        st["round"] += 1
        st["hands"] = [deck[i * 5:(i + 1) * 5] for i in range(4)]
        st["rest"] = [deck[20 + i * 8:20 + (i + 1) * 8] for i in range(4)]
        st["trump"] = None
        st["phase"] = "trump"
        st["trick"] = []
        st["last_trick"] = None
        st["tricks"] = [0, 0]
        st["turn"] = st["hakem"]
        cls.touch(st)
        return [f"🃏 دست {st['round']} شروع شد. حاکم: 👑 {_pname(st, st['hakem'])} (منتظر انتخاب حکم)"]

    @classmethod
    def current(cls, st):
        return [] if st["over"] else [st["players"][st["turn"]]["id"]]

    @classmethod
    def legal(cls, st, seat):
        hand = st["hands"][seat]
        if st["trick"]:
            lead = st["trick"][0][1][0]
            same = [c for c in hand if c[0] == lead]
            if same:
                return same
        return list(hand)

    @classmethod
    def _trick_winner(cls, st):
        lead, trump = st["trick"][0][1][0], st["trump"]
        best = None
        for seat, card in st["trick"]:
            key = (2 if card[0] == trump else 1 if card[0] == lead else 0, int(card[1:]))
            if best is None or key > best[0]:
                best = (key, seat)
        return best[1]

    @classmethod
    def _finish_trick(cls, st):
        w = cls._trick_winner(st)
        st["last_trick"] = [list(x) for x in st["trick"]]
        team = w % 2
        st["tricks"][team] += 1
        st["trick"] = []
        st["turn"] = w
        ev = [f"🏆 {_pname(st, w)} دست رو برد (تیم {team+1}: {st['tricks'][team]})"]
        if st["tricks"][team] == 7:
            ev += cls._finish_round(st, team)
        return ev

    @classmethod
    def _finish_round(cls, st, team):
        other = 1 - team
        hakem_team = st["hakem"] % 2
        pts = 1
        txt = ""
        if st["tricks"][other] == 0:
            pts = 3 if team != hakem_team else 2
            txt = " (حاکم‌کوت!)" if team != hakem_team else " (کوت!)"
        st["points"][team] += pts
        ev = [f"✅ تیم {team+1} این دست رو برد{txt} +{pts} امتیاز. امتیازها: تیم۱ {st['points'][0]} — تیم۲ {st['points'][1]}"]
        if st["points"][team] >= HOKM_TARGET_POINTS:
            st["over"] = True
            st["winner"] = team
            ev.append(f"🏆 تیم {team+1} برنده بازی شد!")
            return ev
        if team != hakem_team:
            st["hakem"] = (st["hakem"] + 1) % 4
        ev += cls._start_round(st)
        return ev

    @classmethod
    def apply(cls, st, pid, action):
        if st["over"]:
            return "بازی تموم شده.", []
        seat = _idx(st, pid)
        t = action.get("type")
        if seat < 0:
            return "تو عضو بازی نیستی.", []
        ev = []
        if st["phase"] == "trump":
            if t != "trump":
                return "اول حاکم باید حکم رو انتخاب کنه.", []
            if seat != st["hakem"]:
                return "فقط حاکم حکم رو انتخاب می‌کنه.", []
            s = action.get("suit")
            if s not in cls.SUITS:
                return "خال نامعتبر.", []
            st["trump"] = s
            for i in range(4):
                st["hands"][i] += st["rest"][i]
            st["rest"] = []
            st["phase"] = "play"
            st["turn"] = st["hakem"]
            ev.append(f"👑 {_pname(st, seat)} حکم رو {cls.SYM[s]} {cls.FA[s]} گذاشت. بازی شروع شد!")
        else:
            if t != "play":
                return "الان باید کارت بازی کنی.", []
            if seat != st["turn"]:
                return "نوبت تو نیست.", []
            card = action.get("card")
            if card not in st["hands"][seat]:
                return "این کارت تو دستت نیست.", []
            if card not in cls.legal(st, seat):
                return "باید هم‌خال زمین بازی کنی.", []
            st["hands"][seat].remove(card)
            st["trick"].append([seat, card])
            ev.append(f"🃏 {_pname(st, seat)}: {cls.label(card)}")
            if len(st["trick"]) < 4:
                st["turn"] = (seat + 1) % 4
            else:
                ev += cls._finish_trick(st)
        if not st["over"]:
            cls.touch(st)
        _log(st, ev)
        return None, ev

    @classmethod
    def auto_action(cls, st, pid):
        seat = _idx(st, pid)
        if st["phase"] == "trump":
            best = max(cls.SUITS, key=lambda s: sum(1 for c in st["hands"][seat] if c[0] == s))
            return {"type": "trump", "suit": best}
        legal = cls.legal(st, seat)
        return {"type": "play", "card": min(legal, key=lambda c: int(c[1:]))}

    @classmethod
    def player_left(cls, st, pid):
        i = _idx(st, pid)
        if i < 0 or st["players"][i].get("left"):
            return []
        st["players"][i]["left"] = True
        ev = [f"🚪 {_pname(st, i)} از بازی خارج شد."]
        _log(st, ev)
        return ev

    @classmethod
    def public_text(cls, st):
        trump = f"{cls.SYM[st['trump']]} {cls.FA[st['trump']]}" if st.get("trump") else "—"
        s = (f"🃏 دست {st['round']} | حکم: {trump} | حاکم: {_pname(st, st['hakem'])}\n"
             f"📊 امتیاز: تیم۱ {st['points'][0]} — تیم۲ {st['points'][1]} | این دست: {st['tricks'][0]}-{st['tricks'][1]}")
        if st["trick"]:
            s += "\n🎴 روی میز: " + "  ".join(f"{_pname(st, a)}:{cls.label(c)}" for a, c in st["trick"])
        return s

    @classmethod
    def parse_text(cls, st, pid, text):
        seat = _idx(st, pid)
        txt = text.replace(" ", "")
        if st["phase"] == "trump":
            for s in cls.SUITS:
                if txt in (cls.SYM[s] + cls.FA[s], cls.FA[s], cls.SYM[s]):
                    return {"type": "trump", "suit": s}
        else:
            for c in st["hands"][seat]:
                if txt == cls.label(c):
                    return {"type": "play", "card": c}
        return cls.parse_common(text)

    @classmethod
    def text_view(cls, st, pid):
        seat = _idx(st, pid)
        hand = cls.sort_hand(st["hands"][seat])
        head = cls.public_text(st) + "\n\n🃏 دست تو: " + "  ".join(cls.label(c) for c in hand)
        if st["turn"] != seat or st["over"]:
            return head + f"\n\n⏳ نوبت {_pname(st, st['turn'])} است.", [["🃏 دست من"], LEAVE_ROW]
        if st["phase"] == "trump":
            rows = [[f"{cls.SYM['S']} {cls.FA['S']}", f"{cls.SYM['H']} {cls.FA['H']}"],
                    [f"{cls.SYM['D']} {cls.FA['D']}", f"{cls.SYM['C']} {cls.FA['C']}"], LEAVE_ROW]
            return head + "\n\n👑 تو حاکمی! با دیدن ۵ کارت اول، حکم رو انتخاب کن:", rows
        legal = cls.sort_hand(cls.legal(st, seat))
        labels = [cls.label(c) for c in legal]
        rows = [labels[i:i + 5] for i in range(0, len(labels), 5)] + [LEAVE_ROW]
        return head + "\n\n🃏 نوبت توئه! یک کارت بزن:", rows

    @classmethod
    def view(cls, st, pid):
        seat = _idx(st, pid)
        my = st["turn"] == seat and not st["over"]
        return {"game": "HOKM", "me": seat, "turn": st["turn"], "my_turn": my, "phase": st["phase"],
                "hakem": st["hakem"], "trump": st["trump"], "hand": cls.sort_hand(st["hands"][seat]),
                "legal": cls.legal(st, seat) if my and st["phase"] == "play" else [],
                "trick": st["trick"], "last_trick": st["last_trick"], "tricks": st["tricks"],
                "points": st["points"], "target": HOKM_TARGET_POINTS, "round": st["round"],
                "counts": [len(h) for h in st["hands"]],
                "players": [{"name": p["name"], "left": bool(p.get("left"))} for p in st["players"]],
                "over": st["over"], "winner": st["winner"], "log": st["log"][-12:]}

    @classmethod
    def result_text(cls, st):
        ps = st["players"]
        w = st["winner"]
        head = "🏁 نتیجهٔ حکم:"
        if w is not None:
            head += f"\n🏆 تیم {w+1} ({ps[w]['name']} و {ps[w+2]['name']}) برنده شد!"
        return f"{head}\nامتیاز نهایی: تیم۱ {st['points'][0]} — تیم۲ {st['points'][1]}"


# ---------------------------- اسم و فامیل ----------------------------------

class NameFamily(BaseEngine):
    key = "NAF"
    CATS = ["اسم", "فامیل", "شهر", "کشور", "حیوان", "رنگ", "غذا", "اشیاء"]
    LETTERS = list("ابپتجچحخدرزسشصطعفقکگلمنوهی")

    @classmethod
    def _norm(cls, s: str) -> str:
        s = (s or "").strip()
        for a, b in (("ي", "ی"), ("ك", "ک"), ("آ", "ا"), ("أ", "ا"), ("إ", "ا"), ("ة", "ه")):
            s = s.replace(a, b)
        return re.sub(r"[\s\u200c\u200f\u200e.,!?؟،\-_]+", "", s)

    @classmethod
    def new(cls, players):
        st = {"players": [dict(p) for p in players], "round": 0, "used": [], "phase": "answer",
              "scores": {p["id"]: 0 for p in players}, "over": False, "winner": None, "log": [],
              "last_result": [], "afk": {}}
        ev = [f"📝 اسم و فامیل شروع شد! {NAF_ROUNDS} دور، هر دور یک حرف. جواب یکتا ۱۰ امتیاز، جواب تکراری ۵."]
        ev += cls._start_round(st)
        _log(st, ev)
        return st, ev

    @classmethod
    def _start_round(cls, st):
        pool = [l for l in cls.LETTERS if l not in st["used"]] or cls.LETTERS
        st["letter"] = random.choice(pool)
        st["used"].append(st["letter"])
        st["round"] += 1
        act = [st["players"][i]["id"] for i in _active(st)]
        st["answers"] = {pid: {} for pid in act}
        st["done"] = {pid: False for pid in act}
        st["phase"] = "answer"
        st["deadline"] = now() + NAF_ROUND_SECONDS
        st["grace_set"] = False
        return [f"🔤 دور {st['round']} از {NAF_ROUNDS} — حرف: «{st['letter']}»\n"
                f"⏱ {NAF_ROUND_SECONDS} ثانیه وقت دارین.\nدسته‌ها: " + "، ".join(cls.CATS)]

    @classmethod
    def current(cls, st):
        if st["over"] or st["phase"] != "answer":
            return []
        return [pid for pid, d in st["done"].items() if not d]

    @classmethod
    def _player_done(cls, st, pid):
        st["done"][pid] = True
        i = _idx(st, pid)
        ev = [f"✅ {_pname(st, i)} جواب‌هاش رو داد."]
        if all(st["done"].values()):
            ev += cls._end_round(st)
        elif not st["grace_set"]:
            st["grace_set"] = True
            st["deadline"] = min(st["deadline"], now() + NAF_GRACE_SECONDS)
            ev.append(f"⏳ بقیه {NAF_GRACE_SECONDS} ثانیه وقت دارن!")
        return None, ev

    @classmethod
    def apply(cls, st, pid, action):
        if st["over"]:
            return "بازی تموم شده.", []
        if st["phase"] != "answer":
            return "الان دور فعال نیست.", []
        if pid not in st["done"]:
            return "تو عضو این دور نیستی.", []
        if st["done"][pid]:
            return "جواب‌هات رو قبلاً ثبت کردی.", []
        t = action.get("type")
        ans = st["answers"].setdefault(pid, {})
        if t == "answer":
            text = str(action.get("text", "")).strip()[:40]
            if text in ("ندارم", "-", "—"):
                text = ""
            cat = action.get("cat") or next((c for c in cls.CATS if c not in ans), None)
            if cat not in cls.CATS:
                return "دستهٔ نامعتبر.", []
            ans[cat] = text
            if all(c in ans for c in cls.CATS):
                return cls._player_done(st, pid)
            return None, []
        if t == "submit":
            data = action.get("answers") or {}
            for c in cls.CATS:
                ans[c] = str(data.get(c, "")).strip()[:40]
            return cls._player_done(st, pid)
        if t == "stop":
            return cls._player_done(st, pid)
        return "حرکت نامعتبر.", []

    @classmethod
    def _end_round(cls, st):
        letter = st["letter"]
        pids = [pid for pid in st["answers"]]
        norm = {pid: {c: cls._norm(st["answers"][pid].get(c, "")) for c in cls.CATS} for pid in pids}

        def valid(a):
            return len(a) >= 2 and a[0] == letter

        cnt = {c: {} for c in cls.CATS}
        for pid in pids:
            for c in cls.CATS:
                a = norm[pid][c]
                if valid(a):
                    cnt[c][a] = cnt[c].get(a, 0) + 1
        lines = [f"📊 نتیجهٔ دور {st['round']} (حرف «{letter}»)"]
        for pid in pids:
            i = _idx(st, pid)
            total, parts = 0, []
            for c in cls.CATS:
                raw, a = st["answers"][pid].get(c, ""), norm[pid][c]
                if not valid(a):
                    parts.append(f"{raw or '—'}❌")
                elif cnt[c][a] > 1:
                    total += 5
                    parts.append(f"{raw}♻️5")
                else:
                    total += 10
                    parts.append(f"{raw}✅10")
            st["scores"][pid] += total
            lines.append(f"👤 {_pname(st, i)} (+{total}، جمع {st['scores'][pid]}):\n   " + " | ".join(parts))
        st["last_result"] = lines
        ev = ["\n".join(lines)]
        if st["round"] >= NAF_ROUNDS:
            st["over"] = True
            best = max(st["scores"].values())
            st["winner"] = [i for i, p in enumerate(st["players"]) if st["scores"][p["id"]] == best][0]
            st["deadline"] = None
            ev.append(f"🏆 {_pname(st, st['winner'])} برنده شد!")
        else:
            st["phase"] = "between"
            st["deadline"] = now() + NAF_BETWEEN_SECONDS
            ev.append(f"⏭ دور بعد تا {NAF_BETWEEN_SECONDS} ثانیه دیگه شروع می‌شه.")
        return ev

    @classmethod
    def timeout(cls, st):
        if st["over"]:
            return []
        if st["phase"] == "answer":
            ev = cls._end_round(st)
        else:
            ev = cls._start_round(st)
        _log(st, ev)
        return ev

    @classmethod
    def auto_action(cls, st, pid):
        return {"type": "stop"}

    @classmethod
    def player_left(cls, st, pid):
        i = _idx(st, pid)
        if i < 0 or st["players"][i].get("left"):
            return []
        st["players"][i]["left"] = True
        ev = [f"🚪 {_pname(st, i)} از بازی خارج شد."]
        if st["phase"] == "answer" and pid in st["done"]:
            st["done"][pid] = True
            if all(st["done"].values()) and len(_active(st)) >= 2:
                ev += cls._end_round(st)
        _log(st, ev)
        return ev

    @classmethod
    def parse_text(cls, st, pid, text):
        if st["phase"] != "answer" or st["done"].get(pid, True):
            return cls.parse_common(text)
        if text == "⏹ استپ (پایان)":
            return {"type": "stop"}
        if text == "📍 وضعیت":
            return {"type": "show"}
        return {"type": "answer", "text": text}

    @classmethod
    def public_text(cls, st):
        board = " | ".join(f"{p['name']}: {st['scores'][p['id']]}" for p in st["players"] if not p.get("left"))
        return f"📝 دور {st['round']}/{NAF_ROUNDS} — حرف «{st.get('letter','')}»\n🏅 {board}"

    @classmethod
    def text_view(cls, st, pid):
        pub = cls.public_text(st)
        if st["phase"] != "answer" or st["done"].get(pid, True):
            return pub + "\n\n⏳ منتظر بقیه / دور بعد...", WAIT_KB
        ans = st["answers"].get(pid, {})
        cat = next(c for c in cls.CATS if c not in ans)
        left = max(0, int(st["deadline"] - now()))
        rows = [["ندارم"], ["⏹ استپ (پایان)"], LEAVE_ROW]
        return (f"{pub}\n\n⏱ {left} ثانیه مونده\n✏️ ({len(ans)+1}/{len(cls.CATS)}) «{cat}» با حرف «{st['letter']}» رو بنویس:", rows)

    @classmethod
    def view(cls, st, pid):
        dl = st.get("deadline")
        return {"game": "NAF", "round": st["round"], "rounds": NAF_ROUNDS, "letter": st.get("letter"),
                "cats": cls.CATS, "phase": st["phase"], "done": bool(st.get("done", {}).get(pid, True)),
                "my_answers": st.get("answers", {}).get(pid, {}),
                "players": [{"name": p["name"], "left": bool(p.get("left")),
                             "done": bool(st.get("done", {}).get(p["id"], False)),
                             "score": st["scores"][p["id"]]} for p in st["players"]],
                "last_result": st["last_result"], "over": st["over"],
                "winner": st["winner"], "log": [],
                "time_left": max(0, int(dl - now())) if dl else 0}

    @classmethod
    def result_text(cls, st):
        order = sorted(st["players"], key=lambda p: -st["scores"][p["id"]])
        lines = ["🏁 نتیجهٔ اسم و فامیل:"]
        for r, p in enumerate(order, 1):
            lines.append(f"{r}. {p['name']} — {st['scores'][p['id']]} امتیاز")
        return "\n".join(lines)


ENGINES = {"SNL": SnakesLadders, "LUDO": LudoEngine, "HOKM": HokmEngine, "NAF": NameFamily}

GAME_TYPES = {
    "HOKM": {"title": "حکم", "icon": "🃏", "min": 4, "max": 4, "web": True},
    "LUDO": {"title": "منچ", "icon": "🎲", "min": 2, "max": 4, "web": True},
    "SNL": {"title": "مار و پله", "icon": "🐍", "min": 2, "max": 4, "web": True},
    "NAF": {"title": "اسم و فامیل", "icon": "📝", "min": 2, "max": 8, "web": True},
    "TOD": {"title": "جرئت یا حقیقت", "icon": "🔥", "min": MIN_PLAYERS_TO_START, "max": DEFAULT_ROOM_CAPACITY,
            "web": False},
}
for _k, _v in GAME_TYPES.items():
    _v["cost"] = ROOM_COSTS[_k]


# ============================== HELPERS ======================================

IR_TZ = timezone(timedelta(hours=3, minutes=30))
_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩", "01234567890123456789")


def norm_digits(s: str) -> str:
    return s.translate(_DIGITS)


def fmt_num(n: int) -> str:
    return f"{n:,}"


def today_key() -> str:
    return datetime.now(IR_TZ).strftime("%Y-%m-%d")


def web_enabled() -> bool:
    return bool(WEB_BASE_URL)


async def send_long(chat_id: str, text: str, keyboard: Optional[list] = None, limit: int = 3500):
    """پیام‌های طولانی را تکه‌تکه می‌فرستد؛ کیبورد فقط روی آخرین تکه می‌نشیند."""
    chunks, cur = [], ""
    for line in text.split("\n"):
        if len(cur) + len(line) + 1 > limit and cur:
            chunks.append(cur)
            cur = ""
        cur += line + "\n"
    chunks.append(cur.rstrip("\n"))
    res = None
    for i, c in enumerate(chunks):
        res = await send_message(chat_id, c, keyboard=keyboard if i == len(chunks) - 1 else None)
    return res


# ============================== COINS ========================================

def get_coins(user_id: str) -> int:
    row = db.fetchone("SELECT coins FROM users WHERE user_id=?", (user_id,))
    return int(row["coins"] or 0) if row else 0


def add_coins(user_id: str, amount: int, reason: str):
    db.execute("UPDATE users SET coins = COALESCE(coins,0) + ? WHERE user_id=?", (amount, user_id))
    db.execute("INSERT INTO coin_log(user_id,delta,reason,created_at) VALUES (?,?,?,?)",
               (user_id, amount, reason, now()))


def spend_coins(user_id: str, amount: int, reason: str) -> bool:
    """کسر اتمیک: فقط اگر موجودی کافی باشد."""
    cur = db.execute(
        "UPDATE users SET coins = coins - ? WHERE user_id=? AND COALESCE(coins,0) >= ?",
        (amount, user_id, amount),
    )
    if cur.rowcount != 1:
        return False
    db.execute("INSERT INTO coin_log(user_id,delta,reason,created_at) VALUES (?,?,?,?)",
               (user_id, -amount, reason, now()))
    return True


async def claim_daily(user_id: str):
    key = today_key()
    cur = db.execute(
        "UPDATE users SET coins = COALESCE(coins,0) + ?, last_daily=? "
        "WHERE user_id=? AND (last_daily IS NULL OR last_daily != ?)",
        (DAILY_GIFT_COINS, key, user_id, key),
    )
    if cur.rowcount == 1:
        db.execute("INSERT INTO coin_log(user_id,delta,reason,created_at) VALUES (?,?,?,?)",
                   (user_id, DAILY_GIFT_COINS, "daily_gift", now()))
        await send_message(
            user_id,
            f"🎁 هدیهٔ امروزت: {DAILY_GIFT_COINS} سکه به حسابت اضافه شد!\n🪙 موجودی: {get_coins(user_id)} سکه\n"
            f"فردا دوباره بیا 😉",
            keyboard=MAIN_MENU,
        )
    else:
        await send_message(user_id, "🎁 هدیهٔ امروزت رو قبلاً گرفتی؛ فردا دوباره بیا!", keyboard=MAIN_MENU)


# ---------------------------- خرید سکه -------------------------------------

def purchase_caption(row, status_line: str = "") -> str:
    u = db.fetchone("SELECT display_name FROM users WHERE user_id=?", (row["user_id"],))
    t = datetime.fromtimestamp(row["created_at"], IR_TZ).strftime("%Y-%m-%d %H:%M")
    s = (f"💳 درخواست خرید سکه\n\n🆔 شناسه: {row['purchase_id']}\n"
         f"👤 کاربر: {u['display_name'] if u else '?'} ({row['user_id']})\n"
         f"🪙 تعداد سکه: {fmt_num(row['coins'])}\n💵 مبلغ: {fmt_num(row['price'])} تومان\n🕒 زمان: {t}")
    if status_line:
        s += f"\n\n{status_line}"
    else:
        s += (f"\n\n(اگر دکمه‌ها کار نکرد بنویس:\nتایید_خرید {row['purchase_id']}\nرد_خرید {row['purchase_id']})")
    return s


def _owner_buttons(pid: str) -> dict:
    return {"inline_keyboard": [[
        {"text": "✅ تایید", "callback_data": f"pa:{pid}"},
        {"text": "❌ رد", "callback_data": f"pr:{pid}"},
    ]]}


async def send_owner_receipt(row):
    caption = purchase_caption(row)
    kind, mid = "text", None
    if row["receipt_file_id"]:
        method, field_name = ("sendPhoto", "photo") if row["receipt_type"] == "photo" else ("sendDocument", "document")
        res = await api_call(method, {"chat_id": OWNER_ID, field_name: row["receipt_file_id"],
                                      "caption": caption, "reply_markup": _owner_buttons(row["purchase_id"])})
        if res.get("ok", False):
            kind, mid = "media", _extract_message_id(res)
    if mid is None:
        res = await api_call("sendMessage", {"chat_id": OWNER_ID,
                                             "text": caption + "\n\n⚠️ ارسال تصویر فیش ناموفق بود (file_id: "
                                             f"{row['receipt_file_id']})",
                                             "reply_markup": _owner_buttons(row["purchase_id"])})
        mid = _extract_message_id(res)
        kind = "text"
        if mid is None:  # دکمه‌های شیشه‌ای پشتیبانی نشد؛ متن ساده
            res = await send_message(OWNER_ID, caption)
            mid = _extract_message_id(res)
    db.execute("UPDATE purchases SET owner_msg_id=?, owner_msg_kind=? WHERE purchase_id=?",
               (mid, kind, row["purchase_id"]))


async def start_buy_coins(user_id: str):
    set_state(user_id, {"flow": "buy_amount"})
    await send_message(
        user_id,
        f"💰 خرید سکه\n\nهر سکه {fmt_num(COIN_PRICE_TOMAN)} تومان (۱۰۰ سکه = {fmt_num(100 * COIN_PRICE_TOMAN)} تومان)\n"
        f"🪙 موجودی فعلی: {get_coins(user_id)}\n\n"
        f"چند سکه می‌خوای؟ یه عدد بفرست (حداقل {MIN_COIN_PURCHASE}):",
        keyboard=[["50", "100"], ["200", "500"], ["❌ انصراف"]],
    )


async def handle_buy_flow(user_id: str, display_name: str, text: str, state: dict) -> bool:
    flow = state["flow"]
    if text == "❌ انصراف":
        set_state(user_id, None)
        await send_message(user_id, "خرید لغو شد.", keyboard=MAIN_MENU)
        return True
    if flow == "buy_amount":
        t = norm_digits(text).replace(",", "").strip()
        if not t.isdigit() or not (MIN_COIN_PURCHASE <= int(t) <= MAX_COIN_PURCHASE):
            await send_message(user_id, f"❌ یه عدد بین {MIN_COIN_PURCHASE} تا {fmt_num(MAX_COIN_PURCHASE)} بفرست.")
            return True
        coins = int(t)
        price = coins * COIN_PRICE_TOMAN
        set_state(user_id, {"flow": "buy_receipt", "coins": coins, "price": price})
        await send_message(
            user_id,
            f"🧾 فاکتور خرید\n\n🪙 تعداد: {fmt_num(coins)} سکه\n💵 مبلغ نهایی: {fmt_num(price)} تومان\n\n"
            f"{PAYMENT_CARD_TEXT}\n\n📸 بعد از پرداخت، عکس فیش رو همین‌جا بفرست.",
            keyboard=[["❌ انصراف"]],
        )
        return True
    if flow == "buy_receipt":
        await send_message(user_id, "📸 لطفاً عکس فیش پرداخت رو بفرست (یا «❌ انصراف»).")
        return True
    return False


async def handle_receipt(user_id: str, msg: dict, state: dict):
    photo, doc = msg.get("photo"), msg.get("document")
    if photo:
        file_id = photo[-1]["file_id"] if isinstance(photo, list) else photo.get("file_id")
        rtype = "photo"
    else:
        mime = (doc or {}).get("mime_type", "")
        if not (mime.startswith("image/") or mime == "application/pdf"):
            await send_message(user_id, "❌ فقط عکس (یا PDF) فیش قابل قبوله.")
            return
        file_id, rtype = doc.get("file_id"), "document"
    pid = new_id("buy")
    db.execute(
        "INSERT INTO purchases(purchase_id,user_id,coins,price,receipt_file_id,receipt_type,status,created_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (pid, user_id, state["coins"], state["price"], file_id, rtype, "PENDING", now()),
    )
    set_state(user_id, None)
    row = db.fetchone("SELECT * FROM purchases WHERE purchase_id=?", (pid,))
    await send_owner_receipt(row)
    await send_message(
        user_id,
        f"✅ فیش دریافت شد و برای بررسی ارسال شد.\n🆔 شناسه: {pid}\n"
        f"بعد از تایید، {fmt_num(state['coins'])} سکه به حسابت اضافه می‌شه و بهت خبر می‌دم.",
        keyboard=MAIN_MENU,
    )


async def decide_purchase(pid: str, approve: bool, by: str) -> str:
    row = db.fetchone("SELECT * FROM purchases WHERE purchase_id=?", (pid,))
    if not row:
        return "خریدی با این شناسه پیدا نشد."
    cur = db.execute(
        "UPDATE purchases SET status=?, decided_at=?, decided_by=? WHERE purchase_id=? AND status='PENDING'",
        ("APPROVED" if approve else "REJECTED", now(), by, pid),
    )
    if cur.rowcount != 1:
        return "این خرید قبلاً بررسی شده."
    if approve:
        add_coins(row["user_id"], row["coins"], f"purchase:{pid}")
        await send_message(
            row["user_id"],
            f"✅ پرداختت تایید شد!\n🪙 {fmt_num(row['coins'])} سکه به حسابت اضافه شد.\n"
            f"موجودی جدید: {get_coins(row['user_id'])} سکه 🎉",
            keyboard=MAIN_MENU,
        )
        log_admin_action(by, "PURCHASE_APPROVED", pid)
        return "✅ تایید شد و سکه واریز شد."
    await send_message(
        row["user_id"],
        f"❌ متأسفانه فیش خرید {fmt_num(row['coins'])} سکه‌ای (شناسه {pid}) تایید نشد.\n"
        f"اگه فکر می‌کنی اشتباهی شده، با پشتیبانی در تماس باش و دوباره تلاش کن.",
        keyboard=MAIN_MENU,
    )
    log_admin_action(by, "PURCHASE_REJECTED", pid)
    return "❌ رد شد و به کاربر اطلاع داده شد."


async def _mark_owner_message(pid: str, result: str):
    row = db.fetchone("SELECT * FROM purchases WHERE purchase_id=?", (pid,))
    if not row or not row["owner_msg_id"]:
        return
    status = "✅ تایید شد" if row["status"] == "APPROVED" else "❌ رد شد"
    cap = purchase_caption(row, f"{status}")
    empty = {"inline_keyboard": []}
    if row["owner_msg_kind"] == "media":
        await api_call("editMessageCaption", {"chat_id": OWNER_ID, "message_id": row["owner_msg_id"],
                                              "caption": cap, "reply_markup": empty})
    else:
        await api_call("editMessageText", {"chat_id": OWNER_ID, "message_id": row["owner_msg_id"],
                                           "text": cap, "reply_markup": empty})


async def handle_callback(cq: dict):
    uid = str((cq.get("from") or {}).get("id", ""))
    data = cq.get("data") or ""
    cq_id = cq.get("id")
    if not is_owner(uid):
        await api_call("answerCallbackQuery", {"callback_query_id": cq_id, "text": "دسترسی نداری."})
        return
    if data[:3] in ("pa:", "pr:"):
        pid = data[3:]
        result = await decide_purchase(pid, data.startswith("pa:"), uid)
        await api_call("answerCallbackQuery", {"callback_query_id": cq_id, "text": result})
        await _mark_owner_message(pid, result)
    else:
        await api_call("answerCallbackQuery", {"callback_query_id": cq_id})


async def owner_pending_purchases(user_id: str):
    rows = db.fetchall("SELECT * FROM purchases WHERE status='PENDING' ORDER BY created_at ASC LIMIT 10")
    if not rows:
        await send_message(user_id, "💳 خرید در انتظاری نداریم.", keyboard=ADMIN_MENU)
        return
    await send_message(user_id, f"💳 {len(rows)} خرید در انتظار (حداکثر ۱۰ تا نشون داده می‌شه):", keyboard=ADMIN_MENU)
    for r in rows:
        await send_owner_receipt(r)


async def owner_manual_charge(user_id: str, text: str):
    parts = norm_digits(text).split()
    if parts and parts[0] == "شارژ":
        parts = parts[1:]
    if len(parts) != 2 or not parts[1].lstrip("-").isdigit():
        await send_message(user_id, "فرمت: <آیدی_کاربر> <تعداد>\nمثال: 12345 50", keyboard=ADMIN_MENU)
        return
    target, n = parts[0], int(parts[1])
    if not db.fetchone("SELECT 1 FROM users WHERE user_id=?", (target,)):
        await send_message(user_id, "کاربری با این آیدی پیدا نشد.", keyboard=ADMIN_MENU)
        return
    if n < 0:
        if not spend_coins(target, -n, f"admin_adjust:{user_id}"):
            await send_message(user_id, "موجودی کاربر کافی نیست.", keyboard=ADMIN_MENU)
            return
    else:
        add_coins(target, n, f"admin_grant:{user_id}")
    log_admin_action(user_id, "MANUAL_CHARGE", f"{target}:{n}")
    await send_message(user_id, f"✅ انجام شد. موجودی جدید: {get_coins(target)}", keyboard=ADMIN_MENU)
    await send_message(target, f"🪙 موجودی حسابت توسط مدیریت تغییر کرد. موجودی فعلی: {get_coins(target)}")


# ============================== GAME RUNTIME =================================

GAME_LOCKS: dict = {}
GAME_TIMERS: dict = {}
AUTOSTART_TASKS: dict = {}
LAST_NOTIFY: dict = {}


def _room(room_id: str):
    return db.fetchone("SELECT * FROM rooms WHERE room_id=?", (room_id,))


def _load(room):
    return json.loads(room["game_state"]) if room and room["game_state"] else None


def _save(room_id: str, st: dict):
    db.execute("UPDATE rooms SET game_state=?, game_version=COALESCE(game_version,0)+1 WHERE room_id=?",
               (json.dumps(st, ensure_ascii=False), room_id))


def get_web_token(room_id: str, user_id: str) -> str:
    row = db.fetchone("SELECT token FROM web_tokens WHERE room_id=? AND user_id=?", (room_id, user_id))
    if row:
        return row["token"]
    token = secrets.token_urlsafe(18)
    db.execute("INSERT INTO web_tokens(token,room_id,user_id,created_at) VALUES (?,?,?,?)",
               (token, room_id, user_id, now()))
    return token


def web_link(room_id: str, user_id: str) -> str:
    return f"{WEB_BASE_URL.rstrip('/')}/g/{get_web_token(room_id, user_id)}"


def schedule_game_timer(room_id: str):
    old = GAME_TIMERS.pop(room_id, None)
    if old and not old.done() and old is not asyncio.current_task():
        old.cancel()
    room = _room(room_id)
    st = _load(room)
    if not st or room["status"] != "IN_PROGRESS":
        return
    dl = ENGINES[room["game_type"]].deadline(st)
    if not dl:
        return
    version = room["game_version"]

    async def _runner():
        try:
            await asyncio.sleep(max(0.2, dl - now() + 0.3))
        except asyncio.CancelledError:
            return
        await game_timeout(room_id, version)

    GAME_TIMERS[room_id] = asyncio.ensure_future(_runner())


async def notify_web_turn(room_id: str, uid: str):
    k = (room_id, uid)
    if now() - LAST_NOTIFY.get(k, 0) < 20:
        return
    LAST_NOTIFY[k] = now()
    await send_message(uid, f"🔔 نوبت توئه!\n👉 {web_link(room_id, uid)}")


async def push_game_update(room_id: str, events: list, actor: Optional[str], prev_current: set):
    room = _room(room_id)
    st = _load(room)
    if not st:
        return
    eng = ENGINES[room["game_type"]]
    cur = set(eng.current(st))
    web_mode = room["ui_mode"] == "WEB"
    public = eng.public_text(st)
    body = "\n".join(events)
    for p in st["players"]:
        if p.get("left"):
            continue
        uid = p["id"]
        if web_mode:
            if uid in cur and uid not in prev_current:
                await notify_web_turn(room_id, uid)
            continue
        if not body and uid != actor:
            continue
        if uid in cur:
            text, rows = eng.text_view(st, uid)
            await send_long(uid, (body + "\n\n" if body else "") + text, keyboard=rows)
        else:
            parts = [body] if body else []
            if public and body and not eng.is_over(st):
                parts.append(public)
            if not parts:
                parts = ["✅ ثبت شد. منتظر بقیه..."]
            kb = WAIT_KB if (uid == actor or uid in prev_current) and not eng.is_over(st) else None
            await send_long(uid, "\n\n".join(parts), keyboard=kb)


async def _commit_and_push(room_id: str, st: dict, events: list, actor: Optional[str], prev: set):
    """ذخیره، بررسی پایان/لغو بازی و ارسال به بازیکن‌ها. باید داخل قفل روم صدا زده شود."""
    room = _room(room_id)
    key = room["game_type"]
    eng = ENGINES[key]
    over = eng.is_over(st)
    abort = (not over) and len(_active(st)) < GAME_TYPES[key]["min"]
    dl = eng.deadline(st)
    if not over and not abort and dl and dl <= now():
        st["deadline"] = now() + AFK_SECONDS
    _save(room_id, st)
    if over or abort:
        await push_game_update(room_id, events, actor, prev)
        await end_generic_game(room_id, "بازی تموم شد." if over else "تعداد بازیکنان فعال کم شد.")
        return
    schedule_game_timer(room_id)
    await push_game_update(room_id, events, actor, prev)


async def game_action(room_id: str, uid: str, action: dict) -> Optional[str]:
    lock = GAME_LOCKS.setdefault(room_id, asyncio.Lock())
    async with lock:
        room = _room(room_id)
        st = _load(room)
        if not room or room["status"] != "IN_PROGRESS" or not st:
            return "این بازی فعال نیست."
        eng = ENGINES[room["game_type"]]
        i = _idx(st, uid)
        if i < 0 or st["players"][i].get("left"):
            return "تو عضو این بازی نیستی."
        prev = set(eng.current(st))
        err, events = eng.apply(st, uid, action)
        if err:
            return err
        st.setdefault("afk", {})[uid] = 0
        await _commit_and_push(room_id, st, events, uid, prev)
        return None


async def game_timeout(room_id: str, version: int):
    lock = GAME_LOCKS.setdefault(room_id, asyncio.Lock())
    async with lock:
        room = _room(room_id)
        st = _load(room)
        if not room or room["status"] != "IN_PROGRESS" or not st or room["game_version"] != version:
            return
        eng = ENGINES[room["game_type"]]
        dl = eng.deadline(st)
        if not dl or now() < dl - 1:
            schedule_game_timer(room_id)
            return
        prev = set(eng.current(st))
        events = eng.timeout(st)
        await _commit_and_push(room_id, st, events, None, prev)


async def generic_player_left(room_id: str, user_id: str):
    lock = GAME_LOCKS.setdefault(room_id, asyncio.Lock())
    async with lock:
        room = _room(room_id)
        st = _load(room)
        if not room or room["status"] != "IN_PROGRESS" or not st:
            return
        eng = ENGINES[room["game_type"]]
        i = _idx(st, user_id)
        if i < 0 or st["players"][i].get("left"):
            return
        prev = set(eng.current(st))
        events = eng.player_left(st, user_id)
        await _commit_and_push(room_id, st, events, None, prev)


async def end_generic_game(room_id: str, reason: str):
    room = _room(room_id)
    if not room or room["status"] != "IN_PROGRESS":
        return
    st = _load(room)
    eng = ENGINES.get(room["game_type"])
    result = eng.result_text(st) if (eng and st) else f"بازی تموم شد. ({reason})"
    t = GAME_TIMERS.pop(room_id, None)
    if t and not t.done() and t is not asyncio.current_task():
        t.cancel()
    if room["current_game_id"]:
        db.execute("UPDATE games SET ended_at=? WHERE game_id=?", (now(), room["current_game_id"]))
    players = st["players"] if st else []
    for p in players:
        db.execute("UPDATE users SET games_played = games_played + 1, score = score + ? WHERE user_id=?",
                   (POINTS_PARTICIPATION, p["id"]))
    closing = bool(room["is_random"])
    db.execute(
        "UPDATE rooms SET status=?, accepting_new=1, current_game_id=NULL, game_state=NULL, last_result=? WHERE room_id=?",
        ("CLOSED" if closing else "OPEN", result, room_id),
    )
    for m in room_members(room_id):
        if closing:
            db.execute("UPDATE users SET current_room_id=NULL WHERE user_id=?", (m["user_id"],))
        await send_long(m["user_id"], f"{result}\n\n🎉 ممنون که بازی کردین! (+{POINTS_PARTICIPATION} امتیاز)",
                        keyboard=MAIN_MENU if closing else ROOM_MENU)


async def start_generic_game(room_id: str, requester_id: str):
    lock = GAME_LOCKS.setdefault(room_id, asyncio.Lock())
    async with lock:
        room = _room(room_id)
        if not room or room["status"] != "OPEN":
            await send_message(requester_id, "❌ این روم الان آمادهٔ شروع نیست.")
            return
        if room["owner_id"] != requester_id:
            await send_message(requester_id, "❌ فقط گاد روم می‌تونه بازی رو شروع کنه.")
            return
        key = room["game_type"]
        cfg, eng = GAME_TYPES[key], ENGINES[key]
        members = room_members(room_id)
        if len(members) < cfg["min"]:
            await send_message(requester_id,
                               f"❌ برای «{cfg['title']}» حداقل {cfg['min']} بازیکن لازمه (الان {len(members)} نفرید).")
            return
        members = members[:cfg["max"]]
        players = [{"id": m["user_id"], "name": m["display_name"]} for m in members]
        st, events = eng.new(players)
        game_id = new_id("game")
        db.execute("INSERT INTO games(game_id,room_id,started_at) VALUES (?,?,?)", (game_id, room_id, now()))
        db.execute("UPDATE rooms SET status='IN_PROGRESS', accepting_new=0, current_game_id=?, game_version=0 "
                   "WHERE room_id=?", (game_id, room_id))
        _save(room_id, st)
        t = AUTOSTART_TASKS.pop(room_id, None)
        if t and not t.done() and t is not asyncio.current_task():
            t.cancel()
        web_mode = room["ui_mode"] == "WEB"
        for p in players:
            head = f"🚀 {cfg['icon']} {cfg['title']} شروع شد! ({len(players)} بازیکن)"
            if web_mode:
                head += f"\n\n🖼 بازی گرافیکی:\n👉 {web_link(room_id, p['id'])}\n(این لینک مخصوص خودته؛ برای کسی نفرست)"
            await send_message(p["id"], head, keyboard=WAIT_KB)
        schedule_game_timer(room_id)
        await push_game_update(room_id, events, None, set())


def active_game_room(user_id: str):
    cur = db.fetchone("SELECT current_room_id FROM users WHERE user_id=?", (user_id,))
    if not cur or not cur["current_room_id"]:
        return None
    room = _room(cur["current_room_id"])
    if not room or room["status"] != "IN_PROGRESS" or (room["game_type"] or "TOD") == "TOD":
        return None
    st = _load(room)
    i = _idx(st, user_id) if st else -1
    if i < 0 or st["players"][i].get("left"):
        return None
    return room


async def game_text(user_id: str, text: str) -> Optional[bool]:
    """اگر کاربر در یک بازی فعال (غیر TOD) است، متن را به موتور می‌دهد.
    خروجی: True = پردازش شد، False = داخل بازی ولی متن نامربوط، None = داخل بازی نیست."""
    cur = db.fetchone("SELECT current_room_id FROM users WHERE user_id=?", (user_id,))
    if not cur or not cur["current_room_id"]:
        return None
    room = _room(cur["current_room_id"])
    if not room or room["status"] != "IN_PROGRESS" or (room["game_type"] or "TOD") == "TOD":
        return None
    st = _load(room)
    if not st or _idx(st, user_id) < 0 or st["players"][_idx(st, user_id)].get("left"):
        return None
    eng = ENGINES[room["game_type"]]
    action = eng.parse_text(st, user_id, text)
    if action is None:
        return False
    if action.get("type") == "show":
        t, rows = eng.text_view(st, user_id)
        if room["ui_mode"] == "WEB":
            t += f"\n\n🖼 {web_link(room['room_id'], user_id)}"
        await send_long(user_id, t, keyboard=rows)
        return True
    err = await game_action(room["room_id"], user_id, action)
    if err:
        await send_message(user_id, f"❌ {err}")
    return True


async def game_text_fallback(user_id: str):
    room = active_game_room(user_id)
    if not room:
        return
    st = _load(room)
    t, rows = ENGINES[room["game_type"]].text_view(st, user_id)
    await send_long(user_id, "از دکمه‌ها استفاده کن 👇\n\n" + t, keyboard=rows)


def _autostart_delay_task(room_id: str):
    async def _run():
        try:
            await asyncio.sleep(AUTOSTART_DELAY)
        except asyncio.CancelledError:
            return
        AUTOSTART_TASKS.pop(room_id, None)
        room = _room(room_id)
        if room and room["status"] == "OPEN":
            if room["game_type"] == "TOD":
                await start_game(room_id, room["owner_id"])
            else:
                await start_generic_game(room_id, room["owner_id"])
    AUTOSTART_TASKS[room_id] = asyncio.ensure_future(_run())


async def after_join_hook(room_id: str):
    """روم‌های رندوم خودکار شروع می‌شوند: پر شدن ظرفیت = شروع فوری؛ رسیدن به حداقل = شروع بعد از چند ثانیه."""
    room = _room(room_id)
    if not room or not room["is_random"] or room["status"] != "OPEN":
        return
    cfg = GAME_TYPES[room["game_type"]]
    cnt = room_member_count(room_id)
    if cnt >= room["capacity"] or (cnt >= cfg["min"] and cfg["min"] == cfg["max"]):
        if room["game_type"] == "TOD":
            await start_game(room_id, room["owner_id"])
        else:
            await start_generic_game(room_id, room["owner_id"])
    elif cnt >= cfg["min"] and room_id not in AUTOSTART_TASKS:
        await broadcast(room_id, f"⏳ بازی تا {AUTOSTART_DELAY} ثانیه دیگه خودکار شروع می‌شه (یا با پر شدن ظرفیت).")
        _autostart_delay_task(room_id)


async def recover_generic_games():
    rows = db.fetchall("SELECT room_id FROM rooms WHERE status='IN_PROGRESS' AND game_state IS NOT NULL "
                       "AND game_type != 'TOD'")
    for r in rows:
        schedule_game_timer(r["room_id"])
    if rows:
        log.info("بازیابی %d بازی فعال بعد از ری‌استارت.", len(rows))


# ============================== ROOMS: CREATE / RANDOM =======================

def create_room_row(owner_id: str, game_type: str, name: str, room_type: str, ui_mode: str, is_random: int = 0) -> str:
    room_id = new_id("room")
    db.execute(
        "INSERT INTO rooms(room_id,name,room_type,owner_id,status,capacity,invite_code,accepting_new,created_at,"
        "game_type,ui_mode,is_random,game_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,0)",
        (room_id, name, room_type, owner_id, "OPEN", GAME_TYPES[game_type]["max"], gen_invite_code(), 1, now(),
         game_type, ui_mode, is_random),
    )
    db.execute("INSERT INTO room_members(room_id,user_id,join_order,joined_at) VALUES (?,?,?,?)",
               (room_id, owner_id, 0, now()))
    db.execute("UPDATE users SET current_room_id=? WHERE user_id=?", (room_id, owner_id))
    return room_id


def user_in_active_room(user_id: str) -> bool:
    cur = db.fetchone("SELECT current_room_id FROM users WHERE user_id=?", (user_id,))
    if not cur or not cur["current_room_id"]:
        return False
    r = _room(cur["current_room_id"])
    m = db.fetchone("SELECT 1 FROM room_members WHERE room_id=? AND user_id=?", (cur["current_room_id"], user_id))
    return bool(r and r["status"] != "CLOSED" and m)


GAME_ORDER = ("HOKM", "LUDO", "SNL", "NAF", "TOD")
LABELS_COST = {f"{GAME_TYPES[k]['icon']} {GAME_TYPES[k]['title']} ({GAME_TYPES[k]['cost']} 🪙)": k for k in GAME_ORDER}
LABELS_FREE = {f"{GAME_TYPES[k]['icon']} {GAME_TYPES[k]['title']}": k for k in GAME_ORDER}


def _game_rows(with_cost: bool):
    labels = list((LABELS_COST if with_cost else LABELS_FREE).keys())
    return [labels[0:2], labels[2:4], [labels[4]], ["❌ انصراف"]]


async def start_create_room(user_id: str):
    if user_in_active_room(user_id):
        await send_message(user_id, "⚠️ تو الان داخل یک روم هستی؛ اول از روم فعلی خارج شو.", keyboard=ROOM_MENU)
        return
    rows = _game_rows(True)
    set_state(user_id, {"flow": "cr_game"})
    await send_message(user_id, f"🎮 بازی مورد نظرت رو انتخاب کن (هزینهٔ ساخت روم از سکه‌هات کم می‌شه)\n"
                                f"🪙 موجودی: {get_coins(user_id)}", keyboard=rows)


async def start_random(user_id: str):
    if user_in_active_room(user_id):
        await send_message(user_id, "⚠️ تو الان داخل یک روم هستی؛ اول از روم فعلی خارج شو.", keyboard=ROOM_MENU)
        return
    rows = _game_rows(False)
    set_state(user_id, {"flow": "rnd_game"})
    await send_message(user_id, "🎲 روم رندوم کاملاً رایگانه! کدوم بازی؟", keyboard=rows)


async def _ask_ui_mode(user_id: str, state: dict):
    state["flow"] = "cr_ui" if state.get("mode") == "create" else "rnd_ui"
    set_state(user_id, state)
    await send_message(user_id, "🖥 بازی رو چطوری می‌خوای بازی کنی؟",
                       keyboard=[["🖼 گرافیکی (وب)", "💬 دکمه‌ای و متنی"], ["❌ انصراف"]])


async def finalize_create(user_id: str, display_name: str, state: dict, ui_mode: str):
    game = state["game"]
    cost = GAME_TYPES[game]["cost"]
    set_state(user_id, None)
    if not spend_coins(user_id, cost, f"create_room:{game}"):
        await send_message(user_id, f"❌ سکه‌ات کافی نیست. موجودی: {get_coins(user_id)} | نیاز: {cost}",
                           keyboard=MAIN_MENU)
        return
    room_id = create_room_row(user_id, game, state["name"], state["rtype"], ui_mode)
    await send_message(user_id, f"🪙 {cost} سکه کم شد. موجودی: {get_coins(user_id)}")
    await send_message(user_id, build_room_summary(room_id), keyboard=ROOM_MENU)


async def finalize_random(user_id: str, display_name: str, game: str, ui_mode: str):
    set_state(user_id, None)
    rows = db.fetchall(
        "SELECT r.*, (SELECT COUNT(*) FROM room_members rm WHERE rm.room_id=r.room_id) cnt FROM rooms r "
        "WHERE room_type='PUBLIC' AND status='OPEN' AND accepting_new=1 AND game_type=? AND ui_mode=? "
        "ORDER BY created_at ASC", (game, ui_mode))
    for r in rows:
        if r["cnt"] < r["capacity"]:
            err = await try_join_room(user_id, display_name, r["room_id"])
            if err is None:
                return
    name = f"رندوم {GAME_TYPES[game]['title']} #{random.randint(100, 999)}"
    room_id = create_room_row(user_id, game, name, "PUBLIC", ui_mode, is_random=1)
    cfg = GAME_TYPES[game]
    await send_message(user_id, f"✅ روم رندوم ساخته شد (رایگان)!\n{cfg['icon']} {cfg['title']}\n"
                                f"⏳ منتظر بازیکن دیگه‌ایم؛ به محض {cfg['min']} نفر شدن بازی شروع می‌شه.",
                       keyboard=ROOM_MENU)
    await after_join_hook(room_id)


async def handle_room_flows(user_id: str, display_name: str, text: str, state: dict) -> bool:
    flow = state["flow"]
    if text == "❌ انصراف":
        set_state(user_id, None)
        await send_message(user_id, "لغو شد.", keyboard=MAIN_MENU)
        return True
    if flow == "cr_game":
        game = LABELS_COST.get(text)
        if not game:
            await send_message(user_id, "یکی از بازی‌ها رو انتخاب کن.")
            return True
        cost = GAME_TYPES[game]["cost"]
        if get_coins(user_id) < cost:
            set_state(user_id, None)
            await send_message(user_id, f"❌ سکه‌ات کافی نیست.\n🪙 موجودی: {get_coins(user_id)} | هزینهٔ ساخت روم: {cost}\n"
                                        f"می‌تونی سکه بخری، هدیهٔ روزانه بگیری یا از «روم رندوم» رایگان بازی کنی.",
                               keyboard=[["💰 خرید سکه", "🎁 هدیه روزانه"], ["🎲 روم رندوم (رایگان)"], ["🔙 منوی اصلی"]])
            return True
        set_state(user_id, {"flow": "cr_name", "game": game, "mode": "create"})
        await send_message(user_id, "✏️ اسم روم رو بفرست:", keyboard=[["❌ انصراف"]])
        return True
    if flow == "cr_name":
        name = text.strip()
        if not name or len(name) > MAX_ROOM_NAME_LEN:
            await send_message(user_id, f"اسم باید بین ۱ تا {MAX_ROOM_NAME_LEN} کاراکتر باشه.")
            return True
        state.update({"flow": "cr_type", "name": name})
        set_state(user_id, state)
        await send_message(user_id, "🔒 نوع روم؟", keyboard=[["🌍 روم عمومی", "🔐 روم خصوصی"], ["❌ انصراف"]])
        return True
    if flow == "cr_type":
        if text not in ("🌍 روم عمومی", "🔐 روم خصوصی"):
            await send_message(user_id, "یکی از گزینه‌ها رو انتخاب کن.")
            return True
        state["rtype"] = "PUBLIC" if "عمومی" in text else "PRIVATE"
        if GAME_TYPES[state["game"]]["web"] and web_enabled():
            await _ask_ui_mode(user_id, state)
        else:
            await finalize_create(user_id, display_name, state, "TEXT")
        return True
    if flow in ("cr_ui", "rnd_ui"):
        if text not in ("🖼 گرافیکی (وب)", "💬 دکمه‌ای و متنی"):
            await send_message(user_id, "یکی از گزینه‌ها رو انتخاب کن.")
            return True
        ui = "WEB" if text.startswith("🖼") else "TEXT"
        if flow == "cr_ui":
            await finalize_create(user_id, display_name, state, ui)
        else:
            await finalize_random(user_id, display_name, state["game"], ui)
        return True
    if flow == "rnd_game":
        game = LABELS_FREE.get(text)
        if not game:
            await send_message(user_id, "یکی از بازی‌ها رو انتخاب کن.")
            return True
        if GAME_TYPES[game]["web"] and web_enabled():
            await _ask_ui_mode(user_id, {"flow": "rnd_ui", "game": game, "mode": "random"})
        else:
            await finalize_random(user_id, display_name, game, "TEXT")
        return True
    return False


# ============================== PANEL TEXTS ==================================

def panel_text(user_id: str) -> str:
    return f"{WELCOME}\n\n🪙 موجودی شما: {get_coins(user_id)} سکه"


async def show_panel(user_id: str):
    await send_message(user_id, panel_text(user_id), keyboard=MAIN_MENU)


async def show_profile(user_id: str):
    u = db.fetchone("SELECT * FROM users WHERE user_id=?", (user_id,))
    rank = db.fetchone("SELECT COUNT(*)+1 r FROM users WHERE score > ?", (u["score"],))
    await send_message(
        user_id,
        f"🙍 پروفایل {u['display_name']}\n🆔 شناسه: {user_id}\n\n🪙 سکه: {u['coins'] or 0}\n"
        f"🎮 بازی‌های شرکت‌کرده: {u['games_played']}\n💬 پاسخ‌های حقیقت: {u['truths_answered']}\n"
        f"🔥 جرئت‌های انجام‌شده: {u['dares_done']}\n⏰ نوبت‌های ردشده: {u['turns_missed']}\n"
        f"⭐ امتیاز کل: {u['score']}\n🏅 رتبه: {rank['r']}",
        keyboard=MAIN_MENU,
    )


def help_text() -> str:
    costs = "\n".join(f"{GAME_TYPES[k]['icon']} {GAME_TYPES[k]['title']}: {GAME_TYPES[k]['cost']} سکه"
                      for k in ("HOKM", "LUDO", "SNL", "NAF", "TOD"))
    return (
        "📘 راهنما\n\n"
        "🪙 سکه فقط برای «ساخت روم» لازمه. ورود به روم دیگران و «روم رندوم» همیشه رایگانه.\n\n"
        f"هزینهٔ ساخت روم:\n{costs}\n\n"
        f"🎁 هر روز {DAILY_GIFT_COINS} سکه هدیه می‌گیری.\n"
        f"💰 خرید سکه: هر سکه {fmt_num(COIN_PRICE_TOMAN)} تومان؛ تعداد رو بفرست، فیش بده و بعد از تایید مدیر شارژ می‌شی.\n\n"
        "🎮 بازی‌ها به دو شکل هستن: گرافیکی (لینک وب) یا دکمه‌ای و متنی داخل همین چت.\n"
        "• حکم: ۴ نفره، دو تیم، اول رسیدن به "
        f"{HOKM_TARGET_POINTS} امتیاز\n• منچ: ۲ تا ۴ نفر، با ۶ مهره میاد بیرون\n"
        "• مار و پله: ۲ تا ۴ نفر، اول رسیدن به ۱۰۰\n"
        f"• اسم و فامیل: ۲ تا ۸ نفر، {NAF_ROUNDS} دور (جواب باید با حرف دور شروع بشه)\n"
        "• جرئت یا حقیقت: با عکس/ویدیو/ویس\n\n"
        "اگه نوبتت رو چند بار از دست بدی، از بازی حذف می‌شی."
    )


# ============================== MAIN ROUTER ==================================

ROOM_PASSTHROUGH = {"🚪 خروج از روم", "👥 بازیکنان روم", "➕ دعوت از دوستان"}
_prev_handle_update = handle_update  # noqa: F821  (روتر قبلی)


async def handle_update(update: dict):  # noqa: F811
    cq = update.get("callback_query")
    if cq:
        await handle_callback(cq)
        return
    msg = update.get("message")
    if not msg:
        await _prev_handle_update(update)
        return
    chat = msg.get("chat", {}) or {}
    frm = msg.get("from", {}) or {}
    user_id = str(chat.get("id") or frm.get("id") or "")
    if not user_id:
        return
    display_name = frm.get("first_name") or frm.get("username") or f"کاربر {user_id}"
    get_or_create_user(user_id, display_name)

    state = get_state(user_id)
    text = (msg.get("text") or "").strip()

    # --- دریافت فیش ---
    if state and state.get("flow") == "buy_receipt" and (msg.get("photo") or msg.get("document")):
        await handle_receipt(user_id, msg, state)
        return
    if not text:
        await _prev_handle_update(update)
        return

    # --- شروع ---
    if text in ("/start", "start", "شروع", "🔙 منوی اصلی"):
        if active_game_room(user_id):
            await game_text_fallback(user_id)
            return
        set_state(user_id, None)
        await show_panel(user_id)
        return

    # --- دستورات مالک ---
    if is_owner(user_id):
        if text == "💳 خریدهای در انتظار":
            await owner_pending_purchases(user_id)
            return
        if text == "🪙 شارژ دستی":
            set_state(user_id, {"flow": "admin_charge"})
            await send_message(user_id, "آیدی کاربر و تعداد سکه رو بفرست (منفی = کسر):\nمثال: 12345 50")
            return
        if state and state.get("flow") == "admin_charge":
            set_state(user_id, None)
            await owner_manual_charge(user_id, text)
            return
        if text.startswith("شارژ "):
            await owner_manual_charge(user_id, text)
            return
        if text.startswith(("تایید_خرید", "رد_خرید")):
            parts = text.split()
            if len(parts) == 2:
                result = await decide_purchase(parts[1], text.startswith("تایید"), user_id)
                await send_message(user_id, result, keyboard=ADMIN_MENU)
                await _mark_owner_message(parts[1], result)
            else:
                await send_message(user_id, "فرمت: تایید_خرید <شناسه>")
            return

    # --- جریان‌های مرحله‌ای من ---
    if state:
        fl = state.get("flow", "")
        if fl.startswith("buy_") and await handle_buy_flow(user_id, display_name, text, state):
            return
        if fl.startswith(("cr_", "rnd_")) and await handle_room_flows(user_id, display_name, text, state):
            return

    # --- بازی در جریان (غیر TOD) ---
    gt = await game_text(user_id, text)
    if gt:
        return
    if text == "🏳️ خروج از بازی":
        await leave_room(user_id, display_name)
        return

    # --- منوها ---
    if text == "🎮 بازی":
        set_state(user_id, None)
        await send_message(user_id, "🎮 چی بازی کنیم؟", keyboard=GAME_MENU)
        return
    if text == "💰 خرید سکه":
        await start_buy_coins(user_id)
        return
    if text == "🎁 هدیه روزانه":
        await claim_daily(user_id)
        return
    if text in ("🙍 پروفایل من", "📊 آمار بازی‌ها"):
        await show_profile(user_id)
        return
    if text in ("📘 راهنما", "📘 راهنمای بازی"):
        await send_message(user_id, help_text(), keyboard=MAIN_MENU)
        return
    if text == "🎯 ساخت روم":
        await start_create_room(user_id)
        return
    if text in ("🎲 روم رندوم (رایگان)", "🎲 روم رندوم", "🔁 جست‌وجوی دوباره"):
        await start_random(user_id)
        return
    if text == "بازگشت" and not state:
        await show_panel(user_id)
        return

    if gt is False and text not in ROOM_PASSTHROUGH:
        await game_text_fallback(user_id)
        return
    await _prev_handle_update(update)


# ============================== WEB (GRAPHICAL GAMES) ========================

PAGE_HTML = r"""<!doctype html>
<html lang="fa" dir="rtl"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1">
<title>بازی</title>
<style>
:root{--bg:#10141c;--card:#1a2130;--line:#2b3447;--tx:#e8ecf4;--mut:#8b96ad;--ac:#4f8cff;--ok:#2ecc71;--bad:#e74c3c}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);font-family:Tahoma,Vazirmatn,sans-serif;font-size:15px}
#app{max-width:680px;margin:0 auto;padding:10px}
.bar{display:flex;justify-content:space-between;align-items:center;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:8px 12px;margin-bottom:8px}
.pill{display:inline-block;padding:2px 10px;border-radius:99px;background:#26304a;margin:2px}
.turn{outline:2px solid var(--ac)}
button.b{background:var(--ac);color:#fff;border:0;border-radius:10px;padding:10px 18px;font-size:16px;cursor:pointer;margin:4px}
button.b:disabled{background:#33405c;color:#7d889f;cursor:default}
.log{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:8px 12px;margin-top:8px;max-height:150px;overflow:auto;font-size:13px;color:var(--mut)}
.toast{position:fixed;bottom:16px;left:50%;transform:translateX(-50%);background:#000c;padding:8px 16px;border-radius:10px;z-index:9}
svg{width:100%;height:auto;background:#141b2a;border-radius:12px;border:1px solid var(--line)}
.card{display:inline-block;min-width:44px;padding:8px 6px;margin:3px;border-radius:8px;background:#f4f4f4;color:#222;text-align:center;font-weight:bold;font-size:18px;border:2px solid transparent;direction:ltr}
.card.red{color:#c0392b}.card.ok{border-color:var(--ok);cursor:pointer;transform:translateY(-4px)}.card.dim{opacity:.45}
.table{display:grid;grid-template-columns:1fr 1fr 1fr;grid-template-rows:auto auto auto;gap:6px;text-align:center;margin:8px 0}
.seat{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:6px;min-height:54px}
.inp{width:100%;background:#0e1320;border:1px solid var(--line);color:var(--tx);border-radius:8px;padding:9px;margin:3px 0 8px;font-size:16px}
.big{font-size:54px;text-align:center;color:var(--ac);font-weight:bold}
pre{white-space:pre-wrap;margin:0;font-family:inherit}
</style></head><body><div id="app"><div class="bar"><b id="ttl">...</b><span id="tmr"></span></div><div id="main"></div><div class="log" id="log"></div></div>
<script>
const T="__TOKEN__";let ver=-1,view=null,busy=false,t0=Date.now(),draft={},draftRound=-1;
const $=id=>document.getElementById(id);
function h(tag,a,...kids){const e=document.createElement(tag);for(const k in(a||{})){if(k==="class")e.className=a[k];else if(k.startsWith("on"))e.addEventListener(k.slice(2),a[k]);else e.setAttribute(k,a[k]);}
 for(const c of kids.flat()){if(c==null||c===false)continue;e.append(c.nodeType?c:document.createTextNode(String(c)));}return e;}
function sv(tag,a,...kids){const e=document.createElementNS("http://www.w3.org/2000/svg",tag);for(const k in(a||{})){if(k.startsWith("on"))e.addEventListener(k.slice(2),a[k]);else e.setAttribute(k,a[k]);}
 for(const c of kids.flat()){if(c==null)continue;e.append(c.nodeType?c:document.createTextNode(String(c)));}return e;}
function toast(m){const t=h("div",{class:"toast"},m);document.body.append(t);setTimeout(()=>t.remove(),2200);}
const COL=["#e74c3c","#2ecc71","#f1c40f","#3498db"];
async function poll(){try{const r=await fetch("/api/state?t="+T+"&v="+ver,{cache:"no-store"});const d=await r.json();
 if(d.unchanged){}else if(d.ended){showEnd(d);return;}else if(d.error){$("main").replaceChildren(h("p",{},"لینک نامعتبره."));return;}else{view=d;ver=d.version;t0=Date.now();render();}}catch(e){}
 setTimeout(poll,1500);}
function showEnd(d){$("ttl").textContent="پایان بازی";$("tmr").textContent="";$("main").replaceChildren(h("div",{class:"bar"},h("pre",{},d.result||"این بازی تموم شده.")));}
async function act(a){if(busy)return;busy=true;try{const r=await fetch("/api/action",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({t:T,action:a})});
 const d=await r.json();if(d.error)toast(d.error);else if(d.ended){showEnd(d);}else{view=d.view;ver=view.version;t0=Date.now();render();}}catch(e){toast("خطای شبکه")}finally{busy=false}}
function tick(){if(!view)return;const left=Math.max(0,(view.time_left||0)-Math.floor((Date.now()-t0)/1000));$("tmr").textContent=(view.over||!view.time_left)?"":("⏱ "+left);}
setInterval(tick,500);
function players(){return h("div",{},view.players.map((p,i)=>h("span",{class:"pill"+(view.turn===i&&!view.over?" turn":"")+(p.left?" dim":""),style:view.game==="SNL"||view.game==="LUDO"?"border-right:6px solid "+COL[p.color]:""},
 p.name+(i===view.me?" (تو)":"")+(p.pos!==undefined?" • "+p.pos:"")+(p.score!==undefined?" • "+p.score:""))));}
function render(){const g=view.game;$("ttl").textContent={SNL:"🐍 مار و پله",LUDO:"🎲 منچ",HOKM:"🃏 حکم",NAF:"📝 اسم و فامیل"}[g];
 const m=$("main");const body={SNL:rSNL,LUDO:rLUDO,HOKM:rHOKM,NAF:rNAF}[g]();m.replaceChildren(...body);
 $("log").replaceChildren(...(view.log||[]).slice().reverse().map(l=>h("div",{},l)));if(!(view.log||[]).length)$("log").style.display="none";else $("log").style.display="block";tick();}
/* ---------------- Snakes & Ladders ---------------- */
function cellXY(n){const r=Math.floor((n-1)/10),c0=(n-1)%10,c=r%2===0?c0:9-c0;return[c*60+30,(9-r)*60+30];}
function rSNL(){const s=sv("svg",{viewBox:"0 0 600 600"});
 for(let n=1;n<=100;n++){const[x,y]=cellXY(n);s.append(sv("rect",{x:x-30,y:y-30,width:60,height:60,fill:((Math.floor((n-1)/10)+n)%2)?"#1d2740":"#243150",stroke:"#2b3447"}),sv("text",{x:x-26,y:y-17,"font-size":11,fill:"#8b96ad"},n));}
 for(const a in view.ladders){const[x1,y1]=cellXY(+a),[x2,y2]=cellXY(view.ladders[a]);s.append(sv("line",{x1:x1-6,y1,x2:x2-6,y2,stroke:"#c98b4a","stroke-width":5}),sv("line",{x1:x1+6,y1,x2:x2+6,y2,stroke:"#c98b4a","stroke-width":5}));}
 for(const a in view.snakes){const[x1,y1]=cellXY(+a),[x2,y2]=cellXY(view.snakes[a]);s.append(sv("path",{d:`M${x1},${y1} Q${(x1+x2)/2+40},${(y1+y2)/2} ${x2},${y2}`,stroke:"#e74c3c","stroke-width":6,fill:"none","stroke-linecap":"round",opacity:.85}),sv("circle",{cx:x1,cy:y1,r:8,fill:"#e74c3c"}));}
 view.players.forEach((p,i)=>{if(p.left)return;const n=Math.max(p.pos,1),[x,y]=cellXY(n);const ox=(i%2)*16-8,oy=Math.floor(i/2)*16-8;s.append(sv("circle",{cx:x+ox,cy:y+oy+(p.pos===0?280:0),r:9,fill:COL[p.color],stroke:"#fff","stroke-width":2}));});
 return[players(),s,h("div",{class:"bar"},h("span",{},view.last_die?"🎲 آخرین تاس: "+view.last_die:""),
  h("button",{class:"b",disabled:!view.my_turn||view.over,onclick:()=>act({type:"roll"})},view.over?"پایان":(view.my_turn?"🎲 تاس بنداز":"منتظر نوبت..."))) ];}
/* ---------------- Ludo ---------------- */
const CX=300,CY=300,R=230;
function ang(i){return(-90+i*360/52)*Math.PI/180;}
function ringXY(i){return[CX+R*Math.cos(ang(i)),CY+R*Math.sin(ang(i))];}
function homeXY(c,j){const i=((13*c-1)%52+52)%52;const r=R-30*(j+1);return[CX+r*Math.cos(ang(i)),CY+r*Math.sin(ang(i))];}
function baseXY(c,k){const a=ang(13*c);const bx=CX+268*Math.cos(a),by=CY+268*Math.sin(a);return[bx+(k%2?13:-13),by+(k<2?-13:13)];}
function pieceXY(c,rel,k){if(rel===-1)return baseXY(c,k);if(rel<=50)return ringXY((13*c+rel)%52);return homeXY(c,rel-51);}
function rLUDO(){const s=sv("svg",{viewBox:"0 0 600 600"});
 for(let i=0;i<52;i++){const[x,y]=ringXY(i);const st=i%13===0;s.append(sv("circle",{cx:x,cy:y,r:st?11:8,fill:st?COL[i/13]:"#26304a",stroke:"#3a4766"}));if(i%13===8)s.append(sv("text",{x:x-6,y:y+5,"font-size":13,fill:"#f1c40f"},"★"));}
 for(let c=0;c<4;c++)for(let j=0;j<6;j++){const[x,y]=homeXY(c,j);s.append(sv("circle",{cx:x,cy:y,r:9,fill:COL[c],opacity:.35+j*.1}));}
 s.append(sv("circle",{cx:CX,cy:CY,r:26,fill:"#0e1320",stroke:"#3a4766"}),sv("text",{x:CX-9,y:CY+7,"font-size":20},"🏁"));
 const seen={};const pcs=[];
 view.players.forEach((p,pi)=>{if(p.left)return;p.pieces.forEach((rel,k)=>{const[x,y]=pieceXY(p.color,rel,k);const key=Math.round(x)+","+Math.round(y);seen[key]=(seen[key]||0)+1;
  const mv=pi===view.me&&view.movable.includes(k);pcs.push([x+(seen[key]-1)*5,y-(seen[key]-1)*5,p.color,k,mv]);});});
 pcs.forEach(([x,y,c,k,mv])=>{s.append(sv("circle",{cx:x,cy:y,r:11,fill:COL[c],stroke:mv?"#fff":"#0008","stroke-width":mv?4:2,style:mv?"cursor:pointer":"",onclick:mv?()=>act({type:"move",piece:k}):()=>{}}),sv("text",{x:x-4,y:y+5,"font-size":12,fill:"#000","pointer-events":"none"},k+1));});
 const msg=view.over?"پایان":!view.my_turn?"منتظر نوبت...":(view.phase==="move"?"روی یک مهرهٔ درخشان بزن (عدد "+view.die+")":"🎲 تاس بنداز");
 return[players(),s,h("div",{class:"bar"},h("span",{},view.die?"🎲 "+view.die:""),h("button",{class:"b",disabled:!(view.my_turn&&view.phase==="roll"),onclick:()=>act({type:"roll"})},msg))];}
/* ---------------- Hokm ---------------- */
const SY={S:"♠",H:"♥",D:"♦",C:"♣"},FA={S:"پیک",H:"دل",D:"خشت",C:"گشنیز"},RK={11:"J",12:"Q",13:"K",14:"A"};
function cardEl(c,ok,dim){const r=+c.slice(1);return h("span",{class:"card"+(c[0]==="H"||c[0]==="D"?" red":"")+(ok?" ok":"")+(dim?" dim":""),onclick:ok?()=>act({type:"play",card:c}):null},(RK[r]||r)+SY[c[0]]);}
function rHOKM(){const me=view.me,pos=(o)=>(me+o)%4;const tr={};(view.trick||[]).forEach(([s,c])=>tr[s]=c);
 const seat=(s)=>h("div",{class:"seat"+(view.turn===s&&!view.over?" turn":"")},(s===view.hakem?"👑 ":"")+view.players[s].name+(s%2===me%2?" 🤝":""),h("div",{},tr[s]?cardEl(tr[s]):h("small",{style:"color:var(--mut)"},view.counts[s]+" کارت")));
 const lt=view.last_trick?h("div",{style:"font-size:12px;color:var(--mut)"},"دست قبل: ",view.last_trick.map(([s,c])=>cardEl(c))):null;
 const tbl=h("div",{class:"table"},h("div"),seat(pos(2)),h("div"),seat(pos(3)),h("div",{class:"seat"},view.trump?"حکم: "+SY[view.trump]+" "+FA[view.trump]:"حکم: —",h("div",{},"امتیاز: "+view.points[0]+" - "+view.points[1]+" / "+view.target),h("div",{},"دست: "+view.tricks[0]+" - "+view.tricks[1]),lt),seat(pos(1)),h("div"),seat(me),h("div"));
 const ctl=[];if(view.phase==="trump"&&view.my_turn)ctl.push(h("div",{class:"bar"},"👑 حکم رو انتخاب کن: ",["S","H","D","C"].map(s=>h("button",{class:"b",onclick:()=>act({type:"trump",suit:s})},SY[s]+" "+FA[s]))));
 else if(view.phase==="trump")ctl.push(h("div",{class:"bar"},"⏳ حاکم داره حکم رو انتخاب می‌کنه..."));
 const hand=h("div",{style:"text-align:center"},view.hand.map(c=>cardEl(c,view.legal.includes(c),view.my_turn&&view.phase==="play"&&!view.legal.includes(c))));
 return[tbl,...ctl,hand,view.over?h("div",{class:"bar"},"🏁 بازی تموم شد"):null];}
/* ---------------- Name & Family ---------------- */
function rNAF(){if(draftRound!==view.round){draft={};draftRound=view.round;}
 const els=[h("div",{class:"big"},view.letter||"—"),h("div",{class:"bar"},"دور "+view.round+" از "+view.rounds),
  h("div",{},view.players.map(p=>h("span",{class:"pill"+(p.left?" dim":"")},(p.done&&view.phase==="answer"?"✅ ":"")+p.name+" • "+p.score)))];
 if(view.phase==="answer"&&!view.done&&!view.over){const f=h("div",{});view.cats.forEach(c=>{f.append(h("label",{},c),h("input",{class:"inp",maxlength:40,value:draft[c]||"",oninput:e=>{draft[c]=e.target.value;},autocomplete:"off"}));});
  f.append(h("button",{class:"b",onclick:()=>act({type:"submit",answers:draft})},"✅ ثبت و پایان (استپ)"));els.push(f);}
 else if(view.phase==="answer"&&!view.over)els.push(h("div",{class:"bar"},"✅ جواب‌هات ثبت شد؛ منتظر بقیه..."));
 else if(view.phase==="between")els.push(h("div",{class:"bar"},"⏭ دور بعد به‌زودی شروع می‌شه..."));
 if(view.last_result&&view.last_result.length&&view.phase!=="answer"||view.over)els.push(h("div",{class:"log",style:"max-height:none;color:var(--tx)"},h("pre",{},(view.last_result||[]).join("\n"))));
 return els;}
poll();
</script></body></html>
"""


def _token_ctx(token: str):
    return db.fetchone("SELECT * FROM web_tokens WHERE token=?", (token,)) if token else None


def _web_view(room, uid: str) -> Optional[dict]:
    st = _load(room)
    if not st or room["status"] != "IN_PROGRESS":
        return None
    eng = ENGINES[room["game_type"]]
    if _idx(st, uid) < 0:
        return None
    view = eng.view(st, uid)
    dl = eng.deadline(st)
    if "time_left" not in view:
        view["time_left"] = max(0, int(dl - now())) if dl else 0
    view["version"] = room["game_version"]
    return view


async def web_index(request):
    token = request.match_info.get("token", "")
    if not _token_ctx(token):
        return web.Response(text="لینک نامعتبر است.", status=404)
    return web.Response(text=PAGE_HTML.replace("__TOKEN__", token), content_type="text/html",
                        headers={"Cache-Control": "no-store"})


async def web_state(request):
    ctx = _token_ctx(request.query.get("t", ""))
    if not ctx:
        return web.json_response({"error": "invalid"}, status=404)
    room = _room(ctx["room_id"])
    view = _web_view(room, ctx["user_id"]) if room else None
    if view is None:
        return web.json_response({"ended": True, "result": (room["last_result"] if room else "") or ""})
    try:
        client_v = int(request.query.get("v", "-1"))
    except ValueError:
        client_v = -1
    if client_v == view["version"]:
        return web.json_response({"unchanged": True})
    return web.json_response(view)


async def web_action(request):
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "bad request"}, status=400)
    ctx = _token_ctx(str(body.get("t", "")))
    action = body.get("action")
    if not ctx or not isinstance(action, dict):
        return web.json_response({"error": "invalid"}, status=400)
    err = await game_action(ctx["room_id"], ctx["user_id"], action)
    if err:
        return web.json_response({"error": err})
    room = _room(ctx["room_id"])
    view = _web_view(room, ctx["user_id"]) if room else None
    if view is None:
        return web.json_response({"ended": True, "result": (room["last_result"] if room else "") or ""})
    return web.json_response({"view": view})


async def web_health(request):
    return web.Response(text="ok")


async def keepalive_loop():
    """روی پلن رایگان Render سرویس بعد از ۱۵ دقیقه بی‌ترافیک می‌خوابه؛ هر ۱۰ دقیقه به آدرس خودش سر می‌زنه."""
    while True:
        await asyncio.sleep(600)
        try:
            session = await get_http_session()
            async with session.get(KEEPALIVE_URL.rstrip("/") + "/health") as r:
                await r.read()
        except Exception as e:
            log.warning("keepalive ناموفق: %s", type(e).__name__)


async def start_web_server():
    if not web_enabled():
        log.info("WEB_BASE_URL خالیه؛ حالت گرافیکی غیرفعال است و فقط بازی متنی کار می‌کنه.")
        return None
    app = web.Application()
    app.router.add_get("/", web_health)
    app.router.add_get("/health", web_health)
    app.router.add_get("/g/{token}", web_index)
    app.router.add_get("/api/state", web_state)
    app.router.add_post("/api/action", web_action)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, WEB_HOST, WEB_PORT).start()
    log.info("وب‌سرور بازی‌ها روی %s:%s بالا اومد (%s)", WEB_HOST, WEB_PORT, WEB_BASE_URL)
    return runner


ADMIN_MENU.insert(2, ["💳 خریدهای در انتظار", "🪙 شارژ دستی"])


# ============================== MAIN LOOP ===================================


async def polling_loop():
    offset = None
    backoff = 1.0
    log.info("شروع Long Polling روی Soroush Plus Bot API ...")
    while True:
        params = {"timeout": POLL_TIMEOUT, "limit": 50}
        if offset is not None:
            params["offset"] = offset
        res = await api_call("getUpdates", params, timeout=POLL_TIMEOUT + 15, get=True)
        if not res.get("ok", False):
            log.warning(
                "getUpdates ناموفق بود: %s — %.0f ثانیه صبر می‌کنیم.",
                res.get("error"), backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
            continue
        backoff = 1.0
        updates = res.get("result", []) or []
        for upd in updates:
            uid = upd.get("update_id")
            if uid is not None:
                offset = int(uid) + 1
            try:
                asyncio.ensure_future(handle_update(upd))
            except Exception:
                log.exception("خطا هنگام پردازش آپدیت")


async def seed_sample_questions():
    cnt = db.fetchone("SELECT COUNT(*) c FROM questions")["c"]
    if cnt > 0:
        return
    samples_truth = [
        "آخرین باری که دروغ گفتی کی بود و چرا؟",
        "اگه یه قدرت خاص داشتی، چی بود؟",
        "خجالت‌آورترین خاطره‌ات چیه؟",
        "کسی رو مخفیانه دوست داشتی؟",
        "بزرگ‌ترین ترس زندگیت چیه؟",
    ]
    samples_dare = [
        "یک ویس خنده‌دار ضبط کن و بفرست!",
        "یک عکس بامزه از خودت با یه ادای مسخره بفرست.",
        "یک آهنگ رو ۱۰ ثانیه برامون بخون و ویس بفرست.",
        "با صدای یک شخصیت کارتونی چندتا جمله بگو و ویس بفرست.",
        "یک ویدیوی کوتاه از خودت در حال رقصیدن بفرست.",
    ]
    for t in samples_truth:
        db.execute(
            "INSERT INTO questions(question_id,qtype,text,category,difficulty,active,created_by,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (new_id("q"), "TRUTH", t, "عمومی", "متوسط", 1, "SEED", now()),
        )
    for d in samples_dare:
        db.execute(
            "INSERT INTO questions(question_id,qtype,text,category,difficulty,active,created_by,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (new_id("q"), "DARE", d, "عمومی", "متوسط", 1, "SEED", now()),
        )
    log.info("سؤالات نمونه بارگذاری شدند.")


async def main():
    if BOT_TOKEN.startswith("PUT_YOUR") or OWNER_ID.startswith("PUT_YOUR"):
        log.error(
            "لطفاً اول BOT_TOKEN و OWNER_ID رو در بالای همین فایل (بخش CONFIG) تنظیم کن."
        )
        return
    me = await api_get_me()
    if not me.get("ok", False):
        log.error(
            "اتصال اولیه به Bot API ناموفق بود (%s). توکن یا اتصال شبکه رو چک کن.",
            me.get("error"),
        )
        # عمداً متوقف نمی‌شیم — شاید فقط یه قطعی موقتیه؛ polling_loop با
        # backoff خودش دوباره امتحان می‌کنه.
    else:
        bot_info = me.get("result", {}) or {}
        log.info("اتصال برقرار شد: ربات @%s", bot_info.get("username", "?"))

    await delete_webhook()
    await seed_sample_questions()
    await recover_pending_turns()
    await recover_generic_games()
    await start_web_server()
    if KEEPALIVE_URL:
        asyncio.ensure_future(keepalive_loop())
    try:
        await polling_loop()
    finally:
        global _http_session
        if _http_session and not _http_session.closed:
            await _http_session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("ربات متوقف شد.")
