import asyncio
import json
import logging
import random
import sqlite3
import string
import time
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import quote

import aiohttp

# ============================== CONFIG ===================================
# این مقادیر را خودت اینجا تنظیم کن (طبق درخواست صریح کاربر، نه .env)

BOT_TOKEN = "70840875:-xCuHRn1bOT8xhejK6tujCSE_slKii578gA"   # از بات‌ساز سروش‌پلاس بگیر
OWNER_ID = "64621354"      # شناسهٔ کاربری خودت در سروش

# توجه: توکن دیگه مستقیم توی API_BASE چسبونده نمی‌شه (مثل نمونه‌ای که
# کاربر داد)، بلکه هر فراخوانی جدا quote و اضافه می‌شه — همون الگوی
# SoroushAdapter._call در نمونه.
API_BASE = "https://api.splus.ir/bot"
POLL_TIMEOUT = 30
DB_PATH = "truth_or_dare.db"

TURN_TIMEOUT_SECONDS = 60
MIN_PLAYERS_TO_START = 2
MAX_ROOM_NAME_LEN = 40
MAX_SKIPS_PER_TURN = 1          # تعداد دفعات مجاز «رد کردن سؤال» در هر نوبت
DEFAULT_ROOM_CAPACITY = 10

POINTS_TRUTH = 10
POINTS_DARE = 15
POINTS_PARTICIPATION = 20

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
    ["🎯 ساخت روم", "🔑 ورود به روم"],
    ["🎲 روم رندوم", "👥 دوستان"],
    ["🙍 پروفایل من", "📊 آمار بازی‌ها"],
    ["📘 راهنمای بازی"],
]

TRUTH_DARE_MENU = [["حقیقت", "جرئت"], ["رد کردن سؤال"]]

WELCOME = (
    "🎉 به دنیای جرئت یا حقیقت خوش اومدی!\n\n"
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
        cnt = room_member_count(room_id)
        if cnt >= room["capacity"]:
            return "ظرفیت این روم تکمیله."
        # توجه: join_order نباید از روی تعداد فعلی اعضا (cnt) ساخته بشه،
        # چون اگه یک نفر قبلاً از روم خارج شده باشه، cnt کوچیک‌تر از
        # بزرگ‌ترین join_order موجوده و عضو جدید همون عدد یک نفر دیگه رو
        # می‌گیره (تداخل). همیشه از MAX(join_order)+1 استفاده می‌کنیم تا
        # هر عضو، چه قدیمی چه جدید، یک join_order یکتا و پایدار داشته باشه
        # و چرخهٔ نوبت‌دهی به‌هم نریزه.
        next_order = db.fetchone(
            "SELECT COALESCE(MAX(join_order), -1) + 1 AS nxt FROM room_members WHERE room_id=?",
            (room_id,),
        )["nxt"]
        db.execute(
            "INSERT INTO room_members(room_id,user_id,join_order,joined_at) VALUES (?,?,?,?)",
            (room_id, user_id, next_order, now()),
        )
    await enter_room_flow(user_id, display_name, room_id)
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
    new_owner = None
    async with db.lock:
        room = db.fetchone("SELECT * FROM rooms WHERE room_id=?", (room_id,))
        db.execute("DELETE FROM room_members WHERE room_id=? AND user_id=?", (room_id, user_id))
        db.execute("UPDATE users SET current_room_id=NULL WHERE user_id=?", (user_id,))
        remaining = room_members(room_id)
        if not remaining:
            db.execute("UPDATE rooms SET status='CLOSED' WHERE room_id=?", (room_id,))
        elif room and room["owner_id"] == user_id:
            new_owner = remaining[0]
            db.execute("UPDATE rooms SET owner_id=? WHERE room_id=?", (new_owner["user_id"], room_id))
    if new_owner is not None:
        await send_message(new_owner["user_id"], "👑 چون گاد قبلی خارج شد، مالکیت روم به تو منتقل شد!")
        await broadcast(room_id, f"👑 مالکیت روم به {new_owner['display_name']} منتقل شد.")
    if remaining:
        await broadcast(room_id, f"🚪 {display_name} از روم خارج شد.")
    await send_message(user_id, "از روم خارج شدی.", keyboard=MAIN_MENU)

    # اگه همین بازیکن وسط یک بازی در حال اجرا خارج شده (نه نوبت خودش، چون
    # اون حالت بالاتر مسدوده)، باید بررسی کنیم که آیا بازی هنوز می‌تونه ادامه
    # پیدا کنه یا نه، و اگه آخرین نفری که منتظرش بودیم خودش بوده، نوبت رو به
    # نفر بعدی بدیم؛ وگرنه این روم تا ابد منتظر کسی می‌مونه که دیگه توی روم
    # نیست (باگ «نوبت گیر می‌کنه چون کسی از بازی رفته بیرون»).
    if room and room["status"] == "IN_PROGRESS":
        pending_turn = db.fetchone(
            "SELECT 1 FROM turns WHERE room_id=? AND status IN ('WAITING_CHOICE','WAITING_ANSWER')",
            (room_id,),
        )
        if not pending_turn:
            await begin_next_turn(room_id)


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
    # current_turn_order دیگه یک شمارندهٔ مطلق نیست؛ آخرین join_order
    # کسی که نوبتش بوده رو نگه می‌داره. -1 یعنی «هنوز هیچ نوبتی داده
    # نشده» (چرخه از کوچیک‌ترین join_order شروع می‌شه).
    db.execute(
        "UPDATE rooms SET status='IN_PROGRESS', accepting_new=0, current_turn_order=-1, "
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
    async with db.lock:
        room = db.fetchone("SELECT * FROM rooms WHERE room_id=?", (room_id,))
        if room is None or room["status"] != "IN_PROGRESS":
            return
        members = room_members(room_id)
        if not members:
            db.execute("UPDATE rooms SET status='CLOSED' WHERE room_id=?", (room_id,))
            return
        if len(members) < MIN_PLAYERS_TO_START:
            # end_game خودش db.lock نمی‌گیره (پایین‌تر هم اصلاح شده)، پس
            # اینجا فقط تصمیم رو می‌گیریم و بعد از آزاد شدن قفل فراخوانی
            # می‌کنیم تا دِدلاک نشه.
            need_end = True
        else:
            need_end = False

        if not need_end:
            # نکتهٔ مهم: نوبت نباید با یک شمارندهٔ مطلق mod طول لیست اعضا
            # حساب بشه، چون با خروج هر بازیکن، طول لیست عوض می‌شه و این
            # باعث می‌شه نوبت بعضی‌ها رد (skip) بشه یا بعضی‌ها پشت‌سرهم دوبار
            # نوبت بگیرن. به‌جاش «اولین عضو با join_order بزرگ‌تر از آخرین
            # نفری که نوبتش بوده» رو پیدا می‌کنیم؛ اگه کسی خارج شده باشه،
            # فقط از چرخه حذف می‌شه و بقیه دقیقاً سر جای خودشون می‌مونن.
            last_join_order = room["current_turn_order"]
            member = next((m for m in members if m["join_order"] > last_join_order), None)
            if member is None:
                member = members[0]  # رسیدیم ته چرخه؛ از اول (کوچیک‌ترین join_order) شروع کن
            db.execute(
                "UPDATE rooms SET current_turn_order=? WHERE room_id=?",
                (member["join_order"], room_id),
            )

    if need_end:
        await end_game(room_id, reason="تعداد بازیکنان فعال کمتر از حد مجاز شد.")
        return

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
        if not text.isdigit() or int(text) < room_member_count(room_id):
            await send_message(user_id, "عدد نامعتبره یا کمتر از تعداد فعلی بازیکنانه.")
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
