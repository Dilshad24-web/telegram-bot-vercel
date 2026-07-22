"""
ClickCash Telegram Bot — Vercel Webhook Version
================================================
Changes from polling version:
- Replaced MemoryStorage → FirebaseFSMStorage (required for serverless:
  each invocation is a fresh process; states must survive between calls)
- Removed start_polling loop → FastAPI webhook endpoint
- Bot/Dispatcher initialized at module level (Vercel warm-start safe)
- All core logic (Downloaded List, Admin Panel, FSM flows) UNCHANGED
"""

import asyncio
import concurrent.futures
import csv
import html as _html
import io
import logging
import os
import socket
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, Optional

import requests
from fastapi import FastAPI, Request, Response
from requests.adapters import HTTPAdapter
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import BaseStorage, StorageKey, StateType
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    Update,
)

# ─────────────────────────────────────────────
#  CONFIG  ← Edit freely
# ─────────────────────────────────────────────

BOT_TOKEN             = "8987131690:AAFQedS-Y5nfHienuByLq2EiFoaFdHxPSXY"
ADMIN_CHAT_ID         = 7917521776
FIREBASE_DATABASE_URL = "https://clickcash-782cf-default-rtdb.firebaseio.com"
CONTACT_SUPPORT_LINK  = "https://t.me/example"   # ← Update via Admin → Settings → Support Link

IMAGE_REWARD = 10    # ₹ (overridable from admin settings)
MIN_WITHDRAW = 100   # ₹ (overridable from admin settings)

RULES_TEXT = (
    "📋 <b>Rules</b>\n\n"
    "1. Upload only original images you own.\n"
    "2. Images must be JPG, JPEG, PNG or WEBP format.\n"
    "3. Send images as <b>Document</b>, not as a photo.\n"
    "4. Duplicate or low-quality images will be rejected.\n"
    "5. Spamming submissions leads to a permanent ban.\n"
    "6. Minimum withdraw amount is ₹100.\n"
    "7. Withdrawal is processed within 24–48 hours.\n"
    "8. Admin decisions are final."
)

ALLOWED_MIMES = {"image/jpeg", "image/png", "image/webp"}
ALLOWED_EXTS  = {"jpg", "jpeg", "png", "webp"}

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("clickcash")

# ─────────────────────────────────────────────
#  PROXY DETECTION
# ─────────────────────────────────────────────

_PYTHONANYWHERE_PROXY = "http://proxy.server:3128"


def _detect_proxy() -> Optional[str]:
    """
    Automatically detect whether a proxy is needed.

    Resolution order:
    1. Respect any proxy already set in the environment (HTTPS_PROXY / HTTP_PROXY).
    2. Try a direct TCP connection to api.telegram.org:443 — if it succeeds, no proxy
       is needed (premium / open-internet server).
    3. Fall back to the PythonAnywhere free-tier proxy.

    This means the same code runs unchanged on free-tier (proxy required) and
    paid/local environments (direct connection).
    """
    for key in ("HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy"):
        val = os.environ.get(key, "").strip()
        if val:
            logger.info("Proxy detected from environment variable %s=%s", key, val)
            return val

    try:
        conn = socket.create_connection(("api.telegram.org", 443), timeout=5)
        conn.close()
        logger.info("Direct internet access confirmed — no proxy needed.")
        return None
    except OSError:
        logger.info(
            "Direct connection to api.telegram.org failed. "
            "Falling back to PythonAnywhere proxy: %s",
            _PYTHONANYWHERE_PROXY,
        )
        return _PYTHONANYWHERE_PROXY


PROXY: Optional[str] = _detect_proxy()


# ─────────────────────────────────────────────
#  FIREBASE REST API HELPERS
# ─────────────────────────────────────────────

def _build_requests_session() -> requests.Session:
    s = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=20,
        pool_maxsize=40,
        max_retries=3,
    )
    s.mount("https://", adapter)
    s.mount("http://",  adapter)
    if PROXY:
        s.proxies = {"http": PROXY, "https": PROXY}
        logger.info("requests.Session: proxy configured → %s", PROXY)
    return s


SESSION = _build_requests_session()

def fb_url(path: str) -> str:
    return f"{FIREBASE_DATABASE_URL}/{path}.json"

def _fb_get_sync(path: str) -> Any:
    try:
        r = SESSION.get(fb_url(path), timeout=10)
        if r.status_code == 200:
            return r.json()
        logger.warning("Firebase GET [%s] status=%s", path, r.status_code)
        return None
    except Exception as e:
        logger.error("Firebase GET error [%s]: %s", path, e)
        return None

def _fb_set_sync(path: str, data: Any) -> bool:
    try:
        r = SESSION.put(fb_url(path), json=data, timeout=10)
        return r.status_code == 200
    except Exception as e:
        logger.error("Firebase SET error [%s]: %s", path, e)
        return False

def _fb_update_sync(path: str, data: Dict) -> bool:
    try:
        r = SESSION.patch(fb_url(path), json=data, timeout=10)
        if r.status_code != 200:
            logger.warning(
                "Firebase UPDATE non-200 [%s] status=%s body=%s",
                path, r.status_code, r.text[:200],
            )
            return False
        return True
    except Exception as e:
        logger.error("Firebase UPDATE error [%s]: %s", path, e)
        return False

def _fb_delete_sync(path: str) -> bool:
    try:
        r = SESSION.delete(fb_url(path), timeout=10)
        return r.status_code == 200
    except Exception as e:
        logger.error("Firebase DELETE error [%s]: %s", path, e)
        return False

async def fb_get(path: str) -> Any:
    return await asyncio.to_thread(_fb_get_sync, path)

async def fb_set(path: str, data: Any) -> bool:
    return await asyncio.to_thread(_fb_set_sync, path, data)

async def fb_update(path: str, data: Dict) -> bool:
    return await asyncio.to_thread(_fb_update_sync, path, data)

async def fb_delete(path: str) -> bool:
    return await asyncio.to_thread(_fb_delete_sync, path)

# ─────────────────────────────────────────────
#  UTILITIES
# ─────────────────────────────────────────────

def _now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def _new_id() -> str:
    return str(uuid.uuid4())[:8].upper()

def _fmt(dt: str) -> str:
    return dt[:19] if dt else "N/A"

def he(s: Any) -> str:
    """HTML-escape any user-provided string."""
    return _html.escape(str(s)) if s is not None else "N/A"

def to_int(val: Any) -> int:
    try:
        return int(val)
    except Exception:
        return 0

# ─────────────────────────────────────────────
#  DATABASE HELPERS
# ─────────────────────────────────────────────

class DB:

    @staticmethod
    async def get_settings() -> Dict:
        data = await fb_get("settings") or {}
        return {
            "image_reward":            to_int(data.get("image_reward", IMAGE_REWARD)),
            "min_withdraw":            to_int(data.get("min_withdraw", MIN_WITHDRAW)),
            "rules":                   data.get("rules",        RULES_TEXT),
            "support_link":            data.get("support_link", CONTACT_SUPPORT_LINK),
            "upload_enabled":          bool(data.get("upload_enabled",          True)),
            "withdraw_upi_enabled":    bool(data.get("withdraw_upi_enabled",    True)),
            "withdraw_amazon_enabled": bool(data.get("withdraw_amazon_enabled", True)),
        }

    @staticmethod
    async def set_setting(key: str, value: Any) -> bool:
        ok = await fb_set(f"settings/{key}", value)
        if not ok:
            logger.error("set_setting FAILED for key=%s", key)
        return ok

    @staticmethod
    async def get_user(user_id: int) -> Dict:
        return await fb_get(f"users/{user_id}") or {}

    @staticmethod
    async def ensure_user(user_id: int, username: str = "") -> Dict:
        user = await fb_get(f"users/{user_id}")
        if not user:
            user = {
                "user_id":    user_id,
                "username":   username,
                "balance":    0,
                "banned":     False,
                "created_at": _now(),
            }
            await fb_set(f"users/{user_id}", user)
            logger.info("New user registered: %s", user_id)
        else:
            if username and user.get("username") != username:
                await fb_update(f"users/{user_id}", {"username": username})
        return user

    @staticmethod
    async def update_user(user_id: int, data: Dict) -> None:
        await fb_update(f"users/{user_id}", data)

    @staticmethod
    async def get_balance(user_id: int) -> int:
        val = await fb_get(f"users/{user_id}/balance")
        return to_int(val)

    @staticmethod
    async def add_balance(user_id: int, amount: int) -> int:
        current = await DB.get_balance(user_id)
        new_bal = max(0, current + amount)
        await fb_set(f"users/{user_id}/balance", new_bal)
        logger.info("Balance update uid=%s  %s%s → %s", user_id, "+" if amount >= 0 else "", amount, new_bal)
        return new_bal

    @staticmethod
    async def is_banned(user_id: int) -> bool:
        val = await fb_get(f"users/{user_id}/banned")
        return bool(val)

    @staticmethod
    async def get_all_users() -> Dict:
        return await fb_get("users") or {}

    @staticmethod
    async def create_submission(data: Dict) -> str:
        sub_id = _new_id()
        await fb_set(f"submissions/{sub_id}", {**data, "submission_id": sub_id})
        return sub_id

    @staticmethod
    async def get_submission(sub_id: str) -> Dict:
        return await fb_get(f"submissions/{sub_id}") or {}

    @staticmethod
    async def update_submission(sub_id: str, data: Dict) -> None:
        await fb_update(f"submissions/{sub_id}", data)

    @staticmethod
    async def get_all_submissions() -> Dict:
        return await fb_get("submissions") or {}

    @staticmethod
    async def get_pending_submissions() -> Dict:
        all_s = await fb_get("submissions") or {}
        return {k: v for k, v in all_s.items() if isinstance(v, dict) and v.get("status") == "pending"}

    @staticmethod
    async def get_user_submissions(user_id: int) -> Dict:
        all_s = await fb_get("submissions") or {}
        result = {}
        for k, v in all_s.items():
            if not isinstance(v, dict):
                continue
            try:
                if int(v.get("user_id", -1)) == user_id:
                    result[k] = v
            except Exception:
                pass
        return result

    @staticmethod
    async def create_withdrawal(data: Dict) -> str:
        wid = _new_id()
        await fb_set(f"withdrawals/{wid}", {**data, "withdrawal_id": wid})
        return wid

    @staticmethod
    async def get_withdrawal(wid: str) -> Dict:
        return await fb_get(f"withdrawals/{wid}") or {}

    @staticmethod
    async def update_withdrawal(wid: str, data: Dict) -> None:
        await fb_update(f"withdrawals/{wid}", data)

    @staticmethod
    async def get_downloaded_submissions() -> Dict:
        all_s = await fb_get("submissions") or {}
        return {
            k: v for k, v in all_s.items()
            if isinstance(v, dict) and v.get("status") == "downloaded"
        }

    @staticmethod
    async def get_pending_withdrawals() -> Dict:
        all_w = await fb_get("withdrawals") or {}
        return {k: v for k, v in all_w.items() if isinstance(v, dict) and v.get("status") == "pending"}

    @staticmethod
    async def get_all_withdrawals() -> Dict:
        return await fb_get("withdrawals") or {}

    @staticmethod
    async def get_user_withdrawals(user_id: int) -> Dict:
        all_w = await fb_get("withdrawals") or {}
        result = {}
        for k, v in all_w.items():
            if not isinstance(v, dict):
                continue
            try:
                if int(v.get("user_id", -1)) == user_id:
                    result[k] = v
            except Exception:
                pass
        return result


# ─────────────────────────────────────────────
#  FIREBASE FSM STORAGE
#  Replaces MemoryStorage for serverless use.
#  FSM states are stored under Firebase path:
#    fsm/{bot_id}/{chat_id}/{user_id}/state
#    fsm/{bot_id}/{chat_id}/{user_id}/data
# ─────────────────────────────────────────────

class FirebaseFSMStorage(BaseStorage):
    """
    Persistent FSM storage backed by Firebase Realtime Database.

    Why needed on Vercel:
      MemoryStorage lives in process RAM. Each Vercel serverless invocation
      is a fresh (or occasionally reused) process — states written in one
      request may be gone by the next. FirebaseFSMStorage writes every
      state change to Firebase so FSM flows survive across invocations.

    Firebase path layout:
      fsm/<bot_id>/<chat_id>/<user_id>/state  → string or null
      fsm/<bot_id>/<chat_id>/<user_id>/data   → dict or {}
    """

    def _path(self, key: StorageKey) -> str:
        return f"fsm/{key.bot_id}/{key.chat_id}/{key.user_id}"

    async def set_state(self, key: StorageKey, state: StateType = None) -> None:
        path = f"{self._path(key)}/state"
        if state is None:
            await fb_set(path, None)
        else:
            state_str = state.state if hasattr(state, "state") else str(state)
            await fb_set(path, state_str)

    async def get_state(self, key: StorageKey) -> Optional[str]:
        val = await fb_get(f"{self._path(key)}/state")
        return val if isinstance(val, str) else None

    async def set_data(self, key: StorageKey, data: Dict[str, Any]) -> None:
        await fb_set(f"{self._path(key)}/data", data or {})

    async def get_data(self, key: StorageKey) -> Dict[str, Any]:
        result = await fb_get(f"{self._path(key)}/data")
        return result if isinstance(result, dict) else {}

    async def close(self) -> None:
        pass  # Nothing to close — Firebase is stateless HTTP


# ─────────────────────────────────────────────
#  FSM STATES
# ─────────────────────────────────────────────

class UploadStates(StatesGroup):
    waiting_document = State()
    waiting_title    = State()
    waiting_keywords = State()   # MANDATORY — no skip
    waiting_category = State()   # Optional — can skip


class WithdrawStates(StatesGroup):
    waiting_upi       = State()
    confirming        = State()
    waiting_amazon    = State()
    confirming_amazon = State()


class AdminStates(StatesGroup):
    setting_image_reward  = State()
    setting_min_withdraw  = State()
    setting_rules         = State()
    setting_support_link  = State()
    searching_user        = State()
    add_balance_amount    = State()
    add_balance_reason    = State()
    remove_balance_amount = State()
    remove_balance_reason = State()
    broadcast_message     = State()
    rejecting_sub         = State()


# ─────────────────────────────────────────────
#  KEYBOARDS
# ─────────────────────────────────────────────

def kb_user_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👤 Profile",         callback_data="profile"),
            InlineKeyboardButton(text="📜 History",         callback_data="history"),
        ],
        [
            InlineKeyboardButton(text="📤 Upload Image",    callback_data="upload"),
            InlineKeyboardButton(text="💸 Withdraw",        callback_data="withdraw"),
        ],
        [
            InlineKeyboardButton(text="📋 Rules",           callback_data="rules"),
            InlineKeyboardButton(text="🆘 Contact Support", callback_data="contact_support"),
        ],
    ])


def kb_admin_main() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="⚙️ Settings",        callback_data="admin_settings"),
            InlineKeyboardButton(text="📬 Submissions",      callback_data="admin_submissions"),
        ],
        [
            InlineKeyboardButton(text="⏳ Pending Images",   callback_data="imglist:0"),
            InlineKeyboardButton(text="📥 Downloaded List",  callback_data="dllist:0"),
        ],
        [
            InlineKeyboardButton(text="📋 All Sub History",  callback_data="sub_history:0"),
            InlineKeyboardButton(text="❌ Rejected History", callback_data="rejected_hist:0"),
        ],
        [
            InlineKeyboardButton(text="👥 Users",            callback_data="admin_users"),
            InlineKeyboardButton(text="💳 Withdrawals",      callback_data="admin_withdrawals"),
        ],
        [
            InlineKeyboardButton(text="📊 Statistics",       callback_data="admin_stats"),
            InlineKeyboardButton(text="📣 Broadcast",        callback_data="admin_broadcast"),
        ],
        [
            InlineKeyboardButton(text="📄 Generate CSV",     callback_data="generate_csv"),
        ],
    ])


def kb_admin_settings(s: Dict = None) -> InlineKeyboardMarkup:
    if s:
        upload_lbl = f"📤 Upload: {'✅ ON' if s.get('upload_enabled', True) else '❌ OFF'}"
        upi_lbl    = f"💳 UPI WD: {'✅ ON' if s.get('withdraw_upi_enabled', True) else '❌ OFF'}"
        amz_lbl    = f"🎁 Amazon GC WD: {'✅ ON' if s.get('withdraw_amazon_enabled', True) else '❌ OFF'}"
    else:
        upload_lbl = "📤 Toggle Upload"
        upi_lbl    = "💳 Toggle UPI WD"
        amz_lbl    = "🎁 Toggle Amazon GC WD"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Image Reward",  callback_data="set_image_reward")],
        [InlineKeyboardButton(text="💳 Min Withdraw",  callback_data="set_min_withdraw")],
        [InlineKeyboardButton(text="📋 Rules Text",    callback_data="set_rules")],
        [InlineKeyboardButton(text="🔗 Support Link",  callback_data="set_support_link")],
        [InlineKeyboardButton(text=upload_lbl,          callback_data="toggle_upload")],
        [InlineKeyboardButton(text=upi_lbl,             callback_data="toggle_withdraw_upi")],
        [InlineKeyboardButton(text=amz_lbl,             callback_data="toggle_withdraw_amazon")],
        [InlineKeyboardButton(text="🔙 Back",           callback_data="admin_back")],
    ])


def kb_cancel() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Cancel", callback_data="cancel"),
    ]])


def kb_category_skip_cancel() -> InlineKeyboardMarkup:
    """Only category can be skipped."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⏭ Skip Category", callback_data="skip_category"),
        InlineKeyboardButton(text="❌ Cancel",         callback_data="cancel"),
    ]])


def kb_back_admin() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🔙 Back to Admin Panel", callback_data="admin_back"),
    ]])


def kb_sub_actions(sub_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_sub:{sub_id}"),
        InlineKeyboardButton(text="❌ Reject",  callback_data=f"reject_sub:{sub_id}"),
    ]])


def kb_image_nav(index: int, total: int, sub_id: str, status: str, prefix: str = "imglist") -> InlineKeyboardMarkup:
    """
    Builds the inline keyboard for the Pending / Downloaded image viewer.

    Pending    → Copy Title | Copy Keywords | Mark Downloaded | Approve | Reject
    Downloaded → Copy Title | Copy Keywords | Approve | Reject
    """
    rows = []

    # Row 1 — Copy buttons (always present)
    rows.append([
        InlineKeyboardButton(text="📋 Copy Title",    callback_data=f"copy_title:{sub_id}"),
        InlineKeyboardButton(text="🔑 Copy Keywords", callback_data=f"copy_kw:{sub_id}"),
    ])

    # Row 2 — Action buttons (differ by status)
    if status == "pending":
        rows.append([
            InlineKeyboardButton(text="📥 Mark Downloaded", callback_data=f"mark_dl:{sub_id}"),
        ])
        rows.append([
            InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_sub:{sub_id}"),
            InlineKeyboardButton(text="❌ Reject",  callback_data=f"reject_sub:{sub_id}"),
        ])
    elif status == "downloaded":
        rows.append([
            InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_sub:{sub_id}"),
            InlineKeyboardButton(text="❌ Reject",  callback_data=f"reject_sub:{sub_id}"),
        ])

    # Navigation row
    nav = []
    if index > 0:
        nav.append(InlineKeyboardButton(text="◀ Prev", callback_data=f"{prefix}:{index - 1}"))
    nav.append(InlineKeyboardButton(text=f"{index + 1}/{total}", callback_data="noop"))
    if index < total - 1:
        nav.append(InlineKeyboardButton(text="Next ▶", callback_data=f"{prefix}:{index + 1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton(text="🔙 Admin Panel", callback_data="admin_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_wd_actions(wid: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Approve", callback_data=f"approve_wd:{wid}"),
        InlineKeyboardButton(text="❌ Reject",  callback_data=f"reject_wd:{wid}"),
    ]])


def kb_confirm_wd(upi: str, amount: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Confirm", callback_data=f"confirm_wd:{upi}:{amount}"),
        InlineKeyboardButton(text="❌ Cancel",  callback_data="cancel"),
    ]])


def kb_confirm_wd_amazon(amazon_id: str, amount: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Confirm", callback_data=f"confirm_wd_amazon:{amazon_id}:{amount}"),
        InlineKeyboardButton(text="❌ Cancel",  callback_data="cancel"),
    ]])


def kb_withdraw_method(settings: Dict) -> InlineKeyboardMarkup:
    buttons = []
    if settings.get("withdraw_upi_enabled", True):
        buttons.append([InlineKeyboardButton(text="💳 UPI",              callback_data="wd_method:upi")])
    if settings.get("withdraw_amazon_enabled", True):
        buttons.append([InlineKeyboardButton(text="🎁 Amazon Gift Card", callback_data="wd_method:amazon")])
    buttons.append([InlineKeyboardButton(text="❌ Cancel", callback_data="cancel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def kb_user_mgmt(user_id: int, banned: bool) -> InlineKeyboardMarkup:
    ban_txt = "🔓 Unban" if banned else "🔨 Ban"
    ban_cb  = f"unban:{user_id}" if banned else f"ban:{user_id}"
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Add Balance",    callback_data=f"add_bal:{user_id}"),
            InlineKeyboardButton(text="➖ Remove Balance", callback_data=f"rem_bal:{user_id}"),
        ],
        [
            InlineKeyboardButton(text="📜 View History",  callback_data=f"admin_user_hist:{user_id}:0"),
            InlineKeyboardButton(text=ban_txt,             callback_data=ban_cb),
        ],
        [InlineKeyboardButton(text="🔙 Back",             callback_data="admin_back")],
    ])


def kb_reject_reason(sub_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⏭ Skip Reason", callback_data=f"reject_skip:{sub_id}"),
        InlineKeyboardButton(text="❌ Cancel",       callback_data="admin_back"),
    ]])


def kb_banned(support_link: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🆘 Contact Support", url=support_link),
    ]])


# ─────────────────────────────────────────────
#  BAN MIDDLEWARE
# ─────────────────────────────────────────────

class BanMiddleware:
    async def __call__(self, handler: Callable, event: Any, data: Dict[str, Any]) -> Any:
        user = getattr(event, "from_user", None)
        if not user or user.id == ADMIN_CHAT_ID:
            return await handler(event, data)

        if await DB.is_banned(user.id):
            settings = await DB.get_settings()
            kb  = kb_banned(settings["support_link"])
            msg = "🚫 <b>You are banned from using this bot.</b>\n\nIf you believe this is a mistake, contact support."
            try:
                if isinstance(event, Message):
                    await event.answer(msg, parse_mode="HTML", reply_markup=kb)
                elif isinstance(event, CallbackQuery):
                    await event.answer("🚫 You are banned.", show_alert=True)
                    await event.message.answer(msg, parse_mode="HTML", reply_markup=kb)
            except Exception:
                pass
            return
        return await handler(event, data)


# ─────────────────────────────────────────────
#  ROUTER
# ─────────────────────────────────────────────

router = Router()


# ─────────────────────────────────────────────
#  /START
# ─────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    uid = message.from_user.id

    if uid == ADMIN_CHAT_ID:
        await message.answer(
            "👋 Welcome back, <b>Admin</b>!\n\nChoose an option:",
            parse_mode="HTML",
            reply_markup=kb_admin_main(),
        )
        return

    await DB.ensure_user(uid, message.from_user.username or "")
    await message.answer(
        "👋 Welcome to <b>ClickCash</b>!\n\n"
        "Upload images, earn rewards, and withdraw to your UPI.\n\n"
        "Use the menu below:",
        parse_mode="HTML",
        reply_markup=kb_user_main(),
    )


# ─────────────────────────────────────────────
#  PROFILE
# ─────────────────────────────────────────────

@router.callback_query(F.data == "profile")
async def cb_profile(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    user = await DB.get_user(uid)
    subs = await DB.get_user_submissions(uid)

    approved = sum(1 for s in subs.values() if s.get("status") == "approved")
    pending  = sum(1 for s in subs.values() if s.get("status") == "pending")
    rejected = sum(1 for s in subs.values() if s.get("status") == "rejected")

    await callback.message.edit_text(
        f"👤 <b>Your Profile</b>\n\n"
        f"🆔 User ID: <code>{uid}</code>\n"
        f"💰 Balance: ₹{user.get('balance', 0)}\n\n"
        f"📊 <b>Submissions</b>\n"
        f"✅ Approved: {approved}\n"
        f"⏳ Pending:  {pending}\n"
        f"❌ Rejected: {rejected}",
        parse_mode="HTML",
        reply_markup=kb_user_main(),
    )
    await callback.answer()


# ─────────────────────────────────────────────
#  HISTORY
# ─────────────────────────────────────────────

@router.callback_query(F.data == "history")
async def cb_history(callback: CallbackQuery) -> None:
    uid  = callback.from_user.id
    subs = await DB.get_user_submissions(uid)
    wds  = await DB.get_user_withdrawals(uid)

    SEP = "\n〰〰〰〰〰〰〰〰〰〰〰\n"

    sub_entries = []
    for sub in sorted(subs.values(), key=lambda x: x.get("date", ""), reverse=True):
        st   = sub.get("status", "")
        icon = {"approved": "✅", "pending": "⏳", "rejected": "❌"}.get(st, "❓")
        status_label = {"approved": "Approved", "pending": "Pending", "rejected": "Rejected"}.get(st, st.capitalize())
        entry = (
            f"{icon} <b>{he(sub.get('title', 'Untitled'))}</b>\n"
            f"    📅 Submitted: {_fmt(sub.get('date', ''))}\n"
            f"    Status: <b>{icon} {status_label}</b>"
        )
        if st == "approved":
            entry += (
                f"\n    💰 Reward: ₹{sub.get('reward', IMAGE_REWARD)}"
                f"\n    ✅ Approved at: {_fmt(sub.get('approved_at', ''))}"
            )
        elif st == "rejected":
            entry += f"\n    ❌ Rejected at: {_fmt(sub.get('rejected_at', ''))}"
            if sub.get("reason"):
                entry += f"\n    📝 Reason: {he(sub['reason'])}"
        sub_entries.append(entry)

    subs_block = SEP.join(sub_entries) if sub_entries else "📭 No image submissions yet."

    wd_entries = []
    for wd in sorted(wds.values(), key=lambda x: x.get("date", ""), reverse=True):
        st   = wd.get("status", "")
        icon = {"approved": "✅", "pending": "⏳", "rejected": "❌"}.get(st, "❓")
        status_label = {"approved": "Approved", "pending": "Pending", "rejected": "Rejected"}.get(st, st.capitalize())
        entry = (
            f"{icon} <b>Withdrawal ₹{wd.get('amount', 0)}</b>\n"
            f"    💳 UPI: <code>{he(wd.get('upi', 'N/A'))}</code>\n"
            f"    📅 Requested: {_fmt(wd.get('date', ''))}\n"
            f"    Status: <b>{icon} {status_label}</b>"
        )
        if st == "approved":
            entry += f"\n    ✅ Approved at: {_fmt(wd.get('approved_at', ''))}"
        elif st == "rejected":
            entry += f"\n    ❌ Rejected & Refunded: {_fmt(wd.get('rejected_at', ''))}"
        wd_entries.append(entry)

    wds_block = SEP.join(wd_entries) if wd_entries else "📭 No withdrawal requests yet."

    text = (
        f"📜 <b>Your History</b>\n\n"
        f"━━━━━━ 🖼 <b>Image Submissions</b> ━━━━━━\n\n"
        f"{subs_block}\n\n"
        f"━━━━━━ 💳 <b>Withdrawals</b> ━━━━━━\n\n"
        f"{wds_block}"
    )

    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb_user_main())
    await callback.answer()


# ─────────────────────────────────────────────
#  UPLOAD FLOW
# ─────────────────────────────────────────────

@router.callback_query(F.data == "upload")
async def cb_upload(callback: CallbackQuery, state: FSMContext) -> None:
    uid      = callback.from_user.id
    settings = await DB.get_settings()

    if not settings["upload_enabled"]:
        await callback.message.edit_text(
            "📤 <b>Uploads Closed</b>\n\n"
            "Uploads are currently closed. Don't worry, you will receive a notification when it opens again!",
            parse_mode="HTML",
            reply_markup=kb_user_main(),
        )
        await callback.answer()
        return

    await state.update_data(
        uploader_user_id = uid,
        uploader_username = callback.from_user.username or "",
    )
    await state.set_state(UploadStates.waiting_document)

    await callback.message.edit_text(
        "📤 <b>Upload Image</b>\n\n"
        "Send your image as a <b>Document</b> (NOT as a photo).\n\n"
        "✅ Accepted: <code>jpg</code>, <code>jpeg</code>, <code>png</code>, <code>webp</code>\n\n"
        "<i>Telegram → 📎 → File → select image</i>",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )
    await callback.answer()


@router.message(UploadStates.waiting_document, F.document)
async def handle_document(message: Message, state: FSMContext) -> None:
    doc  = message.document
    mime = doc.mime_type or ""
    name = doc.file_name or ""
    ext  = name.rsplit(".", 1)[-1].lower() if "." in name else ""

    if mime not in ALLOWED_MIMES and ext not in ALLOWED_EXTS:
        await message.answer(
            "❌ <b>Invalid file type.</b>\n\n"
            "Only JPG, JPEG, PNG, WEBP are accepted.\n"
            "Send as <b>Document</b>, not as a photo.",
            parse_mode="HTML",
            reply_markup=kb_cancel(),
        )
        return

    await state.update_data(
        file_id        = doc.file_id,
        file_unique_id = doc.file_unique_id,
        file_name      = name,
    )
    await state.set_state(UploadStates.waiting_title)
    await message.answer(
        "✅ <b>Image received!</b>\n\n"
        "Now send the <b>Title</b> for your image:",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )


@router.message(UploadStates.waiting_document)
async def handle_document_wrong(message: Message, state: FSMContext) -> None:
    await message.answer(
        "❌ Please send your image as a <b>Document</b> (File), not as a photo or other type.",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )


@router.message(UploadStates.waiting_title)
async def handle_title(message: Message, state: FSMContext) -> None:
    title = (message.text or "").strip()
    if not title:
        await message.answer("❌ Title cannot be empty. Please enter a valid title:", reply_markup=kb_cancel())
        return
    await state.update_data(title=title)
    await state.set_state(UploadStates.waiting_keywords)
    await message.answer(
        "📝 <b>Title saved!</b>\n\n"
        "Now send the <b>Keywords</b> for your image.\n"
        "<i>Separate with commas. Example: nature, sky, forest</i>",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )


@router.message(UploadStates.waiting_keywords)
async def handle_keywords(message: Message, state: FSMContext) -> None:
    keywords = (message.text or "").strip()
    if not keywords:
        await message.answer("❌ Keywords are mandatory. Please enter keywords:", reply_markup=kb_cancel())
        return
    await state.update_data(keywords=keywords)
    await state.set_state(UploadStates.waiting_category)
    await message.answer(
        "📁 <b>Keywords saved!</b>\n\n"
        "Now send the <b>Category</b> for your image, or tap Skip:",
        parse_mode="HTML",
        reply_markup=kb_category_skip_cancel(),
    )


@router.callback_query(UploadStates.waiting_category, F.data == "skip_category")
async def cb_skip_category(callback: CallbackQuery, state: FSMContext) -> None:
    await state.update_data(category="")
    await _save_submission(callback.message, state, bot)
    await callback.answer()


@router.message(UploadStates.waiting_category)
async def handle_category(message: Message, state: FSMContext) -> None:
    category = (message.text or "").strip()
    await state.update_data(category=category)
    await _save_submission(message, state, bot)


async def _save_submission(message: Message, state: FSMContext, bot_instance: Bot) -> None:
    data = await state.get_data()
    uid      = data.get("uploader_user_id")
    username = data.get("uploader_username", "")

    if not uid:
        await message.answer("❌ Session error. Please start again.", reply_markup=kb_user_main())
        await state.clear()
        return

    sub_id = await DB.create_submission({
        "user_id":      uid,
        "username":     username,
        "file_id":      data.get("file_id", ""),
        "file_unique_id": data.get("file_unique_id", ""),
        "file_name":    data.get("file_name", ""),
        "title":        data.get("title", ""),
        "keywords":     data.get("keywords", ""),
        "category":     data.get("category", ""),
        "status":       "pending",
        "date":         _now(),
    })
    await state.clear()

    await message.answer(
        f"✅ <b>Submission received!</b>\n\n"
        f"🆔 ID: <code>{sub_id}</code>\n"
        f"📝 Title: {he(data.get('title', ''))}\n"
        f"⏳ Status: Pending review",
        parse_mode="HTML",
        reply_markup=kb_user_main(),
    )

    try:
        await bot_instance.send_message(
            ADMIN_CHAT_ID,
            f"📬 <b>New Submission!</b>\n\n"
            f"🆔 ID: <code>{sub_id}</code>\n"
            f"👤 User: <code>{uid}</code>  @{he(username)}\n"
            f"📝 Title: {he(data.get('title', ''))}\n"
            f"🔑 Keywords: <code>{he(data.get('keywords', ''))}</code>\n"
            f"📁 Category: {he(data.get('category', '') or 'N/A')}\n"
            f"📅 Date: {_now()}",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.warning("Admin notify failed: %s", e)


# ─────────────────────────────────────────────
#  CANCEL
# ─────────────────────────────────────────────

@router.callback_query(F.data == "cancel")
async def cb_cancel(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    uid = callback.from_user.id
    if uid == ADMIN_CHAT_ID:
        await callback.message.edit_text(
            "🛠 <b>Admin Panel</b>", parse_mode="HTML", reply_markup=kb_admin_main()
        )
    else:
        await callback.message.edit_text("❌ Cancelled.", reply_markup=kb_user_main())
    await callback.answer()


@router.callback_query(F.data == "noop")
async def cb_noop(callback: CallbackQuery) -> None:
    await callback.answer()


# ─────────────────────────────────────────────
#  WITHDRAW FLOW
# ─────────────────────────────────────────────

@router.callback_query(F.data == "withdraw")
async def cb_withdraw(callback: CallbackQuery, state: FSMContext) -> None:
    uid      = callback.from_user.id
    settings = await DB.get_settings()
    balance  = await DB.get_balance(uid)

    if not settings["withdraw_upi_enabled"] and not settings["withdraw_amazon_enabled"]:
        await callback.message.edit_text(
            "💸 <b>Withdrawals Closed</b>\n\nWithdrawals are currently disabled.",
            parse_mode="HTML",
            reply_markup=kb_user_main(),
        )
        await callback.answer()
        return

    if balance < settings["min_withdraw"]:
        await callback.message.edit_text(
            f"💸 <b>Insufficient Balance</b>\n\n"
            f"💰 Your balance: ₹{balance}\n"
            f"💳 Minimum withdraw: ₹{settings['min_withdraw']}\n\n"
            f"Keep uploading to earn more!",
            parse_mode="HTML",
            reply_markup=kb_user_main(),
        )
        await callback.answer()
        return

    await callback.message.edit_text(
        f"💸 <b>Withdraw</b>\n\n"
        f"💰 Available balance: ₹{balance}\n\n"
        f"Select withdrawal method:",
        parse_mode="HTML",
        reply_markup=kb_withdraw_method(settings),
    )
    await callback.answer()


@router.callback_query(F.data == "wd_method:upi")
async def cb_wd_method_upi(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(WithdrawStates.waiting_upi)
    await callback.message.edit_text(
        "💳 <b>UPI Withdrawal</b>\n\nEnter your <b>UPI ID</b>:\n<i>Example: name@upi</i>",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )
    await callback.answer()


@router.message(WithdrawStates.waiting_upi)
async def handle_upi_id(message: Message, state: FSMContext) -> None:
    upi     = (message.text or "").strip()
    uid     = message.from_user.id
    balance = await DB.get_balance(uid)
    settings = await DB.get_settings()

    if not upi or "@" not in upi:
        await message.answer("❌ Invalid UPI ID. Enter in format <code>name@bank</code>:", parse_mode="HTML", reply_markup=kb_cancel())
        return
    if balance < settings["min_withdraw"]:
        await message.answer(
            f"❌ Insufficient balance (₹{balance}). Minimum: ₹{settings['min_withdraw']}",
            reply_markup=kb_user_main(),
        )
        await state.clear()
        return

    await message.answer(
        f"💳 <b>Confirm Withdrawal</b>\n\n"
        f"UPI: <code>{he(upi)}</code>\n"
        f"Amount: ₹{balance}\n\n"
        f"Press <b>Confirm</b> to proceed.",
        parse_mode="HTML",
        reply_markup=kb_confirm_wd(upi, balance),
    )


@router.callback_query(WithdrawStates.confirming, F.data.startswith("confirm_wd:"))
async def cb_confirm_wd(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    uid   = callback.from_user.id
    parts = callback.data.split(":", 2)
    if len(parts) < 3:
        await callback.answer("Invalid request.", show_alert=True)
        return

    upi     = parts[1]
    amount  = to_int(parts[2])
    balance = await DB.get_balance(uid)
    settings = await DB.get_settings()

    if balance < settings["min_withdraw"] or balance < amount:
        await callback.message.edit_text(
            "❌ Balance changed. Request cancelled.",
            parse_mode="HTML",
            reply_markup=kb_user_main(),
        )
        await state.clear()
        await callback.answer()
        return

    await DB.add_balance(uid, -amount)
    wid = await DB.create_withdrawal({
        "user_id": uid,
        "method":  "upi",
        "upi":     upi,
        "amount":  amount,
        "status":  "pending",
        "date":    _now(),
    })
    await state.clear()

    await callback.message.edit_text(
        f"✅ <b>Withdrawal Requested!</b>\n\n"
        f"🆔 Request ID: <code>{wid}</code>\n"
        f"💳 UPI: <code>{he(upi)}</code>\n"
        f"💰 Amount: ₹{amount}\n"
        f"⏳ Status: Pending",
        parse_mode="HTML",
        reply_markup=kb_user_main(),
    )

    try:
        await bot.send_message(
            ADMIN_CHAT_ID,
            f"💳 <b>New UPI Withdrawal Request!</b>\n\n"
            f"🆔 ID: <code>{wid}</code>\n"
            f"👤 User: <code>{uid}</code>\n"
            f"💳 UPI: <code>{he(upi)}</code>\n"
            f"💰 Amount: ₹{amount}\n"
            f"📅 Date: {_now()}",
            parse_mode="HTML",
            reply_markup=kb_wd_actions(wid),
        )
    except Exception as e:
        logger.error("Admin notify failed: %s", e)

    await callback.answer()


@router.callback_query(F.data == "wd_method:amazon")
async def cb_wd_method_amazon(callback: CallbackQuery, state: FSMContext) -> None:
    await state.set_state(WithdrawStates.waiting_amazon)
    await callback.message.edit_text(
        "🎁 <b>Amazon Gift Card Withdrawal</b>\n\nEnter your <b>Amazon Account Email/Mobile</b>:",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )
    await callback.answer()


@router.message(WithdrawStates.waiting_amazon)
async def handle_amazon_id(message: Message, state: FSMContext) -> None:
    amazon_id = (message.text or "").strip()
    uid       = message.from_user.id
    balance   = await DB.get_balance(uid)
    settings  = await DB.get_settings()

    if not amazon_id:
        await message.answer("❌ Please enter your Amazon account email/mobile:", reply_markup=kb_cancel())
        return
    if balance < settings["min_withdraw"]:
        await message.answer(
            f"❌ Insufficient balance (₹{balance}). Minimum: ₹{settings['min_withdraw']}",
            reply_markup=kb_user_main(),
        )
        await state.clear()
        return

    await state.update_data(amazon_id=amazon_id, amount=balance)
    await state.set_state(WithdrawStates.confirming_amazon)
    await message.answer(
        f"🎁 <b>Confirm Amazon Gift Card Withdrawal</b>\n\n"
        f"Amazon Account: <code>{he(amazon_id)}</code>\n"
        f"Amount: ₹{balance}\n\n"
        f"Press <b>Confirm</b> to proceed.",
        parse_mode="HTML",
        reply_markup=kb_confirm_wd_amazon(amazon_id, balance),
    )


@router.callback_query(WithdrawStates.confirming_amazon, F.data.startswith("confirm_wd_amazon:"))
async def cb_confirm_wd_amazon(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    uid   = callback.from_user.id
    parts = callback.data.split(":", 2)
    if len(parts) < 3:
        await callback.answer("Invalid request.", show_alert=True)
        return

    amazon_id = parts[1]
    amount    = to_int(parts[2])
    balance   = await DB.get_balance(uid)
    settings  = await DB.get_settings()

    if balance < settings["min_withdraw"] or balance < amount:
        await callback.message.edit_text(
            "❌ Balance changed. Request cancelled.",
            parse_mode="HTML",
            reply_markup=kb_user_main(),
        )
        await state.clear()
        await callback.answer()
        return

    await DB.add_balance(uid, -amount)
    wid = await DB.create_withdrawal({
        "user_id":   uid,
        "method":    "amazon",
        "amazon_id": amazon_id,
        "amount":    amount,
        "status":    "pending",
        "date":      _now(),
    })
    await state.clear()

    await callback.message.edit_text(
        f"✅ <b>Withdrawal Requested!</b>\n\n"
        f"🆔 Request ID: <code>{wid}</code>\n"
        f"🎁 Amazon Account: <code>{he(amazon_id)}</code>\n"
        f"💰 Amount: ₹{amount}\n"
        f"⏳ Status: Pending",
        parse_mode="HTML",
        reply_markup=kb_user_main(),
    )

    try:
        await bot.send_message(
            ADMIN_CHAT_ID,
            f"🎁 <b>New Amazon Gift Card Withdrawal Request!</b>\n\n"
            f"🆔 ID: <code>{wid}</code>\n"
            f"👤 User: <code>{uid}</code>\n"
            f"🎁 Amazon Account: <code>{he(amazon_id)}</code>\n"
            f"💰 Amount: ₹{amount}\n"
            f"📅 Date: {_now()}",
            parse_mode="HTML",
            reply_markup=kb_wd_actions(wid),
        )
    except Exception as e:
        logger.error("Admin notify failed: %s", e)

    await callback.answer()


# ─────────────────────────────────────────────
#  RULES / SUPPORT
# ─────────────────────────────────────────────

@router.callback_query(F.data == "rules")
async def cb_rules(callback: CallbackQuery) -> None:
    settings = await DB.get_settings()
    await callback.message.edit_text(
        settings["rules"],
        parse_mode="HTML",
        reply_markup=kb_user_main(),
    )
    await callback.answer()


@router.callback_query(F.data == "contact_support")
async def cb_contact_support(callback: CallbackQuery) -> None:
    settings = await DB.get_settings()
    await callback.message.edit_text(
        "🆘 <b>Contact Support</b>\n\nTap the button below to reach us:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💬 Contact Support", url=settings["support_link"])],
            [InlineKeyboardButton(text="🔙 Back",            callback_data="back_main")],
        ]),
    )
    await callback.answer()


@router.callback_query(F.data == "back_main")
async def cb_back_main(callback: CallbackQuery) -> None:
    if callback.from_user.id == ADMIN_CHAT_ID:
        await callback.message.edit_text(
            "🛠 <b>Admin Panel</b>", parse_mode="HTML", reply_markup=kb_admin_main()
        )
    else:
        await callback.message.edit_text("🏠 Main Menu:", reply_markup=kb_user_main())
    await callback.answer()


# ─────────────────────────────────────────────
#  ADMIN BACK
# ─────────────────────────────────────────────

@router.callback_query(F.data == "admin_back")
async def cb_admin_back(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text(
        "🛠 <b>Admin Panel</b>", parse_mode="HTML", reply_markup=kb_admin_main()
    )
    await callback.answer()


# ─────────────────────────────────────────────
#  ADMIN — SETTINGS
# ─────────────────────────────────────────────

@router.callback_query(F.data == "admin_settings")
async def cb_admin_settings(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return
    s = await DB.get_settings()
    await callback.message.edit_text(
        f"⚙️ <b>Settings</b>\n\n"
        f"💰 Image Reward: ₹{s['image_reward']}\n"
        f"💳 Min Withdraw: ₹{s['min_withdraw']}\n"
        f"🔗 Support: {he(s['support_link'])}\n"
        f"📤 Upload: {'✅ ON' if s['upload_enabled'] else '❌ OFF'}\n"
        f"💳 UPI WD: {'✅ ON' if s['withdraw_upi_enabled'] else '❌ OFF'}\n"
        f"🎁 Amazon GC WD: {'✅ ON' if s['withdraw_amazon_enabled'] else '❌ OFF'}\n\n"
        f"Choose what to update:",
        parse_mode="HTML",
        reply_markup=kb_admin_settings(s),
    )
    await callback.answer()


@router.callback_query(F.data == "set_image_reward")
async def cb_set_image_reward(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        return
    await state.set_state(AdminStates.setting_image_reward)
    await callback.message.edit_text(
        "💰 Enter new <b>Image Reward</b> amount (₹):",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )
    await callback.answer()


@router.message(AdminStates.setting_image_reward)
async def handle_set_reward(message: Message, state: FSMContext) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    try:
        val = int(message.text.strip())
        assert val > 0
    except Exception:
        await message.answer("❌ Enter a positive number.")
        return
    ok = await DB.set_setting("image_reward", val)
    await state.clear()
    if ok:
        await message.answer(f"✅ Image reward set to ₹{val}.", reply_markup=kb_admin_main())
    else:
        await message.answer("❌ Firebase write failed.", reply_markup=kb_admin_main())


@router.callback_query(F.data == "set_min_withdraw")
async def cb_set_min_withdraw(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        return
    await state.set_state(AdminStates.setting_min_withdraw)
    await callback.message.edit_text(
        "💳 Enter new <b>Min Withdraw</b> amount (₹):",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )
    await callback.answer()


@router.message(AdminStates.setting_min_withdraw)
async def handle_set_min_withdraw(message: Message, state: FSMContext) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    try:
        val = int(message.text.strip())
        assert val > 0
    except Exception:
        await message.answer("❌ Enter a positive number.")
        return
    ok = await DB.set_setting("min_withdraw", val)
    await state.clear()
    if ok:
        await message.answer(f"✅ Min withdraw set to ₹{val}.", reply_markup=kb_admin_main())
    else:
        await message.answer("❌ Firebase write failed.", reply_markup=kb_admin_main())


@router.callback_query(F.data == "set_rules")
async def cb_set_rules(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        return
    await state.set_state(AdminStates.setting_rules)
    await callback.message.edit_text(
        "📋 Send the new <b>Rules</b> text (HTML supported):",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )
    await callback.answer()


@router.message(AdminStates.setting_rules)
async def handle_set_rules(message: Message, state: FSMContext) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    text = message.text or message.caption or ""
    if not text.strip():
        await message.answer("❌ Rules text cannot be empty.")
        return
    ok = await DB.set_setting("rules", text.strip())
    await state.clear()
    if ok:
        await message.answer("✅ Rules updated.", reply_markup=kb_admin_main())
    else:
        await message.answer("❌ Firebase write failed.", reply_markup=kb_admin_main())


@router.callback_query(F.data == "set_support_link")
async def cb_set_support_link(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        return
    await state.set_state(AdminStates.setting_support_link)
    await callback.message.edit_text(
        "🔗 Send the new <b>Support Link</b> (must start with https://):",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )
    await callback.answer()


@router.message(AdminStates.setting_support_link)
async def handle_set_support_link(message: Message, state: FSMContext) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    link = (message.text or "").strip()
    if not link.startswith("http"):
        await message.answer("❌ Invalid link. Must start with http:// or https://")
        return
    ok = await DB.set_setting("support_link", link)
    await state.clear()
    if ok:
        await message.answer("✅ Support link updated.", reply_markup=kb_admin_main())
    else:
        await message.answer("❌ Firebase write failed.", reply_markup=kb_admin_main())


@router.callback_query(F.data == "toggle_upload")
async def cb_toggle_upload(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        return
    s = await DB.get_settings()
    new_val = not s["upload_enabled"]
    await DB.set_setting("upload_enabled", new_val)
    s["upload_enabled"] = new_val
    await callback.message.edit_reply_markup(reply_markup=kb_admin_settings(s))
    await callback.answer(f"Upload {'enabled' if new_val else 'disabled'}.")


@router.callback_query(F.data == "toggle_withdraw_upi")
async def cb_toggle_withdraw_upi(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        return
    s = await DB.get_settings()
    new_val = not s["withdraw_upi_enabled"]
    await DB.set_setting("withdraw_upi_enabled", new_val)
    s["withdraw_upi_enabled"] = new_val
    await callback.message.edit_reply_markup(reply_markup=kb_admin_settings(s))
    await callback.answer(f"UPI Withdraw {'enabled' if new_val else 'disabled'}.")


@router.callback_query(F.data == "toggle_withdraw_amazon")
async def cb_toggle_withdraw_amazon(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        return
    s = await DB.get_settings()
    new_val = not s["withdraw_amazon_enabled"]
    await DB.set_setting("withdraw_amazon_enabled", new_val)
    s["withdraw_amazon_enabled"] = new_val
    await callback.message.edit_reply_markup(reply_markup=kb_admin_settings(s))
    await callback.answer(f"Amazon GC Withdraw {'enabled' if new_val else 'disabled'}.")


# ─────────────────────────────────────────────
#  ADMIN — USERS
# ─────────────────────────────────────────────

@router.callback_query(F.data == "admin_users")
async def cb_admin_users(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return
    await state.set_state(AdminStates.searching_user)
    await callback.message.edit_text(
        "👥 <b>User Search</b>\n\nEnter the <b>User ID</b> or <b>@username</b> to search:",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )
    await callback.answer()


@router.message(AdminStates.searching_user)
async def handle_search_user(message: Message, state: FSMContext) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    query = (message.text or "").strip().lstrip("@")
    all_users = await DB.get_all_users()

    found = None
    for u in all_users.values():
        if not isinstance(u, dict):
            continue
        if str(u.get("user_id", "")) == query or (u.get("username", "").lower() == query.lower()):
            found = u
            break

    if not found:
        await message.answer(
            f"❌ User <code>{he(query)}</code> not found.",
            parse_mode="HTML",
            reply_markup=kb_admin_main(),
        )
        await state.clear()
        return

    await state.clear()
    uid     = int(found["user_id"])
    banned  = bool(found.get("banned", False))
    balance = to_int(found.get("balance", 0))

    await message.answer(
        f"👤 <b>User Info</b>\n\n"
        f"🆔 ID: <code>{uid}</code>\n"
        f"👤 Username: @{he(found.get('username', 'N/A'))}\n"
        f"💰 Balance: ₹{balance}\n"
        f"🚫 Banned: {'Yes' if banned else 'No'}\n"
        f"📅 Joined: {_fmt(found.get('created_at', ''))}",
        parse_mode="HTML",
        reply_markup=kb_user_mgmt(uid, banned),
    )


@router.callback_query(F.data.startswith("ban:"))
async def cb_ban(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        return
    uid = to_int(callback.data.split(":", 1)[1])
    await DB.update_user(uid, {"banned": True})
    await callback.message.edit_reply_markup(reply_markup=kb_user_mgmt(uid, True))
    await callback.answer(f"User {uid} banned.")


@router.callback_query(F.data.startswith("unban:"))
async def cb_unban(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        return
    uid = to_int(callback.data.split(":", 1)[1])
    await DB.update_user(uid, {"banned": False})
    await callback.message.edit_reply_markup(reply_markup=kb_user_mgmt(uid, False))
    await callback.answer(f"User {uid} unbanned.")


@router.callback_query(F.data.startswith("add_bal:"))
async def cb_add_bal(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        return
    uid = to_int(callback.data.split(":", 1)[1])
    await state.set_state(AdminStates.add_balance_amount)
    await state.update_data(target_uid=uid)
    await callback.message.reply(
        f"➕ Enter amount to <b>add</b> to user <code>{uid}</code>:",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )
    await callback.answer()


@router.message(AdminStates.add_balance_amount)
async def handle_add_balance_amount(message: Message, state: FSMContext) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    try:
        amount = int(message.text.strip())
        assert amount > 0
    except Exception:
        await message.answer("❌ Enter a positive number.")
        return
    await state.update_data(add_amount=amount)
    await state.set_state(AdminStates.add_balance_reason)
    await message.answer(
        f"📝 Enter a <b>reason</b> for adding ₹{amount} (or send 'skip'):",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )


@router.message(AdminStates.add_balance_reason)
async def handle_add_balance_reason(message: Message, state: FSMContext, bot: Bot) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    data   = await state.get_data()
    uid    = data.get("target_uid")
    amount = data.get("add_amount", 0)
    reason = (message.text or "").strip()
    if reason.lower() == "skip":
        reason = ""

    new_bal = await DB.add_balance(uid, amount)
    await state.clear()
    await message.answer(
        f"✅ Added ₹{amount} to user <code>{uid}</code>.\n"
        f"New balance: ₹{new_bal}",
        parse_mode="HTML",
        reply_markup=kb_admin_main(),
    )

    try:
        reason_line = f"\n📝 Reason: {he(reason)}" if reason else ""
        await bot.send_message(
            uid,
            f"💰 <b>Balance Added!</b>\n\n"
            f"₹{amount} has been added to your account.{reason_line}\n"
            f"New Balance: ₹{new_bal}",
            parse_mode="HTML",
            reply_markup=kb_user_main(),
        )
    except Exception as e:
        logger.warning("Notify user %s failed: %s", uid, e)


@router.callback_query(F.data.startswith("rem_bal:"))
async def cb_rem_bal(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        return
    uid = to_int(callback.data.split(":", 1)[1])
    await state.set_state(AdminStates.remove_balance_amount)
    await state.update_data(target_uid=uid)
    await callback.message.reply(
        f"➖ Enter amount to <b>remove</b> from user <code>{uid}</code>:",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )
    await callback.answer()


@router.message(AdminStates.remove_balance_amount)
async def handle_rem_balance_amount(message: Message, state: FSMContext) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    try:
        amount = int(message.text.strip())
        assert amount > 0
    except Exception:
        await message.answer("❌ Enter a positive number.")
        return
    await state.update_data(rem_amount=amount)
    await state.set_state(AdminStates.remove_balance_reason)
    await message.answer(
        f"📝 Enter a <b>reason</b> for removing ₹{amount} (or send 'skip'):",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )


@router.message(AdminStates.remove_balance_reason)
async def handle_rem_balance_reason(message: Message, state: FSMContext, bot: Bot) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    data   = await state.get_data()
    uid    = data.get("target_uid")
    amount = data.get("rem_amount", 0)
    reason = (message.text or "").strip()
    if reason.lower() == "skip":
        reason = ""

    new_bal = await DB.add_balance(uid, -amount)
    await state.clear()
    await message.answer(
        f"✅ Removed ₹{amount} from user <code>{uid}</code>.\n"
        f"New balance: ₹{new_bal}",
        parse_mode="HTML",
        reply_markup=kb_admin_main(),
    )


@router.callback_query(F.data.startswith("admin_user_hist:"))
async def cb_admin_user_hist(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        return
    parts = callback.data.split(":")
    uid   = to_int(parts[1])
    try:
        index = int(parts[2])
    except Exception:
        index = 0

    subs = await DB.get_user_submissions(uid)
    subs_list = sorted(subs.values(), key=lambda x: x.get("date", ""), reverse=True)
    total = len(subs_list)

    if total == 0:
        await callback.message.edit_text(
            f"📜 <b>History for</b> <code>{uid}</code>\n\n📭 No submissions.",
            parse_mode="HTML",
            reply_markup=kb_back_admin(),
        )
        await callback.answer()
        return

    index = max(0, min(index, total - 1))
    sub   = subs_list[index]
    st    = sub.get("status", "")
    icon  = {"approved": "✅", "pending": "⏳", "rejected": "❌"}.get(st, "❓")
    extra = ""
    if st == "approved":
        extra = f"\n💰 Reward: ₹{sub.get('reward', IMAGE_REWARD)}\n✅ Approved: {_fmt(sub.get('approved_at', ''))}"
    elif st == "rejected":
        extra = f"\n❌ Rejected: {_fmt(sub.get('rejected_at', ''))}"
        if sub.get("reason"):
            extra += f"\n📝 Reason: {he(sub['reason'])}"

    text = (
        f"📜 <b>History for</b> <code>{uid}</code> [{index + 1}/{total}]\n\n"
        f"🆔 Sub ID: <code>{sub.get('submission_id', 'N/A')}</code>\n"
        f"📝 Title: {he(sub.get('title', 'N/A'))}\n"
        f"📅 Submitted: {_fmt(sub.get('date', ''))}\n"
        f"{icon} Status: {st.capitalize()}{extra}"
    )
    nav = []
    if index > 0:
        nav.append(InlineKeyboardButton(text="◀ Prev", callback_data=f"admin_user_hist:{uid}:{index - 1}"))
    nav.append(InlineKeyboardButton(text=f"{index + 1}/{total}", callback_data="noop"))
    if index < total - 1:
        nav.append(InlineKeyboardButton(text="Next ▶", callback_data=f"admin_user_hist:{uid}:{index + 1}"))

    kb = InlineKeyboardMarkup(inline_keyboard=[nav, [InlineKeyboardButton(text="🔙 Back", callback_data="admin_back")]])
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await callback.answer()


# ─────────────────────────────────────────────
#  ADMIN — BROADCAST
# ─────────────────────────────────────────────

@router.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return
    await state.set_state(AdminStates.broadcast_message)
    await callback.message.edit_text(
        "📣 <b>Broadcast</b>\n\nSend the message you want to broadcast to all users:",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )
    await callback.answer()


@router.message(AdminStates.broadcast_message)
async def handle_broadcast(message: Message, state: FSMContext, bot: Bot) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    text = message.text or message.caption or ""
    if not text.strip():
        await message.answer("❌ Broadcast message cannot be empty.")
        return

    await state.clear()
    all_users = await DB.get_all_users()
    sent = failed = 0

    for u in all_users.values():
        if not isinstance(u, dict):
            continue
        uid = to_int(u.get("user_id", 0))
        if not uid or uid == ADMIN_CHAT_ID:
            continue
        try:
            await bot.send_message(uid, text, parse_mode="HTML")
            sent += 1
        except Exception:
            failed += 1

    await message.answer(
        f"📣 <b>Broadcast Complete</b>\n\n✅ Sent: {sent}\n❌ Failed: {failed}",
        parse_mode="HTML",
        reply_markup=kb_admin_main(),
    )


# ─────────────────────────────────────────────
#  ADMIN — STATISTICS
# ─────────────────────────────────────────────

@router.callback_query(F.data == "admin_stats")
async def cb_admin_stats(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    all_users = await DB.get_all_users()
    all_subs  = await DB.get_all_submissions()
    all_wds   = await DB.get_all_withdrawals()

    total_users    = len([u for u in all_users.values() if isinstance(u, dict)])
    banned_users   = sum(1 for u in all_users.values() if isinstance(u, dict) and u.get("banned"))
    total_balance  = sum(to_int(u.get("balance", 0)) for u in all_users.values() if isinstance(u, dict))

    approved_subs  = sum(1 for s in all_subs.values() if isinstance(s, dict) and s.get("status") == "approved")
    pending_subs   = sum(1 for s in all_subs.values() if isinstance(s, dict) and s.get("status") == "pending")
    rejected_subs  = sum(1 for s in all_subs.values() if isinstance(s, dict) and s.get("status") == "rejected")
    downloaded_subs = sum(1 for s in all_subs.values() if isinstance(s, dict) and s.get("status") == "downloaded")

    pending_wds    = sum(1 for w in all_wds.values() if isinstance(w, dict) and w.get("status") == "pending")
    approved_wds   = sum(1 for w in all_wds.values() if isinstance(w, dict) and w.get("status") == "approved")
    total_paid     = sum(to_int(w.get("amount", 0)) for w in all_wds.values() if isinstance(w, dict) and w.get("status") == "approved")

    await callback.message.edit_text(
        f"📊 <b>Statistics</b>\n\n"
        f"👥 <b>Users</b>\n"
        f"  Total: {total_users}\n"
        f"  Banned: {banned_users}\n"
        f"  Total Balance: ₹{total_balance}\n\n"
        f"🖼 <b>Submissions</b>\n"
        f"  ✅ Approved: {approved_subs}\n"
        f"  ⏳ Pending: {pending_subs}\n"
        f"  📥 Downloaded: {downloaded_subs}\n"
        f"  ❌ Rejected: {rejected_subs}\n\n"
        f"💳 <b>Withdrawals</b>\n"
        f"  ⏳ Pending: {pending_wds}\n"
        f"  ✅ Approved: {approved_wds}\n"
        f"  💰 Total Paid: ₹{total_paid}",
        parse_mode="HTML",
        reply_markup=kb_back_admin(),
    )
    await callback.answer()


# ─────────────────────────────────────────────
#  ADMIN — SUBMISSIONS LIST (bulk view)
# ─────────────────────────────────────────────

@router.callback_query(F.data == "admin_submissions")
async def cb_admin_submissions(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    pending = await DB.get_pending_submissions()
    if not pending:
        await callback.message.edit_text(
            "📬 <b>Pending Submissions</b>\n\n✅ No pending submissions right now!",
            parse_mode="HTML",
            reply_markup=kb_back_admin(),
        )
        await callback.answer()
        return

    await callback.message.edit_text(
        f"📬 <b>Pending Submissions</b> ({len(pending)})\nSending below…",
        parse_mode="HTML",
        reply_markup=kb_back_admin(),
    )
    for sid, sub in sorted(pending.items(), key=lambda x: x[1].get("date", ""))[:20]:
        txt = (
            f"📬 <b>Submission</b>\n\n"
            f"🆔 Sub ID: <code>{sid}</code>\n"
            f"👤 User: <code>{sub.get('user_id')}</code>  @{he(sub.get('username') or 'N/A')}\n"
            f"📄 File: {he(sub.get('file_name', 'N/A'))}\n"
            f"📝 Title: {he(sub.get('title', 'N/A'))}\n"
            f"🔑 Keywords: <code>{he(sub.get('keywords') or 'N/A')}</code>\n"
            f"📁 Category: {he(sub.get('category') or 'N/A')}\n"
            f"📅 Date: {_fmt(sub.get('date', ''))}"
        )
        try:
            await callback.message.answer(txt, parse_mode="HTML", reply_markup=kb_sub_actions(sid))
        except Exception as e:
            logger.error("Send sub error: %s", e)
    await callback.answer()


# ─────────────────────────────────────────────
#  ADMIN — APPROVE SUBMISSION
# ─────────────────────────────────────────────

@router.callback_query(F.data.startswith("approve_sub:"))
async def cb_approve_sub(callback: CallbackQuery, bot: Bot) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    sub_id = callback.data.split(":", 1)[1]
    sub    = await DB.get_submission(sub_id)

    if not sub:
        await callback.answer("Submission not found.", show_alert=True)
        return
    if sub.get("status") not in ("pending", "downloaded"):
        await callback.answer(f"Already {sub.get('status')}.", show_alert=True)
        return

    settings = await DB.get_settings()
    reward   = settings["image_reward"]
    uid      = int(sub["user_id"])

    await DB.update_submission(sub_id, {
        "status":      "approved",
        "approved_at": _now(),
        "reward":      reward,
    })
    new_bal = await DB.add_balance(uid, reward)

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await callback.message.reply(
        f"✅ Sub <code>{sub_id}</code> approved.\n"
        f"₹{reward} added to user <code>{uid}</code>.\n"
        f"New balance: ₹{new_bal}",
        parse_mode="HTML",
    )

    try:
        await bot.send_message(
            uid,
            f"✅ <b>Your image has been approved!</b>\n\n"
            f"📝 Title: {he(sub.get('title', 'N/A'))}\n"
            f"💰 Reward: ₹{reward} added to your balance\n"
            f"💰 New Balance: ₹{new_bal}",
            parse_mode="HTML",
            reply_markup=kb_user_main(),
        )
    except Exception as e:
        logger.warning("Notify user %s failed: %s", uid, e)

    await callback.answer("Approved! ✅")


# ─────────────────────────────────────────────
#  ADMIN — REJECT SUBMISSION
# ─────────────────────────────────────────────

@router.callback_query(F.data.startswith("reject_sub:"))
async def cb_reject_sub(callback: CallbackQuery, state: FSMContext) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    sub_id = callback.data.split(":", 1)[1]
    sub    = await DB.get_submission(sub_id)

    if not sub:
        await callback.answer("Submission not found.", show_alert=True)
        return
    if sub.get("status") not in ("pending", "downloaded"):
        await callback.answer(f"Already {sub.get('status')}.", show_alert=True)
        return

    await state.set_state(AdminStates.rejecting_sub)
    await state.update_data(rejecting_sub_id=sub_id)

    await callback.message.reply(
        f"✏️ Enter <b>rejection reason</b> for <code>{sub_id}</code> (or skip):",
        parse_mode="HTML",
        reply_markup=kb_reject_reason(sub_id),
    )
    await callback.answer()


@router.message(AdminStates.rejecting_sub)
async def handle_reject_reason(message: Message, state: FSMContext, bot: Bot) -> None:
    if message.from_user.id != ADMIN_CHAT_ID:
        return
    data   = await state.get_data()
    sub_id = data.get("rejecting_sub_id", "")
    reason = message.text.strip() if message.text else ""
    await _do_reject(sub_id, reason, message.chat.id, state, bot)


@router.callback_query(F.data.startswith("reject_skip:"))
async def cb_reject_skip(callback: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        return
    sub_id = callback.data.split(":", 1)[1]
    await state.clear()
    await _do_reject(sub_id, "", callback.message.chat.id, state, bot)
    await callback.answer()


async def _do_reject(sub_id: str, reason: str, chat_id: int, state: FSMContext, bot: Bot) -> None:
    sub = await DB.get_submission(sub_id)
    if not sub or sub.get("status") not in ("pending", "downloaded"):
        await bot.send_message(chat_id, "❌ Cannot reject — not found or already processed.")
        await state.clear()
        return

    uid = int(sub["user_id"])

    await DB.update_submission(sub_id, {
        "status":      "rejected",
        "rejected_at": _now(),
        "reason":      reason,
    })
    await state.clear()

    await bot.send_message(
        chat_id,
        f"❌ Sub <code>{sub_id}</code> rejected.",
        parse_mode="HTML",
        reply_markup=kb_admin_main(),
    )

    reason_line = f"\n\n📝 Reason: {he(reason)}" if reason else ""
    try:
        await bot.send_message(
            uid,
            f"❌ <b>Your image was rejected.</b>\n\n"
            f"📝 Title: {he(sub.get('title', 'N/A'))}{reason_line}\n\n"
            f"No reward has been added. You can upload a better image.",
            parse_mode="HTML",
            reply_markup=kb_user_main(),
        )
    except Exception as e:
        logger.warning("Notify user %s failed: %s", uid, e)


# ─────────────────────────────────────────────
#  ADMIN — IMAGE LIST (pending, paginated)
# ─────────────────────────────────────────────

@router.callback_query(F.data.startswith("imglist:"))
async def cb_image_list(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    try:
        index = int(callback.data.split(":", 1)[1])
    except ValueError:
        index = 0

    all_subs = await DB.get_all_submissions()
    subs     = sorted(
        [v for v in all_subs.values() if isinstance(v, dict) and v.get("status") == "pending"],
        key=lambda x: x.get("date", ""),
    )
    total = len(subs)

    if total == 0:
        try:
            await callback.message.edit_text(
                "⏳ <b>Pending Images</b>\n\n✅ No pending submissions right now!",
                parse_mode="HTML",
                reply_markup=kb_back_admin(),
            )
        except Exception:
            pass
        try:
            await callback.answer()
        except Exception:
            pass
        return

    index  = max(0, min(index, total - 1))
    sub    = subs[index]
    sub_id = sub.get("submission_id", "N/A")
    status = sub.get("status", "pending")
    icon   = {"approved": "✅", "pending": "⏳", "rejected": "❌"}.get(status, "❓")

    caption = (
        f"⏳ <b>Pending Images</b> [{index + 1}/{total}]\n\n"
        f"🆔 Sub ID: <code>{sub_id}</code>\n"
        f"👤 User: <code>{sub.get('user_id')}</code>  @{he(sub.get('username') or 'N/A')}\n"
        f"📄 File: {he(sub.get('file_name', 'N/A'))}\n"
        f"📝 Title: {he(sub.get('title', 'N/A'))}\n"
        f"🔑 Keywords: <code>{he(sub.get('keywords') or 'N/A')}</code>\n"
        f"📁 Category: {he(sub.get('category') or 'N/A')}\n"
        f"📅 Date: {_fmt(sub.get('date', ''))}\n"
        f"{icon} Status: {status.capitalize()}"
    )
    nav_kb = kb_image_nav(index, total, sub_id, status, "imglist")

    try:
        await callback.message.answer_document(
            document     = sub.get("file_id", ""),
            caption      = caption,
            parse_mode   = "HTML",
            reply_markup = nav_kb,
        )
        try:
            await callback.message.delete()
        except Exception:
            pass
    except Exception as e:
        logger.error("Image list send error: %s", e)
        try:
            await callback.message.edit_text(caption, parse_mode="HTML", reply_markup=nav_kb)
        except Exception:
            pass

    try:
        await callback.answer()
    except Exception:
        pass


# ─────────────────────────────────────────────
#  ADMIN — COPY TITLE / COPY KEYWORDS
# ─────────────────────────────────────────────

@router.callback_query(F.data.startswith("copy_title:"))
async def cb_copy_title(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return
    sub_id = callback.data.split(":", 1)[1]
    sub    = await DB.get_submission(sub_id)
    if not sub:
        await callback.answer("Submission not found.", show_alert=True)
        return
    title = sub.get("title") or "N/A"
    await callback.message.answer(
        f"📋 <b>Title</b> — tap to copy:\n\n<code>{he(title)}</code>",
        parse_mode="HTML",
    )
    await callback.answer("Title sent ✅")


@router.callback_query(F.data.startswith("copy_kw:"))
async def cb_copy_keywords(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return
    sub_id = callback.data.split(":", 1)[1]
    sub    = await DB.get_submission(sub_id)
    if not sub:
        await callback.answer("Submission not found.", show_alert=True)
        return
    keywords = sub.get("keywords") or "N/A"
    await callback.message.answer(
        f"🔑 <b>Keywords</b> — tap to copy:\n\n<code>{he(keywords)}</code>",
        parse_mode="HTML",
    )
    await callback.answer("Keywords sent ✅")


# ─────────────────────────────────────────────
#  ADMIN — MARK AS DOWNLOADED
# ─────────────────────────────────────────────

@router.callback_query(F.data.startswith("mark_dl:"))
async def cb_mark_downloaded(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    sub_id = callback.data.split(":", 1)[1]
    sub    = await DB.get_submission(sub_id)

    if not sub:
        await callback.answer("Submission not found.", show_alert=True)
        return
    if sub.get("status") != "pending":
        await callback.answer(
            f"Cannot mark — status is already '{sub.get('status')}'.",
            show_alert=True,
        )
        return

    ok = await fb_update(f"submissions/{sub_id}", {
        "status":        "downloaded",
        "downloaded_at": _now(),
    })
    if not ok:
        logger.error("mark_downloaded Firebase PATCH failed for sub_id=%s", sub_id)
        try:
            await callback.answer(
                "❌ Firebase write failed! Check Firebase rules.",
                show_alert=True,
            )
        except Exception:
            pass
        return

    logger.info("Submission %s marked as downloaded.", sub_id)

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    try:
        await callback.answer("📥 Marked as Downloaded!", show_alert=False)
    except Exception:
        pass
    await callback.message.reply(
        f"📥 Sub <code>{sub_id}</code> moved to <b>Downloaded List</b>.\n"
        f"Process it externally, then Approve or Reject from 📥 Downloaded List.",
        parse_mode="HTML",
        reply_markup=kb_admin_main(),
    )


# ─────────────────────────────────────────────
#  ADMIN — DOWNLOADED LIST (paginated)
# ─────────────────────────────────────────────

@router.callback_query(F.data.startswith("dllist:"))
async def cb_downloaded_list(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    try:
        index = int(callback.data.split(":", 1)[1])
    except ValueError:
        index = 0

    all_subs = await DB.get_all_submissions()
    subs = sorted(
        [v for v in all_subs.values() if isinstance(v, dict) and v.get("status") == "downloaded"],
        key=lambda x: x.get("downloaded_at", x.get("date", "")),
    )
    total = len(subs)

    if total == 0:
        try:
            await callback.message.edit_text(
                "📥 <b>Downloaded List</b>\n\n✅ No images in the downloaded list right now!",
                parse_mode="HTML",
                reply_markup=kb_back_admin(),
            )
        except Exception:
            pass
        try:
            await callback.answer()
        except Exception:
            pass
        return

    index  = max(0, min(index, total - 1))
    sub    = subs[index]
    sub_id = sub.get("submission_id", "N/A")
    status = "downloaded"

    caption = (
        f"📥 <b>Downloaded List</b> [{index + 1}/{total}]\n\n"
        f"🆔 Sub ID: <code>{sub_id}</code>\n"
        f"👤 User: <code>{sub.get('user_id')}</code>  @{he(sub.get('username') or 'N/A')}\n"
        f"📄 File: {he(sub.get('file_name', 'N/A'))}\n"
        f"📝 Title: {he(sub.get('title', 'N/A'))}\n"
        f"🔑 Keywords: <code>{he(sub.get('keywords') or 'N/A')}</code>\n"
        f"📁 Category: {he(sub.get('category') or 'N/A')}\n"
        f"📅 Submitted: {_fmt(sub.get('date', ''))}\n"
        f"📥 Downloaded at: {_fmt(sub.get('downloaded_at', ''))}"
    )
    nav_kb = kb_image_nav(index, total, sub_id, status, "dllist")

    try:
        await callback.message.answer_document(
            document     = sub.get("file_id", ""),
            caption      = caption,
            parse_mode   = "HTML",
            reply_markup = nav_kb,
        )
        try:
            await callback.message.delete()
        except Exception:
            pass
    except Exception as e:
        logger.error("Downloaded list send error: %s", e)
        try:
            await callback.message.edit_text(caption, parse_mode="HTML", reply_markup=nav_kb)
        except Exception:
            pass

    try:
        await callback.answer()
    except Exception:
        pass


# ─────────────────────────────────────────────
#  ADMIN — SUBMISSION HISTORY (all statuses)
# ─────────────────────────────────────────────

@router.callback_query(F.data.startswith("sub_history:"))
async def cb_sub_history(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    try:
        index = int(callback.data.split(":", 1)[1])
    except ValueError:
        index = 0

    all_subs = await DB.get_all_submissions()
    subs     = sorted(all_subs.values(), key=lambda x: x.get("date", ""), reverse=True)
    total    = len(subs)

    if total == 0:
        await callback.message.edit_text(
            "📋 <b>Submission History</b>\n\n📭 No submissions yet.",
            parse_mode="HTML",
            reply_markup=kb_back_admin(),
        )
        await callback.answer()
        return

    index  = max(0, min(index, total - 1))
    sub    = subs[index]
    sub_id = sub.get("submission_id", "N/A")
    status = sub.get("status", "pending")
    icon   = {"approved": "✅", "pending": "⏳", "rejected": "❌", "downloaded": "📥"}.get(status, "❓")

    extra = ""
    if status == "approved":
        extra = f"\n💰 Reward: ₹{sub.get('reward', IMAGE_REWARD)}\n✅ Approved: {_fmt(sub.get('approved_at', ''))}"
    elif status == "rejected":
        extra = f"\n❌ Rejected: {_fmt(sub.get('rejected_at', ''))}"
        if sub.get("reason"):
            extra += f"\n📝 Reason: {he(sub['reason'])}"
    elif status == "downloaded":
        extra = f"\n📥 Downloaded at: {_fmt(sub.get('downloaded_at', ''))}"

    caption = (
        f"📋 <b>Sub History</b> [{index + 1}/{total}]\n\n"
        f"🆔 Sub ID: <code>{sub_id}</code>\n"
        f"👤 User: <code>{sub.get('user_id')}</code>  @{he(sub.get('username') or 'N/A')}\n"
        f"📄 File: {he(sub.get('file_name', 'N/A'))}\n"
        f"📝 Title: {he(sub.get('title', 'N/A'))}\n"
        f"🔑 Keywords: <code>{he(sub.get('keywords') or 'N/A')}</code>\n"
        f"📁 Category: {he(sub.get('category') or 'N/A')}\n"
        f"📅 Submitted: {_fmt(sub.get('date', ''))}\n"
        f"{icon} Status: {status.capitalize()}{extra}"
    )

    nav = []
    if index > 0:
        nav.append(InlineKeyboardButton(text="◀ Prev", callback_data=f"sub_history:{index - 1}"))
    nav.append(InlineKeyboardButton(text=f"{index + 1}/{total}", callback_data="noop"))
    if index < total - 1:
        nav.append(InlineKeyboardButton(text="Next ▶", callback_data=f"sub_history:{index + 1}"))

    kb = InlineKeyboardMarkup(inline_keyboard=[nav, [InlineKeyboardButton(text="🔙 Back", callback_data="admin_back")]])

    try:
        await callback.message.answer_document(
            document=sub.get("file_id", ""),
            caption=caption,
            parse_mode="HTML",
            reply_markup=kb,
        )
        try:
            await callback.message.delete()
        except Exception:
            pass
    except Exception as e:
        logger.error("Sub history send error: %s", e)
        await callback.message.edit_text(caption, parse_mode="HTML", reply_markup=kb)

    await callback.answer()


# ─────────────────────────────────────────────
#  ADMIN — REJECTED HISTORY
# ─────────────────────────────────────────────

@router.callback_query(F.data.startswith("rejected_hist:"))
async def cb_rejected_hist(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    try:
        index = int(callback.data.split(":", 1)[1])
    except ValueError:
        index = 0

    all_subs = await DB.get_all_submissions()
    subs = sorted(
        [v for v in all_subs.values() if isinstance(v, dict) and v.get("status") == "rejected"],
        key=lambda x: x.get("rejected_at", x.get("date", "")),
        reverse=True,
    )
    total = len(subs)

    if total == 0:
        await callback.message.edit_text(
            "❌ <b>Rejected History</b>\n\n✅ No rejected submissions.",
            parse_mode="HTML",
            reply_markup=kb_back_admin(),
        )
        await callback.answer()
        return

    index  = max(0, min(index, total - 1))
    sub    = subs[index]
    sub_id = sub.get("submission_id", "N/A")

    caption = (
        f"❌ <b>Rejected History</b> [{index + 1}/{total}]\n\n"
        f"🆔 Sub ID: <code>{sub_id}</code>\n"
        f"👤 User: <code>{sub.get('user_id')}</code>  @{he(sub.get('username') or 'N/A')}\n"
        f"📝 Title: {he(sub.get('title', 'N/A'))}\n"
        f"📅 Submitted: {_fmt(sub.get('date', ''))}\n"
        f"❌ Rejected: {_fmt(sub.get('rejected_at', ''))}\n"
        f"📝 Reason: {he(sub.get('reason') or 'None given')}"
    )

    nav = []
    if index > 0:
        nav.append(InlineKeyboardButton(text="◀ Prev", callback_data=f"rejected_hist:{index - 1}"))
    nav.append(InlineKeyboardButton(text=f"{index + 1}/{total}", callback_data="noop"))
    if index < total - 1:
        nav.append(InlineKeyboardButton(text="Next ▶", callback_data=f"rejected_hist:{index + 1}"))

    kb = InlineKeyboardMarkup(inline_keyboard=[nav, [InlineKeyboardButton(text="🔙 Back", callback_data="admin_back")]])
    await callback.message.edit_text(caption, parse_mode="HTML", reply_markup=kb)
    await callback.answer()


# ─────────────────────────────────────────────
#  ADMIN — WITHDRAWAL REQUESTS
# ─────────────────────────────────────────────

@router.callback_query(F.data == "admin_withdrawals")
async def cb_admin_withdrawals(callback: CallbackQuery) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    pending = await DB.get_pending_withdrawals()
    if not pending:
        await callback.message.edit_text(
            "💳 <b>Withdraw Requests</b>\n\n✅ No pending requests!",
            parse_mode="HTML",
            reply_markup=kb_back_admin(),
        )
        await callback.answer()
        return

    await callback.message.edit_text(
        f"💳 <b>Pending Withdrawals</b> ({len(pending)})\nSending below…",
        parse_mode="HTML",
        reply_markup=kb_back_admin(),
    )
    for wid, wd in sorted(pending.items(), key=lambda x: x[1].get("date", ""))[:20]:
        method = wd.get("method", "upi")
        if method == "amazon":
            detail = f"🎁 Amazon: <code>{he(wd.get('amazon_id', 'N/A'))}</code>"
        else:
            detail = f"💳 UPI: <code>{he(wd.get('upi', 'N/A'))}</code>"
        txt = (
            f"💳 <b>Withdrawal Request</b>\n\n"
            f"🆔 ID: <code>{wid}</code>\n"
            f"👤 User: <code>{wd.get('user_id')}</code>\n"
            f"{detail}\n"
            f"💰 Amount: ₹{wd.get('amount', 0)}\n"
            f"📅 Date: {_fmt(wd.get('date', ''))}"
        )
        try:
            await callback.message.answer(txt, parse_mode="HTML", reply_markup=kb_wd_actions(wid))
        except Exception as e:
            logger.error("Send wd error: %s", e)
    await callback.answer()


@router.callback_query(F.data.startswith("approve_wd:"))
async def cb_approve_wd(callback: CallbackQuery, bot: Bot) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    wid = callback.data.split(":", 1)[1]
    wd  = await DB.get_withdrawal(wid)

    if not wd:
        await callback.answer("Withdrawal not found.", show_alert=True)
        return
    if wd.get("status") != "pending":
        await callback.answer(f"Already {wd.get('status')}.", show_alert=True)
        return

    await DB.update_withdrawal(wid, {"status": "approved", "approved_at": _now()})

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await callback.message.reply(f"✅ Withdrawal <code>{wid}</code> approved.", parse_mode="HTML")
    uid = to_int(wd.get("user_id", 0))
    try:
        await bot.send_message(
            uid,
            f"✅ <b>Withdrawal Approved!</b>\n\n"
            f"💰 Amount: ₹{wd.get('amount', 0)}\n"
            f"✅ Processed successfully.",
            parse_mode="HTML",
            reply_markup=kb_user_main(),
        )
    except Exception as e:
        logger.warning("Notify user %s failed: %s", uid, e)

    await callback.answer("Approved! ✅")


@router.callback_query(F.data.startswith("reject_wd:"))
async def cb_reject_wd(callback: CallbackQuery, bot: Bot) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    wid = callback.data.split(":", 1)[1]
    wd  = await DB.get_withdrawal(wid)

    if not wd:
        await callback.answer("Withdrawal not found.", show_alert=True)
        return
    if wd.get("status") != "pending":
        await callback.answer(f"Already {wd.get('status')}.", show_alert=True)
        return

    uid    = to_int(wd.get("user_id", 0))
    amount = to_int(wd.get("amount", 0))

    await DB.update_withdrawal(wid, {"status": "rejected", "rejected_at": _now()})
    await DB.add_balance(uid, amount)

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await callback.message.reply(
        f"❌ Withdrawal <code>{wid}</code> rejected. ₹{amount} refunded to user <code>{uid}</code>.",
        parse_mode="HTML",
    )

    try:
        await bot.send_message(
            uid,
            f"❌ <b>Withdrawal Rejected</b>\n\n"
            f"💰 Amount: ₹{amount} has been refunded to your balance.",
            parse_mode="HTML",
            reply_markup=kb_user_main(),
        )
    except Exception as e:
        logger.warning("Notify user %s failed: %s", uid, e)

    await callback.answer("Rejected.")


# ─────────────────────────────────────────────
#  FALLBACK HANDLER
# ─────────────────────────────────────────────

@router.message()
async def handle_fallback(message: Message, state: FSMContext) -> None:
    if await state.get_state() is not None:
        return
    uid = message.from_user.id
    if uid == ADMIN_CHAT_ID:
        await message.answer("Use the panel below:", reply_markup=kb_admin_main())
    else:
        await message.answer("Use the menu below:", reply_markup=kb_user_main())


# ─────────────────────────────────────────────
#  ADMIN — GENERATE CSV FOR ADOBE STOCK
# ─────────────────────────────────────────────

@router.callback_query(F.data == "generate_csv")
async def cb_generate_csv(callback: CallbackQuery, bot: Bot) -> None:
    if callback.from_user.id != ADMIN_CHAT_ID:
        await callback.answer("Unauthorized.", show_alert=True)
        return

    try:
        await callback.answer("⏳ Generating CSV…")
    except Exception:
        pass

    # Fetch ONLY downloaded submissions — no status changes made here
    all_subs = await DB.get_all_submissions()
    downloaded = sorted(
        [v for v in all_subs.values() if isinstance(v, dict) and v.get("status") == "downloaded"],
        key=lambda x: x.get("downloaded_at", x.get("date", "")),
    )

    if not downloaded:
        await callback.message.answer(
            "📄 <b>Generate CSV</b>\n\n"
            "❌ Downloaded List is empty.\n"
            "Mark images as Downloaded first, then generate CSV.",
            parse_mode="HTML",
            reply_markup=kb_admin_main(),
        )
        return

    # ── Build CSV in memory ───────────────────────────────────────────────
    # Adobe Stock required columns (case-sensitive, exact order)
    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_ALL, lineterminator="\n")

    # EXACT headers required by Adobe Stock
    writer.writerow(["Filename", "Title", "Keywords", "Category", "Releases"])

    for sub in downloaded:
        writer.writerow([
            sub.get("file_name", ""),   # EXACT original filename (e.g. IMG_123.jpg)
            sub.get("title",     ""),   # Title saved at upload time
            sub.get("keywords",  ""),   # Keywords saved at upload time
            "",                          # Category — blank as required
            "",                          # Releases — blank as required
        ])

    # UTF-8 with BOM so Excel / Adobe Stock opens it correctly
    csv_bytes = output.getvalue().encode("utf-8-sig")
    output.close()

    # ── Send CSV as document to admin chat ───────────────────────────────
    timestamp = _now().replace(" ", "_").replace(":", "-")
    filename  = f"adobe_stock_{timestamp}.csv"

    await bot.send_document(
        chat_id  = ADMIN_CHAT_ID,
        document = BufferedInputFile(file=csv_bytes, filename=filename),
        caption  = (
            f"📄 <b>Adobe Stock CSV</b>\n\n"
            f"✅ <b>{len(downloaded)}</b> items exported from Downloaded List.\n"
            f"📅 Generated: {_now()}"
        ),
        parse_mode = "HTML",
    )
    logger.info("CSV generated: %s items → %s", len(downloaded), filename)




# ─────────────────────────────────────────────
#  BOT / DISPATCHER / FASTAPI — Module-level init
#  (Vercel reuses warm containers; init runs once)
# ─────────────────────────────────────────────

# Thread pool for asyncio.to_thread Firebase calls
_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=20,
    thread_name_prefix="firebase-io",
)

# AiohttpSession: use proxy if detected (PythonAnywhere), otherwise direct
_bot_session = AiohttpSession(proxy=PROXY) if PROXY else AiohttpSession()

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    session=_bot_session,
)

# FirebaseFSMStorage — persists FSM state across serverless invocations
storage = FirebaseFSMStorage()
dp      = Dispatcher(storage=storage)

dp.message.middleware(BanMiddleware())
dp.callback_query.middleware(BanMiddleware())
dp.include_router(router)

# ─────────────────────────────────────────────
#  FASTAPI APP — Webhook endpoint
# ─────────────────────────────────────────────

app = FastAPI(title="ClickCash Bot", docs_url=None, redoc_url=None)


@app.get("/")
async def health() -> dict:
    """Health check — Vercel pings this to keep the container warm."""
    return {"status": "ok", "bot": "ClickCash Bot", "mode": "webhook"}


@app.post("/api/webhook")
async def webhook(request: Request) -> Response:
    """
    Main Telegram webhook endpoint.
    Telegram POSTs every update here; we feed it to aiogram Dispatcher.
    Must return 200 OK quickly — heavy work is awaited inline since
    Vercel functions are async-capable.
    """
    try:
        data   = await request.json()
        update = Update.model_validate(data, context={"bot": bot})
        await dp.feed_update(bot=bot, update=update)
    except Exception as e:
        logger.error("Webhook processing error: %s", e)
    # Always return 200 so Telegram doesn't retry endlessly
    return Response(content="ok", status_code=200)
