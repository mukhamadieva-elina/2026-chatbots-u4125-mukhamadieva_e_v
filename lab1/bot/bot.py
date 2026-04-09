import asyncio
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, time, timezone
from typing import Any, Dict, Optional, Tuple

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

try:
    # Python 3.9+
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("stands-bot")


ROLE_BACKEND = "backend"
ROLE_FRONTEND = "frontend"
ROLE_TESTER = "tester"
ROLE_OBSERVER = "observer"

ROLE_LABELS = {
    ROLE_BACKEND: "Бэкенд",
    ROLE_FRONTEND: "Фронтенд",
    ROLE_TESTER: "Тестировщик",
    ROLE_OBSERVER: "Наблюдатель",
}

STAND_BACKEND = "backend"
STAND_FRONTEND = "frontend"

MOSCOW_TZ = "Europe/Moscow"

MAIN_MENU_KEYBOARD = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton("/status"), KeyboardButton("/role")],
    ],
    resize_keyboard=True,
    one_time_keyboard=False,
    input_field_placeholder="Выберите действие…",
)


def _now_moscow() -> datetime:
    """
    Returns timezone-aware datetime in Moscow time.
    Falls back to UTC if zoneinfo isn't available.
    """
    if ZoneInfo is None:
        return datetime.now(timezone.utc)
    return datetime.now(ZoneInfo(MOSCOW_TZ))


def _format_taken_at(iso_dt: str) -> str:
    """
    Input: ISO string with timezone (best effort).
    Output: human-readable in Moscow time (DD.MM HH:MM).
    """
    try:
        dt = datetime.fromisoformat(iso_dt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if ZoneInfo is not None:
            dt = dt.astimezone(ZoneInfo(MOSCOW_TZ))
        return dt.strftime("%d.%m %H:%M")
    except Exception:
        return iso_dt


@dataclass(frozen=True)
class UserInfo:
    user_id: int
    username: str
    role: str
    is_admin: bool


class DataStore:
    """
    Very small JSON datastore with an in-process lock and atomic writes.

    Data schema (v1):
      {
        "version": 1,
        "users": {
          "<user_id>": {"role": "backend|frontend|tester|observer", "username": "name", "registered_at": "..."}
        },
        "stands": {
          "<microservice>": {
            "<stand_name>": {
              "type": "backend|frontend",
              "taken_by": null | {"user_id": 123, "username": "name", "taken_at": "..."}
            }
          }
        }
      }
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = asyncio.Lock()

    def _initial_data(self) -> Dict[str, Any]:
        return {
            "version": 1,
            "users": {},
            "stands": {
                "microservice_1": {
                    "feature_stand_1": {"type": STAND_BACKEND, "taken_by": None},
                    "feature_stand_2": {"type": STAND_BACKEND, "taken_by": None},
                    "feature_stand_3": {"type": STAND_BACKEND, "taken_by": None},
                },
                "frontend_app": {
                    "feature_stand_1": {"type": STAND_FRONTEND, "taken_by": None},
                    "feature_stand_2": {"type": STAND_FRONTEND, "taken_by": None},
                },
            },
        }

    async def load(self) -> Dict[str, Any]:
        async with self._lock:
            if not os.path.exists(self.path):
                data = self._initial_data()
                await self._save_nolock(data)
                return data

            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                logger.exception("Failed to read JSON; recreating initial structure")
                data = self._initial_data()
                await self._save_nolock(data)
                return data

    async def save(self, data: Dict[str, Any]) -> None:
        async with self._lock:
            await self._save_nolock(data)

    async def _save_nolock(self, data: Dict[str, Any]) -> None:
        tmp_path = f"{self.path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, self.path)


def _get_super_admin_id() -> Optional[int]:
    raw = os.getenv("SUPER_ADMIN_ID", "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _get_user_info(update: Update, data: Dict[str, Any]) -> Optional[UserInfo]:
    if update.effective_user is None:
        return None
    user_id = update.effective_user.id
    username = update.effective_user.username or f"id{user_id}"
    is_admin = (_get_super_admin_id() == user_id)

    user_row = data.get("users", {}).get(str(user_id))
    if not user_row:
        return None

    role = user_row.get("role", ROLE_OBSERVER)
    if role not in ROLE_LABELS:
        role = ROLE_OBSERVER
    return UserInfo(user_id=user_id, username=username, role=role, is_admin=is_admin)


def _can_manage_any(user: UserInfo) -> bool:
    if user.is_admin:
        return True
    return user.role == ROLE_TESTER


def _can_manage_stand(user: UserInfo, stand_type: str) -> bool:
    if user.is_admin or user.role == ROLE_TESTER:
        return True
    if user.role == ROLE_BACKEND:
        return stand_type == STAND_BACKEND
    if user.role == ROLE_FRONTEND:
        return stand_type == STAND_FRONTEND
    return False


def _build_role_keyboard(prefix: str = "role:set:") -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("Бэкенд", callback_data=f"{prefix}{ROLE_BACKEND}")],
        [InlineKeyboardButton("Фронтенд", callback_data=f"{prefix}{ROLE_FRONTEND}")],
        [InlineKeyboardButton("Тестировщик", callback_data=f"{prefix}{ROLE_TESTER}")],
        [InlineKeyboardButton("Наблюдатель", callback_data=f"{prefix}{ROLE_OBSERVER}")],
    ]
    return InlineKeyboardMarkup(rows)


def _welcome_text(role_label: Optional[str] = None) -> str:
    lines = ["Привет! Я бот для управления фича-стендами."]
    if role_label:
        lines.append(f"Ваша роль: <b>{role_label}</b>.")
    lines.append("")
    lines.append("Доступные команды:")
    lines.append("- /status — список стендов и кнопки управления")
    lines.append("- /role — смена роли")
    return "\n".join(lines)


def _stand_status_line(ms: str, stand_name: str, stand: Dict[str, Any]) -> str:
    taken_by = stand.get("taken_by")
    if not taken_by:
        return f"🟢 <b>{ms}</b> / <b>{stand_name}</b> — свободен"

    username = taken_by.get("username") or "unknown"
    taken_at = taken_by.get("taken_at") or "unknown"
    when = _format_taken_at(taken_at)
    return f"🔴 <b>{ms}</b> / <b>{stand_name}</b> — занят @{username} (с {when})"


def _build_status_message_and_keyboard(
    data: Dict[str, Any], user: Optional[UserInfo]
) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    stands = data.get("stands", {})
    lines: list[str] = []
    keyboard_rows: list[list[InlineKeyboardButton]] = []

    if user is None:
        lines.append("Вы не зарегистрированы. Нажмите /start и выберите роль.")
        return ("\n".join(lines), None)

    role_label = "Админ" if user.is_admin else ROLE_LABELS.get(user.role, user.role)
    lines.append(f"Ваша роль: <b>{role_label}</b>")
    lines.append("")
    lines.append("Стенды:")
    lines.append("")

    def is_visible_stand(stand_type: str) -> bool:
        # Observers can view everything (read-only).
        if user.role == ROLE_OBSERVER and not user.is_admin:
            return True

        # Admin/tester can view everything.
        if _can_manage_any(user):
            return True

        # Backend/front only view their stands to avoid confusion.
        if user.role == ROLE_BACKEND:
            return stand_type == STAND_BACKEND
        if user.role == ROLE_FRONTEND:
            return stand_type == STAND_FRONTEND

        return True

    for ms, ms_stands in stands.items():
        if not isinstance(ms_stands, dict):
            continue
        for stand_name, stand in ms_stands.items():
            if not isinstance(stand, dict):
                continue

            stand_type = stand.get("type", "")
            taken_by = stand.get("taken_by")

            if not is_visible_stand(stand_type):
                continue

            lines.append(_stand_status_line(ms, stand_name, stand))

            # Observers never get buttons.
            if user.role == ROLE_OBSERVER and not user.is_admin:
                continue

            if not _can_manage_stand(user, stand_type):
                continue

            cb_base = f"stand:{ms}:{stand_name}:"
            if taken_by is None:
                keyboard_rows.append([InlineKeyboardButton("Занять", callback_data=f"{cb_base}take")])
            else:
                # If occupied by someone else, show "Освободить" only to users who have the right to occupy it.
                keyboard_rows.append(
                    [InlineKeyboardButton("Освободить", callback_data=f"{cb_base}free")]
                )

    text = "\n".join(lines)
    return (text, InlineKeyboardMarkup(keyboard_rows) if keyboard_rows else None)


async def _safe_edit_message(
    update: Update,
    text: str,
    reply_markup: Optional[InlineKeyboardMarkup],
) -> None:
    """
    Edits the message that contains the pressed inline button.
    If Telegram rejects "message is not modified", we swallow it.
    """
    if update.callback_query is None:
        return
    try:
        await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except BadRequest as e:
        if "message is not modified" in str(e).lower():
            return
        raise


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: DataStore = context.application.bot_data["store"]
    data = await store.load()

    if update.effective_user is None or update.effective_chat is None:
        return

    user_id = update.effective_user.id
    username = update.effective_user.username or f"id{user_id}"

    user_row = data.get("users", {}).get(str(user_id))
    if user_row:
        role = user_row.get("role", ROLE_OBSERVER)
        role_label = "Админ" if (_get_super_admin_id() == user_id) else ROLE_LABELS.get(role, role)
        await update.message.reply_text(
            _welcome_text(role_label=role_label),
            parse_mode=ParseMode.HTML,
            reply_markup=MAIN_MENU_KEYBOARD,
        )
        return

    await update.message.reply_text(
        _welcome_text(role_label=None) + "\n\nВыберите вашу роль (можно сменить позже через /role).",
        parse_mode=ParseMode.HTML,
        reply_markup=_build_role_keyboard(prefix="role:first:"),
    )


async def cmd_role(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: DataStore = context.application.bot_data["store"]
    data = await store.load()

    user = _get_user_info(update, data)
    if user is None:
        await update.message.reply_text("Сначала зарегистрируйтесь командой /start.")
        return

    role_label = "Админ" if user.is_admin else ROLE_LABELS.get(user.role, user.role)
    await update.message.reply_text(
        f"Текущая роль: <b>{role_label}</b>\nВыберите новую роль:",
        parse_mode=ParseMode.HTML,
        reply_markup=_build_role_keyboard(prefix="role:set:"),
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: DataStore = context.application.bot_data["store"]
    data = await store.load()
    user = _get_user_info(update, data)

    text, keyboard = _build_status_message_and_keyboard(data, user)
    await update.message.reply_text(
        text=text,
        parse_mode=ParseMode.HTML,
        reply_markup=keyboard,
        disable_web_page_preview=True,
    )

async def on_menu_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Safety net: if a client sends menu text instead of command.
    (Reply keyboard buttons may send plain text; Telegram usually still parses /status, /role as commands,
    but we handle both to be robust.)
    """
    if update.message is None or update.message.text is None:
        return
    txt = update.message.text.strip().lower()
    if txt in {"/status", "status", "📋 статус"}:
        await cmd_status(update, context)
        return
    if txt in {"/role", "role", "👤 роль"}:
        await cmd_role(update, context)
        return


def _release_stands_user_cannot_hold(data: Dict[str, Any], user_id: int, new_role: str, is_admin: bool) -> int:
    """
    If the user changes role and loses permissions, release their stands automatically.
    Returns count of released stands.
    """
    if is_admin or new_role == ROLE_TESTER:
        return 0

    allowed_type = None
    if new_role == ROLE_BACKEND:
        allowed_type = STAND_BACKEND
    elif new_role == ROLE_FRONTEND:
        allowed_type = STAND_FRONTEND
    else:
        # Observer: cannot hold anything.
        allowed_type = None

    released = 0
    for ms_stands in (data.get("stands") or {}).values():
        if not isinstance(ms_stands, dict):
            continue
        for stand in ms_stands.values():
            if not isinstance(stand, dict):
                continue
            taken_by = stand.get("taken_by")
            if not taken_by or taken_by.get("user_id") != user_id:
                continue
            stand_type = stand.get("type")
            if allowed_type is None or stand_type != allowed_type:
                stand["taken_by"] = None
                released += 1
    return released


async def on_role_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: DataStore = context.application.bot_data["store"]
    data = await store.load()

    if update.callback_query is None or update.effective_user is None:
        return

    await update.callback_query.answer()

    cb = update.callback_query.data or ""
    # role:first:<role> or role:set:<role>
    parts = cb.split(":")
    if len(parts) != 3:
        return
    _, mode, new_role = parts
    if new_role not in ROLE_LABELS:
        return

    user_id = update.effective_user.id
    username = update.effective_user.username or f"id{user_id}"
    is_admin = (_get_super_admin_id() == user_id)

    users = data.setdefault("users", {})
    user_row = users.get(str(user_id))
    is_first = user_row is None

    if is_first:
        users[str(user_id)] = {
            "role": new_role,
            "username": username,
            "registered_at": _now_moscow().isoformat(),
        }
        released = 0
    else:
        prev_role = user_row.get("role", ROLE_OBSERVER)
        user_row["role"] = new_role
        user_row["username"] = username
        user_row["role_changed_at"] = _now_moscow().isoformat()

        # If new role cannot hold currently taken stands, release them.
        released = _release_stands_user_cannot_hold(data, user_id, new_role, is_admin=is_admin)
        user_row["released_on_role_change"] = released
        user_row["prev_role"] = prev_role

    await store.save(data)

    role_label = ROLE_LABELS[new_role]
    extra = ""
    if released:
        extra = f"\n\nАвтоматически освобождено стендов: <b>{released}</b> (из-за смены роли)."

    text = f"Роль установлена: <b>{role_label}</b>.{extra}\n\nОткройте /status."
    await _safe_edit_message(update, text=text, reply_markup=None)

    # Also send a separate message with the persistent menu keyboard.
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=_welcome_text(role_label=role_label),
            parse_mode=ParseMode.HTML,
            reply_markup=MAIN_MENU_KEYBOARD,
        )
    except Exception:
        logger.exception("Failed to send welcome/menu message after role set")


def _get_stand_ref(data: Dict[str, Any], ms: str, stand_name: str) -> Optional[Dict[str, Any]]:
    stands = data.get("stands") or {}
    ms_stands = stands.get(ms)
    if not isinstance(ms_stands, dict):
        return None
    stand = ms_stands.get(stand_name)
    if not isinstance(stand, dict):
        return None
    return stand


async def on_stand_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: DataStore = context.application.bot_data["store"]
    data = await store.load()

    if update.callback_query is None or update.effective_user is None:
        return
    await update.callback_query.answer()

    cb = update.callback_query.data or ""
    # stand:<microservice>:<stand_name>:take|free
    parts = cb.split(":")
    if len(parts) != 4:
        return
    _, ms, stand_name, action = parts

    user_id = update.effective_user.id
    username = update.effective_user.username or f"id{user_id}"
    is_admin = (_get_super_admin_id() == user_id)

    users = data.setdefault("users", {})
    user_row = users.get(str(user_id))
    if not user_row:
        await _safe_edit_message(
            update,
            text="Вы не зарегистрированы. Нажмите /start и выберите роль.",
            reply_markup=None,
        )
        return

    role = user_row.get("role", ROLE_OBSERVER)
    if role not in ROLE_LABELS:
        role = ROLE_OBSERVER

    user = UserInfo(user_id=user_id, username=username, role=role, is_admin=is_admin)

    stand = _get_stand_ref(data, ms, stand_name)
    if stand is None:
        await _safe_edit_message(update, text="Стенд не найден. Откройте /status заново.", reply_markup=None)
        return

    stand_type = stand.get("type", "")
    if user.role == ROLE_OBSERVER and not user.is_admin:
        # Observer never manages stands.
        text, keyboard = _build_status_message_and_keyboard(data, user)
        await _safe_edit_message(update, text=text, reply_markup=keyboard)
        return

    if not _can_manage_stand(user, stand_type):
        text, keyboard = _build_status_message_and_keyboard(data, user)
        await _safe_edit_message(update, text=text, reply_markup=keyboard)
        return

    taken_by = stand.get("taken_by")
    now_iso = _now_moscow().isoformat()

    if action == "take":
        if taken_by is not None:
            # Someone already took it; show updated status.
            text, keyboard = _build_status_message_and_keyboard(data, user)
            await _safe_edit_message(update, text=text, reply_markup=keyboard)
            return

        stand["taken_by"] = {"user_id": user_id, "username": username, "taken_at": now_iso}
        await store.save(data)

    elif action == "free":
        if taken_by is None:
            text, keyboard = _build_status_message_and_keyboard(data, user)
            await _safe_edit_message(update, text=text, reply_markup=keyboard)
            return

        # Freeing by:
        # - stand owner always can free (if they have permissions for that stand type)
        # - tester/admin can free any stand they are allowed to occupy (they are allowed to occupy any)
        stand["taken_by"] = None
        await store.save(data)

    else:
        return

    # Re-render the same message (edit) after state change.
    fresh = await store.load()
    fresh_user = _get_user_info(update, fresh)
    text, keyboard = _build_status_message_and_keyboard(fresh, fresh_user)
    await _safe_edit_message(update, text=text, reply_markup=keyboard)


async def daily_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Every day at 10:00 (Moscow) send a reminder to users that still occupy stands.
    """
    store: DataStore = context.application.bot_data["store"]
    data = await store.load()

    # Build mapping user_id -> list of stands
    occupied: Dict[int, list[str]] = {}
    stands = data.get("stands") or {}
    for ms, ms_stands in stands.items():
        if not isinstance(ms_stands, dict):
            continue
        for stand_name, stand in ms_stands.items():
            if not isinstance(stand, dict):
                continue
            taken_by = stand.get("taken_by")
            if not taken_by:
                continue
            uid = taken_by.get("user_id")
            if not isinstance(uid, int):
                continue
            occupied.setdefault(uid, []).append(f"{ms}/{stand_name}")

    if not occupied:
        return

    for uid, items in occupied.items():
        try:
            text = (
                "Напоминание: у вас есть занятые стенды:\n\n"
                + "\n".join([f"🔴 <b>{s}</b>" for s in items])
                + "\n\nЕсли стенды больше не нужны — освободите их через /status."
            )
            await context.bot.send_message(chat_id=uid, text=text, parse_mode=ParseMode.HTML)
        except Exception:
            logger.exception("Failed to send reminder to user_id=%s", uid)


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error", exc_info=context.error)
    # Best-effort: notify user if this happened during an interaction.
    try:
        if isinstance(update, Update) and update.effective_message is not None:
            await update.effective_message.reply_text("Произошла ошибка. Попробуйте ещё раз или откройте /status заново.")
    except Exception:
        logger.exception("Failed to notify user about error")


def main() -> None:
    load_dotenv()

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN in environment")

    data_file = os.getenv("DATA_FILE", "data.json").strip() or "data.json"
    store = DataStore(path=data_file)

    app = Application.builder().token(token).build()
    app.bot_data["store"] = store

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("role", cmd_role))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_menu_text))

    app.add_handler(CallbackQueryHandler(on_role_callback, pattern=r"^role:(first|set):(backend|frontend|tester|observer)$"))
    app.add_handler(CallbackQueryHandler(on_stand_callback, pattern=r"^stand:"))

    app.add_error_handler(on_error)

    # Daily reminders at 10:00 Moscow time.
    tz = ZoneInfo(MOSCOW_TZ) if ZoneInfo is not None else timezone.utc
    app.job_queue.run_daily(daily_reminder_job, time=time(hour=10, minute=0, tzinfo=tz), name="daily-reminder")

    logger.info("Bot started. Data file: %s", data_file)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
