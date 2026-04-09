import asyncio
import json
import logging
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, time, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

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
    ConversationHandler,
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


# ---- Roles (internal keys) ---------------------------------------------------
ROLE_BACKEND = "backend"
ROLE_FRONTEND = "frontend"
ROLE_QA = "qa"
ROLE_OBSERVER = "observer"
ROLE_ADMIN = "admin"

ROLE_LABELS_RU = {
    ROLE_BACKEND: "Бэкенд",
    ROLE_FRONTEND: "Фронтенд",
    ROLE_QA: "Тестировщик",
    ROLE_OBSERVER: "Наблюдатель",
    ROLE_ADMIN: "Админ",
}

# DB table `stands.type` values
STAND_BACKEND = "backend"
STAND_FRONTEND = "frontend"

MOSCOW_TZ = "Europe/Moscow"


def _main_menu_keyboard(*, is_admin: bool) -> ReplyKeyboardMarkup:
    """
    Persistent reply keyboard. For admins:
    - show /status and /admin
    For non-admins:
    - show /status and /role
    """
    if is_admin:
        keys = [[KeyboardButton("/status"), KeyboardButton("/admin")]]
    else:
        keys = [[KeyboardButton("/status"), KeyboardButton("/role")]]
    return ReplyKeyboardMarkup(
        keyboard=keys,
        resize_keyboard=True,
        one_time_keyboard=False,
        input_field_placeholder="Выберите действие…",
    )


def _now_moscow() -> datetime:
    """Timezone-aware now in Moscow time (fallback: UTC)."""
    if ZoneInfo is None:
        return datetime.now(timezone.utc)
    return datetime.now(ZoneInfo(MOSCOW_TZ))


def _format_taken_at(dt_iso: str) -> str:
    """Format ISO datetime to DD.MM HH:MM in Moscow time."""
    try:
        dt = datetime.fromisoformat(dt_iso)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if ZoneInfo is not None:
            dt = dt.astimezone(ZoneInfo(MOSCOW_TZ))
        return dt.strftime("%d.%m %H:%M")
    except Exception:
        return dt_iso


def _welcome_text(role_label: Optional[str] = None, *, show_admin: bool = False) -> str:
    lines = ["Привет! Я бот для управления фича-стендами."]
    if role_label:
        lines.append(f"Ваша роль: <b>{role_label}</b>.")
    lines.append("")
    lines.append("Доступные команды:")
    lines.append("- /status — стенды и кнопки управления")
    lines.append("- /role — смена роли")
    if show_admin:
        lines.append("- /admin — админка")
    return "\n".join(lines)


@dataclass(frozen=True)
class UserInfo:
    user_id: int
    username: str
    role: str
    is_admin: bool


def _parse_admin_ids() -> set[int]:
    """
    Supports:
    - SUPER_ADMIN_ID=123
    - SUPER_ADMIN_IDS=1,2,3
    """
    ids: set[int] = set()
    raw_single = os.getenv("SUPER_ADMIN_ID", "").strip()
    raw_multi = os.getenv("SUPER_ADMIN_IDS", "").strip()

    raw = ",".join([x for x in [raw_multi, raw_single] if x])
    if not raw:
        return ids

    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            continue
    return ids


# ---- SQLite layer ------------------------------------------------------------


SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS roles (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,          -- Telegram ID
  username TEXT,
  role_id INTEGER NOT NULL,
  FOREIGN KEY(role_id) REFERENCES roles(id)
);

CREATE TABLE IF NOT EXISTS services (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS stands (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  service_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  type TEXT NOT NULL CHECK(type IN ('backend','frontend')),
  UNIQUE(service_id, name),
  FOREIGN KEY(service_id) REFERENCES services(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS stand_usage (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  stand_id INTEGER NOT NULL,
  user_id INTEGER NOT NULL,
  booked_at TEXT NOT NULL,
  released_at TEXT, -- NULL => currently occupied
  FOREIGN KEY(stand_id) REFERENCES stands(id) ON DELETE CASCADE,
  FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_usage_active ON stand_usage(stand_id) WHERE released_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_usage_user_active ON stand_usage(user_id) WHERE released_at IS NULL;
"""


ROLE_DB_NAMES = {
    ROLE_BACKEND: "Backend",
    ROLE_FRONTEND: "Frontend",
    ROLE_QA: "QA",
    ROLE_OBSERVER: "Observer",
    ROLE_ADMIN: "Admin",
}


def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")
    return con


class SQLiteStore:
    """
    Simple SQLite store.
    - Uses one connection (thread-safe via a lock + to_thread).
    - All public methods are async to integrate with PTB async handlers.
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._lock = asyncio.Lock()
        self._con = _connect(db_path)

    async def close(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._con.close)

    async def init(self) -> None:
        async with self._lock:
            await asyncio.to_thread(self._con.executescript, SCHEMA_SQL)
            await asyncio.to_thread(self._seed_roles)
            await asyncio.to_thread(self._con.commit)

    def _seed_roles(self) -> None:
        for _, db_name in ROLE_DB_NAMES.items():
            self._con.execute("INSERT OR IGNORE INTO roles(name) VALUES (?)", (db_name,))

    async def is_empty(self) -> bool:
        async with self._lock:
            row = await asyncio.to_thread(
                self._con.execute,
                "SELECT COUNT(*) AS c FROM services",
            )
            c = row.fetchone()["c"]
            return int(c) == 0

    async def ensure_initial_stands(self) -> None:
        """
        Ensures the initial structure exists (only if services table is empty).
        """
        async with self._lock:
            row = await asyncio.to_thread(
                self._con.execute,
                "SELECT COUNT(*) AS c FROM services",
            )
            if int(row.fetchone()["c"]) > 0:
                return

            await asyncio.to_thread(self._create_service_with_stands_initial)
            await asyncio.to_thread(self._con.commit)

    def _create_service_with_stands_initial(self) -> None:
        # microservice_1 (backend stands)
        s1 = self._ensure_service("microservice_1")
        self._ensure_stand(s1, "feature_stand_1", STAND_BACKEND)
        self._ensure_stand(s1, "feature_stand_2", STAND_BACKEND)
        self._ensure_stand(s1, "feature_stand_3", STAND_BACKEND)

        # frontend_app (frontend stands)
        s2 = self._ensure_service("frontend_app")
        self._ensure_stand(s2, "feature_stand_1", STAND_FRONTEND)
        self._ensure_stand(s2, "feature_stand_2", STAND_FRONTEND)

    def _ensure_service(self, name: str) -> int:
        self._con.execute("INSERT OR IGNORE INTO services(name) VALUES (?)", (name,))
        row = self._con.execute("SELECT id FROM services WHERE name = ?", (name,)).fetchone()
        return int(row["id"])

    def _ensure_stand(self, service_id: int, name: str, stand_type: str) -> int:
        self._con.execute(
            "INSERT OR IGNORE INTO stands(service_id, name, type) VALUES (?,?,?)",
            (service_id, name, stand_type),
        )
        row = self._con.execute(
            "SELECT id FROM stands WHERE service_id = ? AND name = ?",
            (service_id, name),
        ).fetchone()
        return int(row["id"])

    async def migrate_from_json_if_needed(self, json_path: str) -> None:
        """
        One-time migration:
        - If DB already has services -> skip
        - If json exists -> load users + services/stands + active bookings into DB
        """
        if not os.path.exists(json_path):
            return
        if not await self.is_empty():
            return

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            logger.exception("Failed to read data.json for migration; skipping")
            return

        async with self._lock:
            await asyncio.to_thread(self._migrate_from_json_payload, payload)
            await asyncio.to_thread(self._con.commit)

    def _role_id_by_key(self, role_key: str) -> int:
        db_name = ROLE_DB_NAMES.get(role_key, ROLE_DB_NAMES[ROLE_OBSERVER])
        row = self._con.execute("SELECT id FROM roles WHERE name = ?", (db_name,)).fetchone()
        return int(row["id"])

    def _migrate_from_json_payload(self, payload: Dict[str, Any]) -> None:
        users = payload.get("users") or {}
        stands = payload.get("stands") or {}

        # services + stands
        for service_name, service_stands in stands.items():
            if not isinstance(service_stands, dict):
                continue
            sid = self._ensure_service(str(service_name))
            for stand_name, stand in service_stands.items():
                if not isinstance(stand, dict):
                    continue
                stype = stand.get("type")
                if stype not in (STAND_BACKEND, STAND_FRONTEND):
                    continue
                stand_id = self._ensure_stand(sid, str(stand_name), stype)

                taken_by = stand.get("taken_by")
                if not taken_by:
                    continue

                uid = taken_by.get("user_id")
                uname = taken_by.get("username") or f"id{uid}"
                booked_at = taken_by.get("taken_at") or _now_moscow().isoformat()
                if isinstance(uid, int):
                    # Ensure user exists (role will be migrated below as well)
                    self._con.execute(
                        "INSERT OR IGNORE INTO users(user_id, username, role_id) VALUES (?,?,?)",
                        (uid, uname, self._role_id_by_key(ROLE_OBSERVER)),
                    )
                    self._con.execute(
                        "INSERT INTO stand_usage(stand_id, user_id, booked_at, released_at) VALUES (?,?,?,NULL)",
                        (stand_id, uid, booked_at),
                    )

        # users + roles
        for user_id_str, u in users.items():
            try:
                uid = int(user_id_str)
            except Exception:
                continue
            if not isinstance(u, dict):
                continue
            role_key = u.get("role", ROLE_OBSERVER)
            if role_key == "tester":
                role_key = ROLE_QA
            if role_key not in ROLE_DB_NAMES:
                role_key = ROLE_OBSERVER
            uname = u.get("username") or f"id{uid}"
            rid = self._role_id_by_key(role_key)
            self._con.execute(
                """
                INSERT INTO users(user_id, username, role_id)
                VALUES (?,?,?)
                ON CONFLICT(user_id) DO UPDATE SET
                  username = excluded.username,
                  role_id = excluded.role_id
                """,
                (uid, uname, rid),
            )

    async def upsert_user(self, user_id: int, username: str, role_key: str) -> None:
        async with self._lock:
            rid = await asyncio.to_thread(self._role_id_by_key, role_key)
            await asyncio.to_thread(
                self._con.execute,
                """
                INSERT INTO users(user_id, username, role_id)
                VALUES (?,?,?)
                ON CONFLICT(user_id) DO UPDATE SET
                  username = excluded.username,
                  role_id = excluded.role_id
                """,
                (user_id, username, rid),
            )
            await asyncio.to_thread(self._con.commit)

    async def get_user(self, user_id: int) -> Optional[Tuple[int, str, str]]:
        """
        Returns (user_id, username, role_key)
        """
        async with self._lock:
            row = await asyncio.to_thread(
                self._con.execute,
                """
                SELECT u.user_id, u.username, r.name AS role_name
                FROM users u
                JOIN roles r ON r.id = u.role_id
                WHERE u.user_id = ?
                """,
                (user_id,),
            )
            r = row.fetchone()
            if not r:
                return None
            role_key = _role_key_from_db_name(str(r["role_name"]))
            return (int(r["user_id"]), str(r["username"] or ""), role_key)

    async def set_user_role(self, user_id: int, role_key: str) -> None:
        async with self._lock:
            rid = await asyncio.to_thread(self._role_id_by_key, role_key)
            await asyncio.to_thread(
                self._con.execute,
                "UPDATE users SET role_id = ? WHERE user_id = ?",
                (rid, user_id),
            )
            await asyncio.to_thread(self._con.commit)

    async def release_user_stands_not_allowed(self, user: UserInfo, new_role: str) -> int:
        """
        When user changes role and loses permissions, auto-release stands they occupy (active usage rows).
        """
        if user.is_admin or new_role == ROLE_QA:
            return 0

        allowed_type: Optional[str]
        if new_role == ROLE_BACKEND:
            allowed_type = STAND_BACKEND
        elif new_role == ROLE_FRONTEND:
            allowed_type = STAND_FRONTEND
        else:
            allowed_type = None  # observer can't hold anything

        async with self._lock:
            return await asyncio.to_thread(self._release_user_stands_not_allowed_sync, user.user_id, allowed_type)

    def _release_user_stands_not_allowed_sync(self, user_id: int, allowed_type: Optional[str]) -> int:
        # Find all active usages by user; release those not allowed.
        rows = self._con.execute(
            """
            SELECT su.id AS usage_id, s.type AS stand_type
            FROM stand_usage su
            JOIN stands s ON s.id = su.stand_id
            WHERE su.user_id = ? AND su.released_at IS NULL
            """,
            (user_id,),
        ).fetchall()

        to_release: List[int] = []
        for r in rows:
            stype = str(r["stand_type"])
            if allowed_type is None or stype != allowed_type:
                to_release.append(int(r["usage_id"]))

        if not to_release:
            return 0

        released_at = _now_moscow().isoformat()
        self._con.executemany(
            "UPDATE stand_usage SET released_at = ? WHERE id = ?",
            [(released_at, uid) for uid in to_release],
        )
        self._con.commit()
        return len(to_release)

    async def list_stands_for_status(self, user: UserInfo) -> List[sqlite3.Row]:
        """
        Returns rows: stand_id, service_name, stand_name, stand_type, taken_user_id, taken_username, booked_at
        """
        async with self._lock:
            return await asyncio.to_thread(self._list_stands_for_status_sync, user)

    def _list_stands_for_status_sync(self, user: UserInfo) -> List[sqlite3.Row]:
        # Visibility filter by role.
        type_filter: Optional[str] = None
        if not (user.is_admin or user.role == ROLE_QA or user.role == ROLE_OBSERVER):
            if user.role == ROLE_BACKEND:
                type_filter = STAND_BACKEND
            elif user.role == ROLE_FRONTEND:
                type_filter = STAND_FRONTEND

        base_sql = """
        SELECT
          s.id AS stand_id,
          sv.name AS service_name,
          s.name AS stand_name,
          s.type AS stand_type,
          su.user_id AS taken_user_id,
          u.username AS taken_username,
          su.booked_at AS booked_at
        FROM stands s
        JOIN services sv ON sv.id = s.service_id
        LEFT JOIN stand_usage su ON su.stand_id = s.id AND su.released_at IS NULL
        LEFT JOIN users u ON u.user_id = su.user_id
        """
        params: List[Any] = []
        if type_filter:
            base_sql += " WHERE s.type = ?"
            params.append(type_filter)
        base_sql += " ORDER BY sv.name, s.name"

        return self._con.execute(base_sql, params).fetchall()

    async def take_stand(self, stand_id: int, user_id: int) -> bool:
        """
        Returns True if taken, False if already taken.
        """
        async with self._lock:
            return await asyncio.to_thread(self._take_stand_sync, stand_id, user_id)

    def _take_stand_sync(self, stand_id: int, user_id: int) -> bool:
        active = self._con.execute(
            "SELECT id FROM stand_usage WHERE stand_id = ? AND released_at IS NULL",
            (stand_id,),
        ).fetchone()
        if active:
            return False

        booked_at = _now_moscow().isoformat()
        self._con.execute(
            "INSERT INTO stand_usage(stand_id, user_id, booked_at, released_at) VALUES (?,?,?,NULL)",
            (stand_id, user_id, booked_at),
        )
        self._con.commit()
        return True

    async def free_stand(self, stand_id: int) -> bool:
        """
        Returns True if freed, False if it wasn't occupied.
        """
        async with self._lock:
            return await asyncio.to_thread(self._free_stand_sync, stand_id)

    def _free_stand_sync(self, stand_id: int) -> bool:
        active = self._con.execute(
            "SELECT id FROM stand_usage WHERE stand_id = ? AND released_at IS NULL",
            (stand_id,),
        ).fetchone()
        if not active:
            return False
        released_at = _now_moscow().isoformat()
        self._con.execute("UPDATE stand_usage SET released_at = ? WHERE id = ?", (released_at, int(active["id"])))
        self._con.commit()
        return True

    async def get_stand_status(self, stand_id: int) -> Optional[sqlite3.Row]:
        async with self._lock:
            row = await asyncio.to_thread(
                self._con.execute,
                """
                SELECT
                  s.id AS stand_id,
                  sv.name AS service_name,
                  s.name AS stand_name,
                  s.type AS stand_type,
                  su.user_id AS taken_user_id,
                  u.username AS taken_username,
                  su.booked_at AS booked_at
                FROM stands s
                JOIN services sv ON sv.id = s.service_id
                LEFT JOIN stand_usage su ON su.stand_id = s.id AND su.released_at IS NULL
                LEFT JOIN users u ON u.user_id = su.user_id
                WHERE s.id = ?
                """,
                (stand_id,),
            )
            return row.fetchone()

    async def list_active_usages_by_user(self) -> Dict[int, List[str]]:
        async with self._lock:
            return await asyncio.to_thread(self._list_active_usages_by_user_sync)

    def _list_active_usages_by_user_sync(self) -> Dict[int, List[str]]:
        rows = self._con.execute(
            """
            SELECT su.user_id, sv.name AS service_name, s.name AS stand_name
            FROM stand_usage su
            JOIN stands s ON s.id = su.stand_id
            JOIN services sv ON sv.id = s.service_id
            WHERE su.released_at IS NULL
            ORDER BY su.user_id, sv.name, s.name
            """
        ).fetchall()

        out: Dict[int, List[str]] = {}
        for r in rows:
            uid = int(r["user_id"])
            out.setdefault(uid, []).append(f"{r['service_name']}/{r['stand_name']}")
        return out

    # ---- Admin operations ----------------------------------------------------

    async def admin_create_service(self, name: str) -> Tuple[int, bool]:
        """
        Returns (service_id, created)
        """
        async with self._lock:
            return await asyncio.to_thread(self._admin_create_service_sync, name)

    def _admin_create_service_sync(self, name: str) -> Tuple[int, bool]:
        name = name.strip()
        if not name:
            raise ValueError("Empty service name")
        existed = (
            self._con.execute("SELECT 1 FROM services WHERE name = ?", (name,)).fetchone() is not None
        )
        if not existed:
            self._con.execute("INSERT INTO services(name) VALUES (?)", (name,))
        row = self._con.execute("SELECT id FROM services WHERE name = ?", (name,)).fetchone()
        self._con.commit()
        return (int(row["id"]), not existed)

    async def admin_list_services(self) -> List[sqlite3.Row]:
        async with self._lock:
            row = await asyncio.to_thread(self._con.execute, "SELECT id, name FROM services ORDER BY name")
            return row.fetchall()

    async def admin_create_stand(self, service_name: str, stand_name: str, stand_type: str) -> Tuple[int, bool]:
        """
        Returns (stand_id, created)
        """
        async with self._lock:
            return await asyncio.to_thread(
                self._admin_create_stand_sync, service_name.strip(), stand_name.strip(), stand_type
            )

    def _admin_create_stand_sync(self, service_name: str, stand_name: str, stand_type: str) -> Tuple[int, bool]:
        if stand_type not in (STAND_BACKEND, STAND_FRONTEND):
            raise ValueError("Invalid stand type")
        if not service_name or not stand_name:
            raise ValueError("Empty service/stand name")

        sid = self._ensure_service(service_name)
        existed = (
            self._con.execute(
                "SELECT 1 FROM stands WHERE service_id = ? AND name = ?",
                (sid, stand_name),
            ).fetchone()
            is not None
        )
        stand_id = self._ensure_stand(sid, stand_name, stand_type)
        self._con.commit()
        return (stand_id, not existed)

    async def admin_list_stands_by_service(self, service_id: int) -> List[sqlite3.Row]:
        async with self._lock:
            row = await asyncio.to_thread(
                self._con.execute,
                """
                SELECT s.id, s.name, s.type
                FROM stands s
                WHERE s.service_id = ?
                ORDER BY s.name
                """,
                (service_id,),
            )
            return row.fetchall()

    async def admin_delete_stand(self, stand_id: int) -> bool:
        async with self._lock:
            return await asyncio.to_thread(self._admin_delete_stand_sync, stand_id)

    def _admin_delete_stand_sync(self, stand_id: int) -> bool:
        active = self._con.execute(
            "SELECT id FROM stand_usage WHERE stand_id = ? AND released_at IS NULL",
            (stand_id,),
        ).fetchone()
        if active:
            return False
        cur = self._con.execute("DELETE FROM stands WHERE id = ?", (stand_id,))
        self._con.commit()
        return cur.rowcount > 0

    async def admin_force_free_stand(self, stand_id: int) -> bool:
        """
        Force-release a stand if it is occupied right now.
        Returns True if something was released, otherwise False.
        """
        async with self._lock:
            return await asyncio.to_thread(self._admin_force_free_stand_sync, stand_id)

    def _admin_force_free_stand_sync(self, stand_id: int) -> bool:
        active = self._con.execute(
            "SELECT id FROM stand_usage WHERE stand_id = ? AND released_at IS NULL",
            (stand_id,),
        ).fetchone()
        if not active:
            return False
        released_at = _now_moscow().isoformat()
        self._con.execute("UPDATE stand_usage SET released_at = ? WHERE id = ?", (released_at, int(active["id"])))
        self._con.commit()
        return True

    async def admin_delete_service(self, service_id: int, force_release: bool = False) -> Tuple[bool, int]:
        """
        Deletes a service.
        - If force_release=False and any stand in this service is occupied -> will not delete.
        - If force_release=True -> releases all occupied stands in this service, then deletes.
        Returns: (deleted, released_count)
        """
        async with self._lock:
            return await asyncio.to_thread(self._admin_delete_service_sync, service_id, force_release)

    def _admin_delete_service_sync(self, service_id: int, force_release: bool) -> Tuple[bool, int]:
        released_count = 0
        active = self._con.execute(
            """
            SELECT COUNT(*) AS c
            FROM stand_usage su
            JOIN stands s ON s.id = su.stand_id
            WHERE s.service_id = ? AND su.released_at IS NULL
            """,
            (service_id,),
        ).fetchone()
        active_count = int(active["c"]) if active else 0

        if active_count > 0 and not force_release:
            return (False, 0)

        if active_count > 0 and force_release:
            released_at = _now_moscow().isoformat()
            cur = self._con.execute(
                """
                UPDATE stand_usage
                SET released_at = ?
                WHERE id IN (
                  SELECT su.id
                  FROM stand_usage su
                  JOIN stands s ON s.id = su.stand_id
                  WHERE s.service_id = ? AND su.released_at IS NULL
                )
                """,
                (released_at, service_id),
            )
            released_count = cur.rowcount or 0

        cur2 = self._con.execute("DELETE FROM services WHERE id = ?", (service_id,))
        self._con.commit()
        return (cur2.rowcount > 0, released_count)


def _role_key_from_db_name(db_name: str) -> str:
    inv = {v: k for k, v in ROLE_DB_NAMES.items()}
    return inv.get(db_name, ROLE_OBSERVER)


def _stand_status_line_from_row(row: sqlite3.Row) -> str:
    ms = str(row["service_name"])
    stand_name = str(row["stand_name"])
    taken_user_id = row["taken_user_id"]
    if taken_user_id is None:
        return f"🟢 <b>{ms}</b> / <b>{stand_name}</b> — свободен"
    username = row["taken_username"] or f"id{taken_user_id}"
    booked_at = row["booked_at"] or ""
    when = _format_taken_at(str(booked_at))
    return f"🔴 <b>{ms}</b> / <b>{stand_name}</b> — занят @{username} (с {when})"


def _build_role_keyboard(prefix: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("Бэкенд", callback_data=f"{prefix}{ROLE_BACKEND}")],
        [InlineKeyboardButton("Фронтенд", callback_data=f"{prefix}{ROLE_FRONTEND}")],
        [InlineKeyboardButton("Тестировщик", callback_data=f"{prefix}{ROLE_QA}")],
        [InlineKeyboardButton("Наблюдатель", callback_data=f"{prefix}{ROLE_OBSERVER}")],
    ]
    return InlineKeyboardMarkup(rows)


def _can_manage_stand(user: UserInfo, stand_type: str) -> bool:
    if user.is_admin or user.role in (ROLE_QA, ROLE_ADMIN):
        return True
    if user.role == ROLE_BACKEND:
        return stand_type == STAND_BACKEND
    if user.role == ROLE_FRONTEND:
        return stand_type == STAND_FRONTEND
    return False


def _build_single_stand_message_and_keyboard(row: sqlite3.Row, user: UserInfo) -> Tuple[str, Optional[InlineKeyboardMarkup]]:
    text = _stand_status_line_from_row(row)

    # Observers never get buttons
    if user.role == ROLE_OBSERVER and not user.is_admin:
        return (text, None)

    stand_type = str(row["stand_type"])
    stand_id = int(row["stand_id"])
    cb_base = f"stand:{stand_id}:"
    taken_user_id = row["taken_user_id"]
    is_owner = taken_user_id is not None and int(taken_user_id) == user.user_id

    # Show "Занять" only if user can manage this stand type.
    if taken_user_id is None:
        if not _can_manage_stand(user, stand_type):
            return (text, None)
        return (text, InlineKeyboardMarkup([[InlineKeyboardButton("Занять", callback_data=f"{cb_base}take")]]))

    # Occupied: show "Освободить" if:
    # - user is the owner, OR
    # - user is QA/Admin (can free any stand)
    # Backend/Frontend cannot free stands occupied by other users.
    if is_owner or user.is_admin or user.role in (ROLE_QA, ROLE_ADMIN):
        return (text, InlineKeyboardMarkup([[InlineKeyboardButton("Освободить", callback_data=f"{cb_base}free")]]))
    return (text, None)


async def _safe_edit_message(update: Update, text: str, reply_markup: Optional[InlineKeyboardMarkup]) -> None:
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


def _user_from_db(user_id: int, username: str, role: str, admin_ids: set[int]) -> UserInfo:
    return UserInfo(
        user_id=user_id,
        username=username,
        role=role,
        is_admin=(user_id in admin_ids),
    )


# ---- Commands ----------------------------------------------------------------


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data["store"]
    admin_ids: set[int] = context.application.bot_data["admin_ids"]

    if update.effective_user is None or update.message is None:
        return

    uid = update.effective_user.id
    uname = update.effective_user.username or f"id{uid}"
    is_admin = uid in admin_ids

    row = await store.get_user(uid)
    if row:
        _, db_username, role_key = row
        role_label = ROLE_LABELS_RU[ROLE_ADMIN] if is_admin else ROLE_LABELS_RU.get(role_key, role_key)
        await update.message.reply_text(
            _welcome_text(role_label=role_label, show_admin=is_admin),
            parse_mode=ParseMode.HTML,
            reply_markup=_main_menu_keyboard(is_admin=is_admin),
        )
        # Keep username up to date
        await store.upsert_user(uid, uname, role_key if not is_admin else ROLE_ADMIN)
        return

    # New user: choose role
    await update.message.reply_text(
        _welcome_text(role_label=None, show_admin=is_admin) + "\n\nВыберите вашу роль (можно сменить позже через /role).",
        parse_mode=ParseMode.HTML,
        reply_markup=_build_role_keyboard(prefix="role:first:"),
    )


async def cmd_role(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data["store"]
    admin_ids: set[int] = context.application.bot_data["admin_ids"]

    if update.effective_user is None or update.message is None:
        return

    uid = update.effective_user.id
    if uid in admin_ids:
        await update.message.reply_text("Админу смена роли недоступна.")
        return

    user_row = await store.get_user(uid)
    if not user_row:
        await update.message.reply_text("Сначала зарегистрируйтесь командой /start.")
        return

    _, _, role_key = user_row
    role_label = ROLE_LABELS_RU[ROLE_ADMIN] if uid in admin_ids else ROLE_LABELS_RU.get(role_key, role_key)
    await update.message.reply_text(
        f"Текущая роль: <b>{role_label}</b>\nВыберите новую роль:",
        parse_mode=ParseMode.HTML,
        reply_markup=_build_role_keyboard(prefix="role:set:"),
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data["store"]
    admin_ids: set[int] = context.application.bot_data["admin_ids"]

    if update.effective_user is None or update.effective_chat is None or update.message is None:
        return

    uid = update.effective_user.id
    uname = update.effective_user.username or f"id{uid}"

    user_row = await store.get_user(uid)
    if not user_row:
        await update.message.reply_text("Вы не зарегистрированы. Нажмите /start и выберите роль.")
        return

    _, _, role_key = user_row
    # keep username up to date
    await store.upsert_user(uid, uname, role_key if uid not in admin_ids else ROLE_ADMIN)
    user = _user_from_db(uid, uname, role_key, admin_ids)

    role_label = ROLE_LABELS_RU[ROLE_ADMIN] if user.is_admin else ROLE_LABELS_RU.get(user.role, user.role)
    header = f"Ваша роль: <b>{role_label}</b>\n\nСтенды (кнопка действия сразу под стендом):"
    await update.message.reply_text(
        header,
        parse_mode=ParseMode.HTML,
        reply_markup=_main_menu_keyboard(is_admin=user.is_admin),
        disable_web_page_preview=True,
    )

    rows = await store.list_stands_for_status(user)
    for r in rows:
        text, kb = _build_single_stand_message_and_keyboard(r, user)
        await update.message.reply_text(
            text=text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
            disable_web_page_preview=True,
        )


async def on_menu_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message is None or update.message.text is None:
        return
    txt = update.message.text.strip().lower()
    if txt in {"/status", "status"}:
        await cmd_status(update, context)
    elif txt in {"/role", "role"}:
        await cmd_role(update, context)
    elif txt in {"/admin", "admin"}:
        await cmd_admin(update, context)


# ---- Role callbacks -----------------------------------------------------------


async def on_role_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data["store"]
    admin_ids: set[int] = context.application.bot_data["admin_ids"]

    if update.callback_query is None or update.effective_user is None:
        return
    await update.callback_query.answer()

    cb = update.callback_query.data or ""
    parts = cb.split(":")
    if len(parts) != 3:
        return
    _, mode, new_role = parts
    if new_role not in (ROLE_BACKEND, ROLE_FRONTEND, ROLE_QA, ROLE_OBSERVER):
        return

    uid = update.effective_user.id
    uname = update.effective_user.username or f"id{uid}"
    is_admin = uid in admin_ids
    if is_admin:
        # Admins do not switch roles via UI; keep admin role fixed.
        await _safe_edit_message(update, "Админу смена роли недоступна.", reply_markup=None)
        return

    existing = await store.get_user(uid)
    if existing is None:
        await store.upsert_user(uid, uname, ROLE_ADMIN if is_admin else new_role)
        released = 0
    else:
        _, _, prev_role = existing
        user = _user_from_db(uid, uname, prev_role, admin_ids)
        released = await store.release_user_stands_not_allowed(user, new_role)
        await store.upsert_user(uid, uname, ROLE_ADMIN if is_admin else new_role)

    role_label = ROLE_LABELS_RU[new_role]
    extra = ""
    if released:
        extra = f"\n\nАвтоматически освобождено стендов: <b>{released}</b> (из-за смены роли)."

    await _safe_edit_message(update, text=f"Роль установлена: <b>{role_label}</b>.{extra}", reply_markup=None)

    # Send a persistent menu message
    try:
        await context.bot.send_message(
            chat_id=uid,
            text=_welcome_text(
                role_label=(ROLE_LABELS_RU[ROLE_ADMIN] if is_admin else role_label),
                show_admin=is_admin,
            ),
            parse_mode=ParseMode.HTML,
            reply_markup=_main_menu_keyboard(is_admin=is_admin),
        )
    except Exception:
        logger.exception("Failed to send welcome/menu after role set")


# ---- Stand callbacks ----------------------------------------------------------


async def on_stand_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data["store"]
    admin_ids: set[int] = context.application.bot_data["admin_ids"]

    if update.callback_query is None or update.effective_user is None:
        return
    await update.callback_query.answer()

    cb = update.callback_query.data or ""
    parts = cb.split(":")
    if len(parts) != 3:
        return
    _, stand_id_raw, action = parts
    try:
        stand_id = int(stand_id_raw)
    except ValueError:
        return

    uid = update.effective_user.id
    uname = update.effective_user.username or f"id{uid}"

    user_row = await store.get_user(uid)
    if not user_row:
        await _safe_edit_message(update, "Вы не зарегистрированы. Нажмите /start.", None)
        return
    _, _, role_key = user_row
    await store.upsert_user(uid, uname, role_key if uid not in admin_ids else ROLE_ADMIN)
    user = _user_from_db(uid, uname, role_key, admin_ids)

    stand_row = await store.get_stand_status(stand_id)
    if stand_row is None:
        await _safe_edit_message(update, "Стенд не найден. Откройте /status заново.", None)
        return

    stand_type = str(stand_row["stand_type"])
    if user.role == ROLE_OBSERVER and not user.is_admin:
        text = _stand_status_line_from_row(stand_row)
        await _safe_edit_message(update, text, None)
        return

    taken_user_id = stand_row["taken_user_id"]
    is_owner = taken_user_id is not None and int(taken_user_id) == uid
    can_manage_this = _can_manage_stand(user, stand_type)

    if action == "take":
        if not can_manage_this:
            text = _stand_status_line_from_row(stand_row)
            await _safe_edit_message(update, text, None)
            return
        ok = await store.take_stand(stand_id, uid)
        if not ok:
            # refresh
            fresh = await store.get_stand_status(stand_id)
            if fresh:
                text, kb = _build_single_stand_message_and_keyboard(fresh, user)
                await _safe_edit_message(update, text, kb)
            return
    elif action == "free":
        # Allow freeing if:
        # - owner, or
        # - QA/Admin (can free any)
        if not (is_owner or user.is_admin or user.role in (ROLE_QA, ROLE_ADMIN)):
            text = _stand_status_line_from_row(stand_row)
            await _safe_edit_message(update, text, None)
            return
        await store.free_stand(stand_id)
    else:
        return

    fresh = await store.get_stand_status(stand_id)
    if fresh is None:
        await _safe_edit_message(update, "Стенд не найден. Откройте /status заново.", None)
        return
    text, kb = _build_single_stand_message_and_keyboard(fresh, user)
    await _safe_edit_message(update, text, kb)


# ---- Admin panel --------------------------------------------------------------


ADMIN_MENU, ADD_SERVICE_NAME, ADD_STAND_SERVICE, ADD_STAND_NAME, ADD_STAND_TYPE = range(5)


def _is_admin_user(update: Update, admin_ids: set[int]) -> bool:
    return update.effective_user is not None and update.effective_user.id in admin_ids


def _admin_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("➕ Добавить микросервис", callback_data="admin:add_service")],
            [InlineKeyboardButton("➕ Добавить стенд", callback_data="admin:add_stand")],
            [InlineKeyboardButton("🧹 Принудительно освободить стенд", callback_data="admin:force_free_stand")],
            [InlineKeyboardButton("🗑 Удалить стенд", callback_data="admin:delete_stand")],
            [InlineKeyboardButton("🗑 Удалить микросервис", callback_data="admin:delete_service")],
            [InlineKeyboardButton("Закрыть", callback_data="admin:close")],
        ]
    )


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    admin_ids: set[int] = context.application.bot_data["admin_ids"]
    if not _is_admin_user(update, admin_ids):
        if update.message:
            await update.message.reply_text("Команда доступна только администратору.")
        return ConversationHandler.END

    if update.message:
        await update.message.reply_text(
            "Админка: выберите действие.",
            reply_markup=_admin_menu_keyboard(),
        )
    return ADMIN_MENU


async def admin_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    store: SQLiteStore = context.application.bot_data["store"]
    admin_ids: set[int] = context.application.bot_data["admin_ids"]

    if update.callback_query is None:
        return ConversationHandler.END
    await update.callback_query.answer()

    if not _is_admin_user(update, admin_ids):
        await _safe_edit_message(update, "Команда доступна только администратору.", None)
        return ConversationHandler.END

    data = update.callback_query.data or ""
    if data == "admin:close":
        await _safe_edit_message(update, "Админка закрыта.", None)
        return ConversationHandler.END

    if data == "admin:add_service":
        await _safe_edit_message(update, "Введите название микросервиса:", None)
        return ADD_SERVICE_NAME

    if data == "admin:add_stand":
        # step 1: ask service name
        await _safe_edit_message(update, "Введите название микросервиса (существующего или нового):", None)
        return ADD_STAND_SERVICE

    if data == "admin:force_free_stand":
        services = await store.admin_list_services()
        if not services:
            await _safe_edit_message(update, "Нет микросервисов.", _admin_menu_keyboard())
            return ADMIN_MENU
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton(str(s["name"]), callback_data=f"admin:ff_service:{int(s['id'])}")] for s in services]
            + [[InlineKeyboardButton("⬅️ Назад", callback_data="admin:back")]]
        )
        await _safe_edit_message(update, "Выберите микросервис:", kb)
        return ADMIN_MENU

    if data == "admin:delete_stand":
        services = await store.admin_list_services()
        if not services:
            await _safe_edit_message(update, "Нет микросервисов.", _admin_menu_keyboard())
            return ADMIN_MENU
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton(str(s["name"]), callback_data=f"admin:del_service:{int(s['id'])}")] for s in services]
            + [[InlineKeyboardButton("⬅️ Назад", callback_data="admin:back")]]
        )
        await _safe_edit_message(update, "Выберите микросервис:", kb)
        return ADMIN_MENU

    if data == "admin:delete_service":
        services = await store.admin_list_services()
        if not services:
            await _safe_edit_message(update, "Нет микросервисов.", _admin_menu_keyboard())
            return ADMIN_MENU
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton(str(s["name"]), callback_data=f"admin:del_srv_confirm:{int(s['id'])}")] for s in services]
            + [[InlineKeyboardButton("⬅️ Назад", callback_data="admin:back")]]
        )
        await _safe_edit_message(update, "Выберите микросервис для удаления:", kb)
        return ADMIN_MENU

    if data == "admin:back":
        await _safe_edit_message(update, "Админка: выберите действие.", _admin_menu_keyboard())
        return ADMIN_MENU

    if data.startswith("admin:del_service:"):
        try:
            service_id = int(data.split(":")[-1])
        except Exception:
            return ADMIN_MENU
        stands = await store.admin_list_stands_by_service(service_id)
        if not stands:
            await _safe_edit_message(update, "В этом микросервисе нет стендов.", _admin_menu_keyboard())
            return ADMIN_MENU
        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton(f"{st['name']} ({st['type']})", callback_data=f"admin:del_stand:{int(st['id'])}")]
                for st in stands
            ]
            + [[InlineKeyboardButton("⬅️ Назад", callback_data="admin:delete_stand")]]
        )
        await _safe_edit_message(update, "Выберите стенд для удаления:", kb)
        return ADMIN_MENU

    if data.startswith("admin:ff_service:"):
        try:
            service_id = int(data.split(":")[-1])
        except Exception:
            return ADMIN_MENU
        stands = await store.admin_list_stands_by_service(service_id)
        if not stands:
            await _safe_edit_message(update, "В этом микросервисе нет стендов.", _admin_menu_keyboard())
            return ADMIN_MENU
        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton(f"{st['name']} ({st['type']})", callback_data=f"admin:ff_stand:{int(st['id'])}")]
                for st in stands
            ]
            + [[InlineKeyboardButton("⬅️ Назад", callback_data="admin:force_free_stand")]]
        )
        await _safe_edit_message(update, "Выберите стенд для принудительного освобождения:", kb)
        return ADMIN_MENU

    if data.startswith("admin:ff_stand:"):
        try:
            stand_id = int(data.split(":")[-1])
        except Exception:
            return ADMIN_MENU
        changed = await store.admin_force_free_stand(stand_id)
        if not changed:
            await _safe_edit_message(update, "Стенд уже свободен.", _admin_menu_keyboard())
            return ADMIN_MENU
        await _safe_edit_message(update, "Стенд принудительно освобождён.", _admin_menu_keyboard())
        return ADMIN_MENU

    if data.startswith("admin:del_srv_confirm:"):
        try:
            service_id = int(data.split(":")[-1])
        except Exception:
            return ADMIN_MENU
        # Try delete without force to detect occupied stands
        deleted, released = await store.admin_delete_service(service_id, force_release=False)
        if deleted:
            await _safe_edit_message(update, "Микросервис удалён.", _admin_menu_keyboard())
            return ADMIN_MENU

        kb = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("🧹 Освободить все стенды и удалить", callback_data=f"admin:del_srv_force:{service_id}")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="admin:delete_service")],
            ]
        )
        await _safe_edit_message(
            update,
            "Микросервис содержит занятые стенды. Что сделать?",
            kb,
        )
        return ADMIN_MENU

    if data.startswith("admin:del_srv_force:"):
        try:
            service_id = int(data.split(":")[-1])
        except Exception:
            return ADMIN_MENU
        deleted, released = await store.admin_delete_service(service_id, force_release=True)
        if not deleted:
            await _safe_edit_message(update, "Не удалось удалить микросервис.", _admin_menu_keyboard())
            return ADMIN_MENU
        await _safe_edit_message(update, f"Микросервис удалён. Освобождено стендов: {released}.", _admin_menu_keyboard())
        return ADMIN_MENU

    if data.startswith("admin:del_stand:"):
        try:
            stand_id = int(data.split(":")[-1])
        except Exception:
            return ADMIN_MENU
        ok = await store.admin_delete_stand(stand_id)
        if not ok:
            kb = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🧹 Освободить и удалить", callback_data=f"admin:force_del_stand:{stand_id}")],
                    [InlineKeyboardButton("⬅️ Назад", callback_data="admin:delete_stand")],
                ]
            )
            await _safe_edit_message(update, "Стенд сейчас занят. Что сделать?", kb)
            return ADMIN_MENU
        await _safe_edit_message(update, "Стенд удалён.", _admin_menu_keyboard())
        return ADMIN_MENU

    if data.startswith("admin:force_del_stand:"):
        try:
            stand_id = int(data.split(":")[-1])
        except Exception:
            return ADMIN_MENU
        await store.admin_force_free_stand(stand_id)
        ok = await store.admin_delete_stand(stand_id)
        if not ok:
            await _safe_edit_message(update, "Не удалось удалить стенд.", _admin_menu_keyboard())
            return ADMIN_MENU
        await _safe_edit_message(update, "Стенд освобождён и удалён.", _admin_menu_keyboard())
        return ADMIN_MENU

    return ADMIN_MENU


async def admin_add_service_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    store: SQLiteStore = context.application.bot_data["store"]
    admin_ids: set[int] = context.application.bot_data["admin_ids"]

    if update.message is None or update.message.text is None:
        return ADMIN_MENU
    if not _is_admin_user(update, admin_ids):
        await update.message.reply_text("Команда доступна только администратору.")
        return ConversationHandler.END

    name = update.message.text.strip()
    try:
        _, created = await store.admin_create_service(name)
        await update.message.reply_text(
            "Микросервис создан." if created else "Микросервис уже существует.",
            reply_markup=_admin_menu_keyboard(),
        )
    except Exception:
        logger.exception("Failed to create service")
        await update.message.reply_text("Ошибка при добавлении микросервиса.", reply_markup=_admin_menu_keyboard())
    return ADMIN_MENU


async def admin_add_stand_service(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    admin_ids: set[int] = context.application.bot_data["admin_ids"]
    if update.message is None or update.message.text is None:
        return ADMIN_MENU
    if not _is_admin_user(update, admin_ids):
        await update.message.reply_text("Команда доступна только администратору.")
        return ConversationHandler.END
    context.user_data["admin_service_name"] = update.message.text.strip()
    await update.message.reply_text("Введите название стенда:")
    return ADD_STAND_NAME


async def admin_add_stand_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    admin_ids: set[int] = context.application.bot_data["admin_ids"]
    if update.message is None or update.message.text is None:
        return ADMIN_MENU
    if not _is_admin_user(update, admin_ids):
        await update.message.reply_text("Команда доступна только администратору.")
        return ConversationHandler.END
    context.user_data["admin_stand_name"] = update.message.text.strip()
    kb = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Backend", callback_data="admin:stand_type:backend")],
            [InlineKeyboardButton("Frontend", callback_data="admin:stand_type:frontend")],
        ]
    )
    await update.message.reply_text("Выберите тип стенда:", reply_markup=kb)
    return ADD_STAND_TYPE


async def admin_add_stand_type(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    store: SQLiteStore = context.application.bot_data["store"]
    admin_ids: set[int] = context.application.bot_data["admin_ids"]

    if update.callback_query is None:
        return ADMIN_MENU
    await update.callback_query.answer()
    if not _is_admin_user(update, admin_ids):
        await _safe_edit_message(update, "Команда доступна только администратору.", None)
        return ConversationHandler.END

    data = update.callback_query.data or ""
    if not data.startswith("admin:stand_type:"):
        return ADD_STAND_TYPE
    stand_type = data.split(":")[-1]
    service_name = str(context.user_data.get("admin_service_name") or "").strip()
    stand_name = str(context.user_data.get("admin_stand_name") or "").strip()

    try:
        _, created = await store.admin_create_stand(service_name, stand_name, stand_type)
        await _safe_edit_message(
            update,
            "Стенд создан." if created else "Стенд уже существует.",
            _admin_menu_keyboard(),
        )
    except Exception:
        logger.exception("Failed to create stand")
        await _safe_edit_message(update, "Ошибка при добавлении стенда.", _admin_menu_keyboard())

    context.user_data.pop("admin_service_name", None)
    context.user_data.pop("admin_stand_name", None)
    return ADMIN_MENU


async def admin_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        await update.message.reply_text("Отменено.", reply_markup=_admin_menu_keyboard())
    return ADMIN_MENU


# ---- Daily reminders ----------------------------------------------------------


async def daily_reminder_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    store: SQLiteStore = context.application.bot_data["store"]
    occupied = await store.list_active_usages_by_user()
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
    try:
        if isinstance(update, Update) and update.effective_message is not None:
            await update.effective_message.reply_text(
                "Произошла ошибка. Попробуйте ещё раз или откройте /status заново."
            )
    except Exception:
        logger.exception("Failed to notify user about error")


async def _post_init(app: Application) -> None:
    """
    Initialize DB and migrate from JSON if needed.
    """
    store: SQLiteStore = app.bot_data["store"]
    json_path: str = app.bot_data["json_path"]
    await store.init()
    await store.migrate_from_json_if_needed(json_path=json_path)
    await store.ensure_initial_stands()


def main() -> None:
    load_dotenv()

    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN in environment")

    db_path = os.getenv("DB_FILE", "bot.db").strip() or "bot.db"
    json_path = os.getenv("DATA_FILE", "data.json").strip() or "data.json"  # migration source (legacy)

    admin_ids = _parse_admin_ids()
    if not admin_ids:
        logger.warning("No SUPER_ADMIN_ID(S) provided; /admin will be unavailable.")

    store = SQLiteStore(db_path=db_path)

    app = Application.builder().token(token).post_init(_post_init).build()
    app.bot_data["store"] = store
    app.bot_data["admin_ids"] = admin_ids
    app.bot_data["json_path"] = json_path

    # Commands
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("role", cmd_role))
    app.add_handler(CommandHandler("status", cmd_status))

    # /admin conversation (interactive admin panel)
    admin_conv = ConversationHandler(
        entry_points=[CommandHandler("admin", cmd_admin)],
        states={
            ADMIN_MENU: [
                CallbackQueryHandler(admin_menu_callback, pattern=r"^admin:"),
            ],
            ADD_SERVICE_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_service_name),
                CommandHandler("cancel", admin_cancel),
            ],
            ADD_STAND_SERVICE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_stand_service),
                CommandHandler("cancel", admin_cancel),
            ],
            ADD_STAND_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_add_stand_name),
                CommandHandler("cancel", admin_cancel),
            ],
            ADD_STAND_TYPE: [
                CallbackQueryHandler(admin_add_stand_type, pattern=r"^admin:stand_type:"),
                CallbackQueryHandler(admin_menu_callback, pattern=r"^admin:"),  # allow back/close
            ],
        },
        fallbacks=[CommandHandler("cancel", admin_cancel)],
        name="admin-conversation",
        persistent=False,
    )
    app.add_handler(admin_conv)

    # Callbacks
    app.add_handler(
        CallbackQueryHandler(on_role_callback, pattern=r"^role:(first|set):(backend|frontend|qa|observer)$")
    )
    app.add_handler(CallbackQueryHandler(on_stand_callback, pattern=r"^stand:\d+:(take|free)$"))

    # Reply-keyboard safety net
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_menu_text))

    # Errors
    app.add_error_handler(on_error)

    # Daily reminders at 10:00 Moscow time
    tz = ZoneInfo(MOSCOW_TZ) if ZoneInfo is not None else timezone.utc
    app.job_queue.run_daily(daily_reminder_job, time=time(hour=17, minute=16, tzinfo=tz), name="daily-reminder")

    logger.info("Bot started. DB: %s", db_path)
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

