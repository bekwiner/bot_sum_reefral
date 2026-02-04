# database.py - Soddalashtirilgan versiya (AI va Liga tizimisiz)
import asyncio
import aiosqlite
import time
import os
import shutil
from datetime import datetime
from typing import Optional, List, Tuple
from contextlib import asynccontextmanager

DB_NAME = "bot_data.db"

CREATE_SQL = [
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id     INTEGER PRIMARY KEY,
        username    TEXT,
        almaz       INTEGER DEFAULT 0,
        ref_by      INTEGER,
        verified    INTEGER DEFAULT 0,
        phone       TEXT,
        created_at  INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS admins (
        user_id  INTEGER PRIMARY KEY,
        username TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS dynamic_texts (
        key     TEXT PRIMARY KEY,
        content TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS required_channels (
        username TEXT PRIMARY KEY
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS suspensions (
        user_id  INTEGER PRIMARY KEY,
        until_ts INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS referrals (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        inviter_id  INTEGER NOT NULL,
        invited_id  INTEGER NOT NULL UNIQUE,
        status      TEXT DEFAULT 'joined',
        created_at  INTEGER,
        verified_at INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS withdraw_requests (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id      INTEGER NOT NULL,
        amount       INTEGER NOT NULL,
        ff_id        TEXT,
        game         TEXT DEFAULT 'ff',
        status       TEXT DEFAULT 'pending',
        created_at   INTEGER,
        processed_at INTEGER,
        processed_by INTEGER,
        note         TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS withdraw_notifications (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id INTEGER NOT NULL,
        chat_id    INTEGER NOT NULL,
        message_id INTEGER NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS settings (
        key   TEXT PRIMARY KEY,
        value TEXT
    );
    """
    ,
    """
    CREATE TABLE IF NOT EXISTS offers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game TEXT NOT NULL,
        label TEXT NOT NULL,
        achko_cost INTEGER NOT NULL,
        created_at INTEGER
    );
    """
    ,
    """
    CREATE TABLE IF NOT EXISTS purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        amount INTEGER NOT NULL,
        proof_chat_id INTEGER,
        proof_message_id INTEGER,
        status TEXT DEFAULT 'pending',
        created_at INTEGER,
        processed_at INTEGER,
        processed_by INTEGER,
        note TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS admin_actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        admin_id INTEGER NOT NULL,
        action TEXT NOT NULL,
        target_user_id INTEGER,
        amount INTEGER,
        note TEXT,
        created_at INTEGER
    );
    """
]

DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
DB_TIMEOUT = float(os.getenv("DB_TIMEOUT", "30"))
_DB_POOL: asyncio.LifoQueue | None = None
_DB_POOL_LOCK = asyncio.Lock()


async def _create_pooled_connection() -> aiosqlite.Connection:
    conn = await aiosqlite.connect(
        DB_NAME,
        timeout=DB_TIMEOUT,
        check_same_thread=False,
    )
    await conn.execute("PRAGMA journal_mode=WAL;")
    await conn.execute("PRAGMA synchronous=NORMAL;")
    await conn.execute("PRAGMA temp_store=MEMORY;")
    await conn.execute("PRAGMA cache_size=-64000;")
    await conn.commit()
    return conn


async def _ensure_pool() -> asyncio.LifoQueue:
    global _DB_POOL
    if _DB_POOL is not None:
        return _DB_POOL
    async with _DB_POOL_LOCK:
        if _DB_POOL is None:
            queue: asyncio.LifoQueue = asyncio.LifoQueue(maxsize=DB_POOL_SIZE)
            for _ in range(DB_POOL_SIZE):
                conn = await _create_pooled_connection()
                await queue.put(conn)
            _DB_POOL = queue
    return _DB_POOL


@asynccontextmanager
async def db_connection():
    pool = await _ensure_pool()
    conn = await pool.get()
    try:
        yield conn
    except Exception:
        try:
            await conn.rollback()
        except Exception:
            pass
        raise
    finally:
        await pool.put(conn)


async def init_db():
    async with db_connection() as db:
        for sql in CREATE_SQL:
            await db.execute(sql)
        await db.commit()
    await _ensure_column("withdraw_requests", "game", "TEXT DEFAULT 'ff'")


async def _ensure_column(table: str, column: str, ddl: str):
    async with db_connection() as db:
        cur = await db.execute(f"PRAGMA table_info({table})")
        rows = await cur.fetchall()
        cols = [r[1] for r in rows]
        if column not in cols:
            await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
            await db.commit()


# ---------------- Users ----------------
async def add_user(user_id: int, username: Optional[str] = None, ref_by: Optional[int] = None):
    now = int(time.time())
    async with db_connection() as db:
        await db.execute("""
            INSERT INTO users(user_id, username, created_at)
            VALUES(?, ?, ?)
            ON CONFLICT(user_id) DO NOTHING
        """, (user_id, username, now))

        if username:
            await db.execute("UPDATE users SET username=? WHERE user_id=?", (username, user_id))

        if ref_by and ref_by != user_id:
            cur = await db.execute("SELECT ref_by FROM users WHERE user_id=?", (user_id,))
            row = await cur.fetchone()

            if row and row[0] is None:
                await db.execute("UPDATE users SET ref_by=? WHERE user_id=?", (ref_by, user_id))

        await db.commit()


async def get_user(user_id: int):
    async with db_connection() as db:
        cur = await db.execute(
            "SELECT user_id, username, ref_by, almaz, verified, phone FROM users WHERE user_id=?",
            (user_id,)
        )
        return await cur.fetchone()


async def add_almaz(user_id: int, amount: int):
    async with db_connection() as db:
        await db.execute(
            "UPDATE users SET almaz = COALESCE(almaz,0) + ? WHERE user_id=?",
            (amount, user_id)
        )
        await db.commit()


async def adjust_balance(user_id: int, delta: int, min_zero: bool = True) -> bool:
    async with db_connection() as db:
        await db.execute("BEGIN IMMEDIATE")
        cur = await db.execute("SELECT COALESCE(almaz,0) FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row:
            await db.rollback()
            return False
        balance = int(row[0])
        new_balance = balance + delta
        if min_zero and new_balance < 0:
            await db.rollback()
            return False
        await db.execute("UPDATE users SET almaz=? WHERE user_id=?", (new_balance, user_id))
        await db.commit()
        return True


async def get_leaderboard(limit: int = 15) -> List[Tuple[str, int]]:
    async with db_connection() as db:
        cur = await db.execute("""
            SELECT username, COALESCE(almaz,0) FROM users
            ORDER BY COALESCE(almaz,0) DESC, user_id ASC
            LIMIT ?
        """, (limit,))
        rows = await cur.fetchall()
        return [(r[0], r[1]) for r in rows]


async def get_user_rank(user_id: int) -> Tuple[int, int]:
    async with db_connection() as db:
        cur = await db.execute("SELECT COUNT(*) FROM users")
        total_row = await cur.fetchone()
        total = int(total_row[0]) if total_row else 0

        cur = await db.execute(
            "SELECT COUNT(*) FROM users WHERE COALESCE(almaz,0) > (SELECT COALESCE(almaz,0) FROM users WHERE user_id=?)",
            (user_id,)
        )
        higher = await cur.fetchone()
        higher_count = int(higher[0]) if higher else 0

        rank = higher_count + 1 if total > 0 else 0
        return rank, total


async def get_ref_by(user_id: int) -> Optional[int]:
    async with db_connection() as db:
        cur = await db.execute("SELECT ref_by FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return row[0] if row and row[0] is not None else None


async def set_ref_by_if_empty(user_id: int, ref_by: Optional[int]):
    if not ref_by or ref_by == user_id:
        return

    async with db_connection() as db:
        cur = await db.execute("SELECT ref_by FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()

        if row and row[0] is None:
            await db.execute("UPDATE users SET ref_by=? WHERE user_id=?", (ref_by, user_id))
            await db.commit()


async def set_verified(user_id: int):
    async with db_connection() as db:
        await db.execute("UPDATE users SET verified = 1 WHERE user_id=?", (user_id,))
        await db.commit()


async def set_phone_verified(user_id: int, phone: str):
    async with db_connection() as db:
        await db.execute(
            "UPDATE users SET phone = ?, verified = 1 WHERE user_id=?",
            (phone, user_id)
        )
        await db.commit()


async def is_verified(user_id: int) -> bool:
    async with db_connection() as db:
        cur = await db.execute("SELECT verified FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row:
            return False
        value = row[0]
        try:
            return bool(int(value))
        except Exception:
            return False


# ---------------- Admins ----------------
async def list_admins() -> List[Tuple[int, Optional[str]]]:
    async with db_connection() as db:
        cur = await db.execute("SELECT user_id, username FROM admins ORDER BY user_id ASC")
        rows = await cur.fetchall()
        return [(r[0], r[1]) for r in rows]


async def add_admin(user_id: int, username: Optional[str]):
    async with db_connection() as db:
        try:
            await db.execute("INSERT INTO admins(user_id, username) VALUES(?, ?)", (user_id, username))
            await db.commit()
            return True
        except Exception:
            return False


async def remove_admin(user_id: int):
    async with db_connection() as db:
        cur = await db.execute("DELETE FROM admins WHERE user_id=?", (user_id,))
        await db.commit()
        return cur.rowcount > 0


async def is_admin(user_id: int) -> bool:
    async with db_connection() as db:
        cur = await db.execute("SELECT 1 FROM admins WHERE user_id=?", (user_id,))
        return await cur.fetchone() is not None


# ---------------- Dynamic texts ----------------
async def get_dynamic_text(key: str) -> str:
    async with db_connection() as db:
        cur = await db.execute("SELECT content FROM dynamic_texts WHERE key=?", (key,))
        row = await cur.fetchone()
        return row[0] if row else ""


async def update_dynamic_text(key: str, content: str):
    async with db_connection() as db:
        await db.execute("""
            INSERT INTO dynamic_texts(key, content)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET content=excluded.content
        """, (key, content))
        await db.commit()


# ---------------- Required channels ----------------
async def list_required_channels() -> list[str]:
    async with db_connection() as db:
        cur = await db.execute("SELECT username FROM required_channels ORDER BY username ASC")
        rows = await cur.fetchall()
        return [r[0] for r in rows]


async def add_required_channel(username: str) -> bool:
    username = username.strip()
    if not username.startswith("@"):
        username = "@" + username
    async with db_connection() as db:
        try:
            await db.execute("INSERT INTO required_channels(username) VALUES(?)", (username,))
            await db.commit()
            return True
        except Exception:
            return False


async def remove_required_channel(username: str) -> bool:
    username = username.strip()
    if not username.startswith("@"):
        username = "@" + username
    async with db_connection() as db:
        cur = await db.execute("DELETE FROM required_channels WHERE username=?", (username,))
        await db.commit()
        return cur.rowcount > 0


async def required_channels_count() -> int:
    async with db_connection() as db:
        cur = await db.execute("SELECT COUNT(*) FROM required_channels")
        row = await cur.fetchone()
        return int(row[0]) if row else 0


# ---------------- Suspensions ----------------
async def set_suspension(user_id: int, seconds: int):
    until_ts = int(time.time()) + max(0, int(seconds))
    async with db_connection() as db:
        await db.execute("""
            INSERT INTO suspensions(user_id, until_ts)
            VALUES(?, ?)
            ON CONFLICT(user_id) DO UPDATE SET until_ts=excluded.until_ts
        """, (user_id, until_ts))
        await db.commit()


async def get_suspension_remaining(user_id: int) -> int:
    now = int(time.time())
    async with db_connection() as db:
        cur = await db.execute("SELECT until_ts FROM suspensions WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row:
            return 0
        remain = row[0] - now
        if remain <= 0:
            await db.execute("DELETE FROM suspensions WHERE user_id=?", (user_id,))
            await db.commit()
            return 0
        return remain


# ---------------- Referrals ----------------
async def create_referral(inviter_id: int, invited_id: int):
    now = int(time.time())
    async with db_connection() as db:
        try:
            cur = await db.execute(
                "INSERT INTO referrals(inviter_id, invited_id, status, created_at) VALUES(?, ?, 'joined', ?)",
                (inviter_id, invited_id, now)
            )
            await db.commit()
            return True
        except Exception:
            # already exists or other error -> return False
            return False


async def mark_referral_verified(invited_id: int):
    now = int(time.time())
    async with db_connection() as db:
        cur = await db.execute("SELECT status FROM referrals WHERE invited_id=?", (invited_id,))
        row = await cur.fetchone()
        # If already verified — nothing to do
        if row and row[0] == "verified":
            return False

        # If referral record exists — mark verified and return True
        if row:
            await db.execute(
                "UPDATE referrals SET status='verified', verified_at=? WHERE invited_id=?",
                (now, invited_id)
            )
            await db.commit()
            return True

        # No referral record — nothing to mark
        return False


async def count_verified_referrals(inviter_id: int) -> int:
    async with db_connection() as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM referrals WHERE inviter_id=? AND status='verified'",
            (inviter_id,)
        )
        row = await cur.fetchone()
        return int(row[0]) if row else 0


async def count_all_referrals(inviter_id: int) -> int:
    async with db_connection() as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM referrals WHERE inviter_id=?",
            (inviter_id,)
        )
        row = await cur.fetchone()
        return int(row[0]) if row else 0


async def get_top_referrers_today(limit: int = 10) -> List[Tuple[int, Optional[str], int]]:
    now = time.time()
    lt = time.localtime(now)
    start_of_day = int(time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, lt.tm_wday, lt.tm_yday, lt.tm_isdst)))

    async with db_connection() as db:
        cur = await db.execute(
            """
            SELECT r.inviter_id, u.username, COUNT(*) AS cnt
            FROM referrals r
            JOIN users u ON u.user_id = r.inviter_id
            WHERE r.status='verified' AND r.verified_at >= ?
            GROUP BY r.inviter_id, u.username
            ORDER BY cnt DESC, r.inviter_id ASC
            LIMIT ?
            """,
            (start_of_day, limit)
        )
        rows = await cur.fetchall()
        return [(r[0], r[1], r[2]) for r in rows]

# ---------------- Offers ----------------
async def create_offer(game: str, label: str, achko_cost: int):
    now = int(time.time())
    async with db_connection() as db:
        cur = await db.execute(
            "INSERT INTO offers(game, label, achko_cost, created_at) VALUES(?, ?, ?, ?)",
            (game, label, achko_cost, now)
        )
        await db.commit()
        return cur.lastrowid


async def list_offers(game: str) -> List[Tuple[int, str, int]]:
    async with db_connection() as db:
        cur = await db.execute(
            "SELECT id, label, achko_cost FROM offers WHERE game=? ORDER BY achko_cost ASC",
            (game,)
        )
        rows = await cur.fetchall()
        return [(r[0], r[1], r[2]) for r in rows]


async def get_offer(offer_id: int):
    async with db_connection() as db:
        cur = await db.execute("SELECT id, game, label, achko_cost FROM offers WHERE id=?", (offer_id,))
        return await cur.fetchone()


async def update_offer(offer_id: int, label: str, achko_cost: int):
    async with db_connection() as db:
        await db.execute("UPDATE offers SET label=?, achko_cost=? WHERE id=?", (label, achko_cost, offer_id))
        await db.commit()


async def delete_offer(offer_id: int):
    async with db_connection() as db:
        cur = await db.execute("DELETE FROM offers WHERE id=?", (offer_id,))
        await db.commit()
        return cur.rowcount > 0


# ---------------- Purchases ----------------
async def create_purchase(user_id: int, amount: int, proof_chat_id: int, proof_message_id: int):
    now = int(time.time())
    async with db_connection() as db:
        cur = await db.execute(
            "INSERT INTO purchases(user_id, amount, proof_chat_id, proof_message_id, status, created_at) VALUES(?, ?, ?, ?, 'pending', ?)",
            (user_id, amount, proof_chat_id, proof_message_id, now)
        )
        await db.commit()
        return cur.lastrowid


async def list_pending_purchases() -> List[Tuple[int, int, int, int]]:
    async with db_connection() as db:
        cur = await db.execute(
            "SELECT id, user_id, amount, created_at FROM purchases WHERE status='pending' ORDER BY created_at ASC"
        )
        return await cur.fetchall()


async def update_purchase_status(purchase_id: int, status: str, processed_by: int | None, note: str | None = None):
    now = int(time.time())
    async with db_connection() as db:
        if note is not None:
            await db.execute(
                "UPDATE purchases SET status=?, processed_at=?, processed_by=?, note=? WHERE id=?",
                (status, now, processed_by, note, purchase_id)
            )
        else:
            await db.execute(
                "UPDATE purchases SET status=?, processed_at=?, processed_by=? WHERE id=?",
                (status, now, processed_by, purchase_id)
            )
        await db.commit()


# ---------------- Atomic withdraw + deduct ----------------
async def create_withdraw_and_deduct(user_id: int, amount: int, ff_id: str, game: str = "ff"):
    now = int(time.time())
    async with db_connection() as db:
        await db.execute("BEGIN IMMEDIATE")
        cur = await db.execute("SELECT COALESCE(almaz,0) FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        if not row:
            await db.rollback()
            return None
        balance = int(row[0])
        if balance < amount:
            await db.rollback()
            return None

        await db.execute("UPDATE users SET almaz = almaz - ? WHERE user_id=?", (amount, user_id))
        cur = await db.execute(
            "INSERT INTO withdraw_requests(user_id, amount, ff_id, game, status, created_at) VALUES(?, ?, ?, ?, 'pending', ?)",
            (user_id, amount, ff_id, game, now)
        )
        await db.commit()
        return cur.lastrowid


# ---------------- Withdraw ----------------
async def create_withdraw_request(user_id: int, amount: int, ff_id: str, game: str = "ff") -> int:
    now = int(time.time())
    async with db_connection() as db:
        cur = await db.execute(
            """
            INSERT INTO withdraw_requests(user_id, amount, ff_id, game, status, created_at)
            VALUES(?, ?, ?, ?, 'pending', ?)
            """,
            (user_id, amount, ff_id, game, now)
        )
        await db.commit()
        return cur.lastrowid


async def get_withdraw_request(request_id: int):
    async with db_connection() as db:
        cur = await db.execute(
            """
            SELECT id, user_id, amount, ff_id, game, status, created_at, processed_at, processed_by, note
            FROM withdraw_requests
            WHERE id=?
            """,
            (request_id,)
        )
        return await cur.fetchone()


async def update_withdraw_status(request_id: int, status: str, processed_by: Optional[int], note: Optional[str]):
    now = int(time.time())
    async with db_connection() as db:
        if note is not None:
            await db.execute(
                """
                UPDATE withdraw_requests
                SET status=?, processed_at=?, processed_by=?, note=?
                WHERE id=?
                """,
                (status, now, processed_by, note, request_id)
            )
        else:
            await db.execute(
                """
                UPDATE withdraw_requests
                SET status=?, processed_at=?, processed_by=?
                WHERE id=?
                """,
                (status, now, processed_by, request_id)
            )
        await db.commit()


async def get_withdraw_stats() -> Tuple[int, int, int, int, int]:
    async with db_connection() as db:
        cur = await db.execute(
            "SELECT status, COUNT(*) FROM withdraw_requests GROUP BY status"
        )
        rows = await cur.fetchall()
    counts = {"pending": 0, "approved": 0, "edited": 0, "rejected": 0}
    total = 0
    for status, cnt in rows:
        total += cnt
        if status in counts:
            counts[status] = cnt
    return total, counts["pending"], counts["approved"], counts["edited"], counts["rejected"]


async def add_withdraw_notification(request_id: int, chat_id: int, message_id: int):
    async with db_connection() as db:
        await db.execute(
            "INSERT INTO withdraw_notifications(request_id, chat_id, message_id) VALUES(?, ?, ?)",
            (request_id, chat_id, message_id)
        )
        await db.commit()


async def get_withdraw_notifications(request_id: int) -> List[Tuple[int, int]]:
    async with db_connection() as db:
        cur = await db.execute(
            "SELECT chat_id, message_id FROM withdraw_notifications WHERE request_id=?",
            (request_id,)
        )
        rows = await cur.fetchall()
        return [(r[0], r[1]) for r in rows]


# ---------------- Admin actions ----------------
async def log_admin_action(
    admin_id: int,
    action: str,
    target_user_id: Optional[int] = None,
    amount: Optional[int] = None,
    note: Optional[str] = None
):
    now = int(time.time())
    async with db_connection() as db:
        await db.execute(
            """
            INSERT INTO admin_actions(admin_id, action, target_user_id, amount, note, created_at)
            VALUES(?, ?, ?, ?, ?, ?)
            """,
            (admin_id, action, target_user_id, amount, note, now)
        )
        await db.commit()


# ---------------- Settings ----------------
async def get_setting(key: str) -> str | None:
    async with db_connection() as db:
        cur = await db.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = await cur.fetchone()
        return row[0] if row else None


async def set_setting(key: str, value: str):
    async with db_connection() as db:
        await db.execute("""
            INSERT INTO settings(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """, (key, value))
        await db.commit()


async def delete_setting(key: str):
    async with db_connection() as db:
        await db.execute("DELETE FROM settings WHERE key=?", (key,))
        await db.commit()


# ---------------- Backup ----------------
async def backup_database() -> str:
    backups_dir = "backups"
    if not os.path.exists(backups_dir):
        os.makedirs(backups_dir)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(backups_dir, f"backup_{timestamp}.db")

    try:
        shutil.copy(DB_NAME, backup_path)
        return backup_path
    except Exception as e:
        return f"Backup xatosi: {e}"
