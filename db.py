# db.py
import aiosqlite
from config import DB_PATH

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            tg_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            status TEXT DEFAULT 'active',
            joined_at TEXT DEFAULT (datetime('now'))
        )""")
        await db.execute("""
        CREATE TABLE IF NOT EXISTS broadcasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            sent_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0
        )""")
        await db.commit()

async def add_user(tg_id: int, username: str = None, full_name: str = None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO users (tg_id, username, full_name, status, joined_at) VALUES (?,?,?,?,datetime('now'))",
            (tg_id, username, full_name, 'active')
        )
        await db.commit()

async def remove_user(tg_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET status='removed' WHERE tg_id=?", (tg_id,))
        await db.commit()

async def get_active_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT tg_id FROM users WHERE status='active'")
        rows = await cur.fetchall()
        return [r[0] for r in rows]

async def count_active_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM users WHERE status='active'")
        row = await cur.fetchone()
        return row[0] if row else 0

async def record_broadcast(content: str, sent: int, failed: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO broadcasts (content, sent_count, failed_count) VALUES (?,?,?)",
            (content, sent, failed)
        )
        await db.commit()
