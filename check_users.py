import asyncio
import aiosqlite
from config import DB_PATH

async def show_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT tg_id, username, full_name, status, joined_at FROM users")
        rows = await cur.fetchall()
        if not rows:
            print("У базі ще немає користувачів 😕")
        else:
            print("📋 Список користувачів у базі:")
            for r in rows:
                print(r)

asyncio.run(show_users())
