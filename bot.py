# bot.py
import os
import asyncio
import sqlite3
import asyncpg # ❗ Драйвер для Neon/PostgreSQL
import csv
import io
import logging 
import html    
import re
from typing import Optional

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    Message, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove,
    InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage 

# ❗ НОВІ ІМПОРТИ ДЛЯ ВЕБХУКА
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from aiohttp import web

from aiohttp.client_exceptions import ClientConnectorError
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter

from dotenv import load_dotenv

# --- Налаштування Логування ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Налаштування Бота ---
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
ARCHIVE_CHANNEL_ID = os.getenv("ARCHIVE_CHANNEL_ID") 
DATABASE_URL = os.getenv("DATABASE_URL") 
ADMINS = [
    7996371062,      # Я
    798102209,      # Галя
    55858261251,       # Настя
    743627341,       # Оля
]

ADMIN_TITLES = {
    7996371062: "бізнес-тренерки Олександри",
    798102209: "тренерки з продукту Галини",
    55858261251: "тренерки з продукту Анастасії",
    743627341: "тренерки з продукту Ольги",
}

# ❗ НАЛАШТУВАННЯ ВЕБХУКА (ДЛЯ RENDER)
BASE_WEBHOOK_URL = os.getenv("RENDER_EXTERNAL_URL")
WEB_SERVER_HOST = "0.0.0.0"
WEB_SERVER_PORT = int(os.getenv("PORT", 8080)) # Render надає порт у змінній PORT

WEBHOOK_PATH = f"/webhook/{BOT_TOKEN}"
WEBHOOK_URL = f"{BASE_WEBHOOK_URL}{WEBHOOK_PATH}"

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=storage) 

pool: asyncpg.Pool = None

class BroadcastStates(StatesGroup):
    waiting_for_content = State()
    waiting_for_folder = State()

# --- ❗❗❗ НОВІ ФУНКЦІЇ РОБОТИ З БАЗОЮ (asyncpg) ❗❗❗ ---

async def init_db():
    """Ініціалізує базу даних PostgreSQL."""
    global pool
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                phone_number TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS folders (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS posts (
                id SERIAL PRIMARY KEY,
                folder_id INTEGER NOT NULL,
                post_title TEXT NOT NULL,       
                message_id BIGINT NOT NULL,    
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (folder_id) REFERENCES folders(id) ON DELETE CASCADE
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS support_tickets (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                user_name TEXT,
                message_text TEXT,
                status TEXT DEFAULT 'open',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP,
                closed_by_admin_id BIGINT
            )
        """)
    logging.info("База даних PostgreSQL ініціалізована.")

async def populate_folders_if_empty():
    """Заповнює папки за замовчуванням, якщо вони порожні."""
    global pool
    folders_to_add = [
        "📘 Корисності",
        "🎓 Іспит Школи Новачка",
        "🎥 Відеоогляди",
        "🎧 Подкасти з психологами"
    ]
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM folders")
        if count == 0:
            logging.info("База 'folders' порожня. Заповнюємо...")
            try:
                await conn.executemany("INSERT INTO folders (name) VALUES ($1)",
                                       [(name,) for name in folders_to_add])
                logging.info("Папки за замовчуванням додано.")
            except asyncpg.exceptions.UniqueViolationError:
                logging.warning("Помилка: Папка вже існує (це дивно, але ігноруємо).")
        
async def log_support_ticket(user_id: int, user_name: str, text: str):
    global pool
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO support_tickets (user_id, user_name, message_text) VALUES ($1, $2, $3)",
                user_id, user_name, text
            )
        logging.info(f"Створено новий тікет (ID: {user_id}) зі статусом 'open'.")
    except Exception as e:
        logging.error(f"Помилка створення тікету: {e}")

async def close_support_ticket(user_id: int, admin_id: int):
    global pool
    try:
        async with pool.acquire() as conn:
            ticket_id = await conn.fetchval(
                "SELECT id FROM support_tickets WHERE user_id = $1 AND status = 'open' ORDER BY created_at DESC LIMIT 1",
                user_id
            )
            if ticket_id:
                await conn.execute(
                    "UPDATE support_tickets SET status = 'closed', closed_at = CURRENT_TIMESTAMP, closed_by_admin_id = $1 WHERE id = $2",
                    admin_id, ticket_id
                )
                logging.info(f"Тікет {ticket_id} (від User ID: {user_id}) закрито адміном {admin_id}.")
            else:
                logging.warning(f"Адмін {admin_id} відповів {user_id}, але відкритих тікетів для нього не знайдено.")
    except Exception as e:
        logging.error(f"Помилка закриття тікету: {e}")

async def get_open_tickets() -> list:
    global pool
    async with pool.acquire() as conn:
        tickets = await conn.fetch(
            "SELECT user_id, user_name, message_text, created_at FROM support_tickets WHERE status = 'open' ORDER BY created_at ASC"
        )
    return tickets 

async def add_new_folder(name: str) -> bool:
    global pool
    async with pool.acquire() as conn:
        try:
            await conn.execute("INSERT INTO folders (name) VALUES ($1)", name)
            return True
        except asyncpg.exceptions.UniqueViolationError:
            return False

async def delete_folder_by_name(name: str) -> bool:
    global pool
    async with pool.acquire() as conn:
        try:
            folder_id = await conn.fetchval("SELECT id FROM folders WHERE name = $1", name)
            if not folder_id:
                return False 
            
            await conn.execute("DELETE FROM posts WHERE folder_id = $1", folder_id)
            await conn.execute("DELETE FROM folders WHERE id = $1", folder_id)
            logging.info(f"Папку ID {folder_id} ({name}) та її пости видалено.")
            return True
        except Exception as e:
            logging.error(f"Помилка при видаленні папки: {e}")
            return False

async def delete_post_by_id(post_id: int) -> (bool, Optional[int]):
    global pool
    async with pool.acquire() as conn:
        try:
            result = await conn.fetchrow("DELETE FROM posts WHERE id = $1 RETURNING folder_id", post_id)
            if result:
                logging.info(f"Пост ID {post_id} видалено з бази.")
                return True, result['folder_id']
            else:
                return False, None
        except Exception as e:
            logging.error(f"Помилка при видаленні поста: {e}")
            return False, None

async def delete_post_by_title(title: str) -> bool:
    global pool
    async with pool.acquire() as conn:
        try:
            result = await conn.execute("DELETE FROM posts WHERE post_title = $1", title)
            if 'DELETE 1' in result:
                logging.info(f"Пост '{title}' видалено з бази.")
                return True
            else:
                return False
        except Exception as e:
            logging.error(f"Помилка при видаленні поста: {e}")
            return False

async def add_user(user_id: int, username: str, full_name: str, phone_number: str = None):
    global pool
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (user_id, username, full_name, phone_number) 
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (user_id) DO UPDATE SET
                username = EXCLUDED.username,
                full_name = EXCLUDED.full_name,
                phone_number = COALESCE($4, users.phone_number)
            """,
            user_id, username, full_name, phone_number
        )

async def get_active_users():
    global pool
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
    return [row['user_id'] for row in rows]

async def delete_user(user_id: int):
    global pool
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM users WHERE user_id = $1", user_id)

async def delete_user_by_phone(phone_query: str) -> bool:
    global pool
    async with pool.acquire() as conn:
        digits_only = re.sub(r'\D', '', phone_query)
        search_suffix = digits_only
        if len(digits_only) > 9:
            search_suffix = digits_only[-9:]
        
        user_id = await conn.fetchval("SELECT user_id FROM users WHERE phone_number LIKE $1", '%' + search_suffix)
        
        if user_id:
            await conn.execute("DELETE FROM users WHERE user_id = $1", user_id)
            return True
        else:
            return False

async def delete_users_by_list(identifiers: list) -> int:
    global pool
    if not identifiers:
        return 0
    
    async with pool.acquire() as conn:
        try:
            result = await conn.fetch(
                """
                DELETE FROM users
                WHERE CAST(user_id AS TEXT) = ANY($1::TEXT[]) OR phone_number = ANY($1::TEXT[])
                RETURNING user_id
                """,
                identifiers
            )
            deleted_count = len(result)
            logging.info(f"Видалено {deleted_count} користувачів за списком.")
            return deleted_count
        except Exception as e:
            logging.error(f"Помилка при масовому видаленні: {e}")
            return 0

async def get_user_id_by_phone_strict(phone_query: str) -> Optional[int]:
    global pool
    async with pool.acquire() as conn:
        digits_only = re.sub(r'\D', '', phone_query)
        search_suffix = digits_only
        if len(digits_only) > 9:
            search_suffix = digits_only[-9:]
        
        user_to_find = await conn.fetchval("SELECT user_id FROM users WHERE phone_number LIKE $1", '%' + search_suffix)
        
        if user_to_find:
            return user_to_find 
        else:
            return None 

async def get_users_by_query(query: str):
    global pool
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id FROM users WHERE full_name LIKE $1 OR username LIKE $1 OR phone_number LIKE $1",
            '%' + query + '%'
        )
    return [row['user_id'] for row in rows]

async def get_users_by_list(identifiers: list) -> dict:
    global pool
    if not identifiers:
        return {}
    async with pool.acquire() as conn:
        results = await conn.fetch(
            """
            SELECT user_id, phone_number, full_name
            FROM users
            WHERE CAST(user_id AS TEXT) = ANY($1::TEXT[]) OR phone_number = ANY($1::TEXT[])
            """,
            identifiers
        )
    found_users = {}
    for row in results:
        found_users[row['user_id']] = {'phone': row['phone_number'], 'name': row['full_name']}
    return found_users

async def save_post(folder_id: int, post_title: str, message_id: int):
    global pool
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO posts (folder_id, post_title, message_id) VALUES ($1, $2, $3)",
            folder_id, post_title, message_id
        )
    logging.info(f"Пост (MsgID: {message_id}) збережено у папку ID {folder_id}.")

async def get_all_posts_by_folder(folder_id: int):
    global pool
    async with pool.acquire() as conn:
        posts = await conn.fetch(
            "SELECT id, post_title, message_id FROM posts WHERE folder_id = $1 ORDER BY created_at DESC",
            folder_id
        )
    return posts 

async def get_folders() -> list:
    global pool
    async with pool.acquire() as conn:
        folders_records = await conn.fetch("SELECT id, name FROM folders")
    return [(row['id'], row['name']) for row in folders_records]

def escape_markdown(text):
    if text is None:
        return ''
    return text.replace('_', '\\_').replace('*', '\\*').replace('`', '\\`').replace('[', '\\[')

def escape_html(text: str) -> str:
    if text is None:
        return ''
    return html.escape(str(text))

# --- Клавіатури ---

def get_main_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Надіслати свій номер телефону", request_contact=True)],
            [KeyboardButton(text="📂 Меню")] 
        ],
        resize_keyboard=True,
        one_time_keyboard=True 
    )
    return keyboard

def get_menu_only_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📂 Меню")]
        ],
        resize_keyboard=True,
        one_time_keyboard=False 
    )
    return keyboard

def get_admin_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📂 Меню")], 
            [KeyboardButton(text="👑 Адмін-панель")] 
        ],
        resize_keyboard=True,
        one_time_keyboard=False 
    )
    return keyboard

async def generate_folder_keyboard(for_admin: bool = False, is_admin_menu: bool = False) -> InlineKeyboardMarkup:
    folders = await get_folders()
    buttons = []
    
    if for_admin:
        prefix = 'save_to_folder_'
    elif is_admin_menu:
        prefix = 'admin_folder_' 
    else:
        prefix = 'folder_'

    for folder_id, name in folders:
        buttons.append([InlineKeyboardButton(text=name, callback_data=f"{prefix}{folder_id}")])
    
    if for_admin:
        buttons.append([InlineKeyboardButton(text="❌ Не зберігати (Тільки розсилка)", callback_data="save_to_folder_0")])
        
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def generate_posts_list_keyboard(posts: list, is_admin: bool = False) -> InlineKeyboardMarkup:
    buttons = []
    for (post_id, title, msg_id) in posts:
        row = [
            InlineKeyboardButton(text=title, callback_data=f"view_post_{msg_id}")
        ]
        if is_admin:
            row.append(InlineKeyboardButton(text="❌ Видалити", callback_data=f"del_post_{post_id}"))
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton(text="⬅️ До Головного меню", callback_data="back_to_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def extract_user_id_from_reply(msg: types.Message) -> Optional[int]:
    if not msg:
        return None
    if getattr(msg, "forward_from", None):
        try:
            fid = getattr(msg.forward_from, "id", None)
            if fid:
                return int(fid)
        except Exception:
            pass
    text_candidates = []
    if getattr(msg, "caption", None):
        text_candidates.append(msg.caption)
    if getattr(msg, "text", None):
        text_candidates.append(msg.text)
    
    marker_code_re = re.compile(r"🔑\s*ID\s*:\s*(\d{4,})", re.IGNORECASE)
    
    for txt in text_candidates:
        if not txt:
            continue
        m = marker_code_re.search(txt)
        if m:
            try:
                logging.info(f"Знайдено ID {m.group(1)} у підписі (чистий текст).")
                return int(m.group(1))
            except Exception:
                pass
                
    logging.warning("Не вдалося знайти ID у підписі через RegEx.")
    return None

# --- ЛОГІКА РОЗСИЛКИ (ВИДІЛЕНА ФУНКЦІЯ) ---
async def process_broadcast_message(content_chat_id: int, content_message_id: int, message: Message, broadcast_filter: str = None):
    
    if broadcast_filter:
        users = await get_users_by_query(broadcast_filter)
        filter_info = f"за фільтром '{broadcast_filter}' (знайдено {len(users)})"
    else:
        users = await get_active_users()
        filter_info = f"усім активним користувачам ({len(users)})"

    await message.answer(f"Починаю розсилку {filter_info}. Будь ласка, зачекайте.")

    sent = 0
    failed = 0
    logging.info(f"DEBUG: Починаю розсилку (copy_message) для {len(users)} користувачів.")

    for uid in users:
        try:
            await asyncio.sleep(0.15)
            await bot.copy_message(
                chat_id=uid,
                from_chat_id=content_chat_id,
                message_id=content_message_id
            )
            sent += 1
        except (TelegramForbiddenError, TelegramBadRequest, TelegramRetryAfter, ClientConnectorError) as e:
            failed += 1
            if isinstance(e, TelegramForbiddenError):
                logging.warning(f"INFO: Користувач {uid} заблокував бота.")
            else:
                logging.error(f"DEBUG: Помилка при відправці користувачу {uid}: {type(e).__name__} - {e}")
        except Exception as e:
            failed += 1
            logging.error(f"DEBUG: Невідома помилка при відправці користувачу {uid}: {type(e).__name__} - {e}")

    logging.info(f"DEBUG: Фінальні результати: Успіх={sent}, Помилки={failed}")
    final_result = f"Розсилка завершена.\nУспіх: {sent}, помилки: {failed}"
    if failed > 0:
        final_result += "\nЗверніть увагу: усі користувачі, які заблокували бота, збережені в базі даних."
    await message.answer(final_result)


# --- ХЕНДЛЕРИ КОМАНД (Розташовані першими) ---

@dp.message(Command("start"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Unknown"
    full_name = message.from_user.full_name or "Невідоме ім'я"
    
    global pool
    async with pool.acquire() as conn:
        phone = await conn.fetchval("SELECT phone_number FROM users WHERE user_id = $1", user_id)

    await add_user(user_id, username, full_name, phone) 
    
    if user_id in ADMINS:
        keyboard = get_admin_keyboard()
        greeting = f"Привіт, Адміністраторе {message.from_user.first_name or ''}! 👋"
        
        if not phone:
             greeting += "\n\n(Адмін, не забудь також надіслати свій контакт для тестування та збереження в БД)"
             keyboard = ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="Надіслати свій номер телефону", request_contact=True)],
                    [KeyboardButton(text="📂 Меню")],
                    [KeyboardButton(text="👑 Адмін-панель")] 
                ],
                resize_keyboard=True,
                one_time_keyboard=True 
            )
    elif phone:
        keyboard = get_menu_only_keyboard()
        greeting = f"""🌿 Привіт!
Раді вітати тебе у навчальному боті EVA ХРК 💚

Тут ти знайдеш:
📚 корисні матеріали для розвитку,
🗓 актуальні навчальні події,
🧠 опитування для вдосконалення,
і найголовніше — підтримку на твоєму шляху в EVA 🌸

Натисню меню нижче, щоб розпочати 👇"""
    else:
        keyboard = get_main_keyboard()
        greeting = (
            f"Привіт, {message.from_user.first_name or 'друже'}! 🎉 Ви приєднались до бота.\n"
            "Будь ласка, **натисніть кнопку нижче**, щоб поділитися номером телефону для повної реєстрації."
        )
    
    await message.answer(greeting, reply_markup=keyboard, parse_mode='Markdown' if user_id not in ADMINS and not phone else None)

@dp.message(Command("menu"))
async def cmd_menu(message: Message):
    is_admin = message.from_user.id in ADMINS
    
    await message.answer(
        "📂 **Головне меню**\n\nОберіть розділ, який вас цікавить:",
        reply_markup=await generate_folder_keyboard(for_admin=False, is_admin_menu=is_admin),
        parse_mode='Markdown'
    )

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message, state: FSMContext):
    if message.from_user.id not in ADMINS:
        await message.reply("У вас немає доступу до цієї команди.")
        return
    
    if not ARCHIVE_CHANNEL_ID:
        await message.answer("❌ **Помилка:** Адміністратор не налаштував `ARCHIVE_CHANNEL_ID` у файлі .env. Розсилка неможлива.")
        return
        
    await state.set_state(BroadcastStates.waiting_for_content)
    await message.answer(
        "Будь ласка, надішліть **будь-який контент** для розсилки (текст, фото, опитування тощо).\n\n"
        "Текст або підпис до медіа буде використано як **заголовок** для цього поста в 'Меню'.\n\n"
        "Або /cancel для відміни."
    )

@dp.message(lambda message: message.text and message.text.lower().strip() == '/check_db')
async def cmd_check_db(message: Message):
    if message.from_user.id not in ADMINS:
        await message.reply("У вас немає прав адміністратора для цієї команди.")
        return
    
    global pool
    async with pool.acquire() as conn:
        results = await conn.fetch("SELECT user_id, username, full_name, phone_number FROM users")
    
    total_count = len(results)
    if total_count == 0:
        await message.reply("База даних порожня. Записів не знайдено.")
        return
        
    response = f"**Звіт по базі даних `users`:**\n\nУСЬОГО ЗАПИСІВ: **{total_count}**\n============================\n"
    for i, row in enumerate(results):
        safe_full_name = escape_markdown(row['full_name'])
        safe_username = escape_markdown(row['username'] or 'НЕМАЄ')
        phone_display = row['phone_number'] or 'НЕМАЄ'
        
        if len(response) + 300 > 4096 and i < total_count - 1: 
            response += f"... (та ще {total_count - i} записів)"
            break
        response += (
            f"🔑 ID: `{row['user_id']}`\n👤 Ім'я: **{safe_full_name}**\n📞 Телефон: `{phone_display}`\n🆔 Username: @{safe_username}\n----------------------------\n"
        )
    await message.reply(response, parse_mode='Markdown')

@dp.message(Command("check_tickets"))
async def cmd_check_tickets(message: Message):
    if message.from_user.id not in ADMINS:
        await message.reply("У вас немає прав адміністратора для цієї команди.")
        return

    tickets = await get_open_tickets()
    
    if not tickets:
        await message.answer("✅ Чудова робота! Усі тікети закриті. Повідомлень без відповіді немає.")
        return
        
    response = f"📢 **ВІДКРИТІ ТІКЕТИ ({len(tickets)}):**\n\n"
    for ticket in tickets:
        safe_name = escape_html(ticket['user_name'])
        safe_text = escape_html(ticket['message_text'][:100] + '...') 
        
        response += (
            f"👤 <b>{safe_name}</b> (ID: <code>{ticket['user_id']}</code>)\n"
            f"<i>{ticket['created_at'].strftime('%Y-%m-%d %H:%M')}</i>\n"
            f"💬 {safe_text}\n"
            "--------------------\n"
        )
    
    await message.answer(response, parse_mode='HTML')

@dp.message(Command("delete_user"))
async def cmd_delete_user(message: Message):
    if message.from_user.id not in ADMINS:
        await message.reply("У вас немає прав адміністратора для цієї команди.")
        return
        
    parts = message.text.split(maxsplit=1)
    
    if len(parts) < 2:
        await message.reply("Будь ласка, вкажіть ID **або** номер телефону користувача для видалення.\nПриклад: /delete_user 123456789\nПриклад: /delete_user +38066...")
        return

    identifier = parts[1].strip()
    target_user_id = None
    
    global pool
    async with pool.acquire() as conn:
        if identifier.isdigit():
            target_user_id = await conn.fetchval("SELECT user_id FROM users WHERE user_id = $1", int(identifier))
            if target_user_id:
                logging.info(f"Знайдено користувача за ID: {target_user_id}")
        
        if not target_user_id:
            logging.info(f"Не знайдено за ID, шукаємо за телефоном: {identifier}")
            target_user_id = await get_user_id_by_phone_strict(identifier) 
            if target_user_id:
                logging.info(f"Знайдено користувача за телефоном: {target_user_id}")
            
    if target_user_id:
        try:
            await delete_user(target_user_id) 
            await message.reply(f"✅ Користувача (ID: {target_user_id}, Запит: {identifier}) успішно видалено з бази даних.")
        except Exception as e:
            await message.reply(f"Помилка під час видалення: {e}")
    else:
        await message.reply(f"❌ Помилка: Користувача з ID або номером телефону '{identifier}' не знайдено.")

@dp.message(Command("add_folder"))
async def cmd_add_folder(message: Message):
    if message.from_user.id not in ADMINS:
        await message.reply("У вас немає прав адміністратора для цієї команди.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("Будь ласка, вкажіть назву папки. Приклад: /add_folder 💡 Лайфхаки")
        return
    folder_name = parts[1].strip()
    if await add_new_folder(folder_name):
        await message.reply(f"✅ Папку '{folder_name}' успішно створено!")
    else:
        await message.reply(f"❌ Помилка: Папка з назвою '{folder_name}' вже існує.")

@dp.message(Command("delete_folder"))
async def cmd_delete_folder(message: Message):
    if message.from_user.id not in ADMINS:
        await message.reply("У вас немає прав адміністратора для цієї команди.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply('Будь ласка, вкажіть точну назву папки для видалення. \nПриклад: /delete_folder "🎥 Відеоогляди"')
        return
    folder_name = parts[1].strip().strip('"') 
    if await delete_folder_by_name(folder_name):
        await message.reply(f"✅ Папку '{folder_name}' та всі її пости успішно видалено.")
    else:
        await message.reply(f"❌ Помилка: Папку з назвою '{folder_name}' не знайдено.")

@dp.message(Command("delete_post"))
async def cmd_delete_post(message: Message):
    if message.from_user.id not in ADMINS:
        await message.reply("У вас немає прав адміністратора для цієї команди.")
        return
    
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply('Будь ласка, вкажіть точну назву поста (в лапках), який хочете видалити.\n'
                          'Приклад: /delete_post "Опитування: Яке ваше питання?"')
        return
    
    post_title = parts[1].strip().strip('"')
    
    if await delete_post_by_title(post_title):
        await message.reply(f"✅ Пост '{post_title}' успішно видалено з 'Меню'.")
    else:
        await message.reply(f"❌ Помилка: Пост з назвою '{post_title}' не знайдено.")

@dp.message(Command("find_user"))
async def cmd_find_user(message: Message):
    if message.from_user.id not in ADMINS:
        await message.reply("У вас немає прав адміністратора для цієї команди.")
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("Вкажіть частину імені, username або номер телефону для пошуку. Приклад: /find_user 38067")
        return
    query = parts[1].strip()
    
    global pool
    async with pool.acquire() as conn:
        results = await conn.fetch(
            "SELECT user_id, username, full_name, phone_number FROM users WHERE full_name LIKE $1 OR username LIKE $1 OR phone_number LIKE $1",
            '%' + query + '%'
        )
        
    if not results:
        await message.reply(f"Користувачів, які містять '{query}', не знайдено.")
        return
    response = f"**Знайдено {len(results)} користувачів за запитом '{query}':**\n\n"
    for i, row in enumerate(results):
        safe_full_name = escape_markdown(row['full_name'])
        safe_username = escape_markdown(row['username'] or 'no_username')
        phone_display = row['phone_number'] or 'немає'
        user_info = f"👤 **{safe_full_name}** ({safe_username})\n"
        user_info += f"📞 Телефон: `{phone_display}`\n" 
        user_info += f"🔑 ID: `{row['user_id']}`\n"
        if len(response) + len(user_info) > 4000 and i < len(results) - 1:
             response += f"... (та ще {len(results) - i} записів)"
             break
        response += user_info + "--------------------------\n"
    await message.reply(response, parse_mode='Markdown')

@dp.message(Command("export_csv"))
async def cmd_export_csv(message: Message):
    if message.from_user.id not in ADMINS:
        await message.reply("У вас немає прав адміністратора для цієї команди.")
        return
    await message.answer("Починаю експорт даних...")
    
    global pool
    async with pool.acquire() as conn:
        results = await conn.fetch("SELECT user_id, username, full_name, phone_number FROM users")
        
    if not results:
        await message.answer("База даних порожня. Немає чого експортувати.")
        return
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    headers = ['ID', 'Username', "Full Name", 'Phone Number']
    csv_writer.writerow(headers)
    
    for row in results:
        csv_writer.writerow([row['user_id'], row['username'], row['full_name'], row['phone_number']])
        
    csv_buffer.seek(0)
    try:
        await message.reply_document(
            document=types.BufferedInputFile(
                file=csv_buffer.getvalue().encode('utf-8'),
                filename='users_export.csv'
            ),
            caption=f"✅ Експортовано {len(results)} записів."
        )
    except Exception as e:
        await message.answer(f"❌ Помилка при відправці файлу: {e}")

@dp.message(Command("send_to_user"))
async def cmd_send_to_user(message: Message):
    if message.from_user.id not in ADMINS:
        await message.reply("У вас немає прав адміністратора для цієї команди.")
        return
    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.reply("Будь ласка, вкажіть ID **або** номер телефону користувача та текст повідомлення.\nПриклад: /send_to_user 987654321 Привіт!\nПриклад: /send_to_user +38066... Привіт!")
        return

    identifier = parts[1]
    text_to_send = parts[2].strip()
    target_user_id = None
    
    global pool
    async with pool.acquire() as conn:
        if identifier.isdigit():
            target_user_id = await conn.fetchval("SELECT user_id FROM users WHERE user_id = $1", int(identifier))
            if target_user_id:
                logging.info(f"Знайдено користувача за ID: {target_user_id}")
        
        if not target_user_id:
            logging.info(f"Не знайдено за ID, шукаємо за телефоном: {identifier}")
            target_user_id = await get_user_id_by_phone_strict(identifier) 
            if target_user_id:
                logging.info(f"Знайдено користувача за телефоном: {target_user_id}")
        
    if not target_user_id:
        await message.reply(f"❌ Помилка: Користувача з ID або номером телефону '{identifier}' не знайдено в базі даних.")
        return

    try:
        await bot.send_message(chat_id=target_user_id, text=text_to_send)
        await message.reply(f"Повідомлення **успішно** надіслано користувачу з ID: `{target_user_id}` (знайдено за '{identifier}')", parse_mode='Markdown')
    except TelegramForbiddenError:
        await message.reply(f"Помилка: Користувач з ID `{target_user_id}` **заблокував бота**. Він не був видалений з бази даних.", parse_mode='Markdown')
    except Exception as e:
        await message.reply(f"Помилка при відправці користувачу `{target_user_id}`: {e}")

@dp.message(Command("send_segment"))
async def cmd_send_segment(message: Message):
    if message.from_user.id not in ADMINS:
        await message.reply("У вас немає прав адміністратора для цієї команди.")
        return
    raw_parts = message.text.split()
    if len(raw_parts) < 3: 
        await message.reply("Будь ласка, вкажіть список ID або номерів телефонів та текст.\nПриклад: /send_segment +380660000000 123456789 Ваш текст.")
        return
    identifiers = []
    text_parts = []
    found_text_start = False
    for part in raw_parts[1:]:
        if (part.startswith('+') and part[1:].isdigit() and len(part) > 5) or (part.isdigit()):
            if not found_text_start:
                identifiers.append(part)
            else:
                text_parts.append(part)
        else:
            found_text_start = True
            text_parts.append(part)
    text_to_send = " ".join(text_parts).strip()
    if not identifiers or not text_to_send:
        await message.reply("Не вдалося розпізнати список ідентифікаторів або текст повідомлення. Переконайтесь, що список йде перед текстом.")
        return
    users_data = await get_users_by_list(identifiers)
    target_uids = list(users_data.keys())
    if not target_uids:
        await message.reply(f"Не знайдено жодного користувача за вказаними {len(identifiers)} ідентифікаторами.")
        return
    await message.answer(f"Починаю цільову розсилку для **{len(target_uids)}** користувачів. Будь ласка, зачекайте.", parse_mode='Markdown')
    sent = 0
    failed = 0
    for uid in target_uids:
        try:
            await asyncio.sleep(0.15)
            await bot.send_message(chat_id=uid, text=text_to_send)
            sent += 1
        except TelegramForbiddenError:
            failed += 1
            logging.warning(f"INFO: Користувач {uid} заблокував бота.")
        except Exception as e:
            failed += 1
            logging.error(f"DEBUG: Помилка при відправці користувачу {uid}: {type(e).__name__}.") 
    final_result = f"Цільова розсилка завершена.\nУспіх: {sent}, помилки: {failed}\n"
    if failed > 0:
        final_result += "Зверніть увагу: користувачі, які заблокували бота, залишені у базі даних."
    await message.answer(final_result)
    
@dp.message(Command("delete_segment"))
async def cmd_delete_segment(message: Message):
    if message.from_user.id not in ADMINS:
        await message.reply("У вас немає прав адміністратора для цієї команди.")
        return
    
    raw_parts = message.text.split()
    if len(raw_parts) < 2: 
        await message.reply("Будь ласка, вкажіть список ID або номерів телефонів для видалення.\nПриклад: /delete_segment +38066... 12345...")
        return

    identifiers = []
    
    for part in raw_parts[1:]:
        if (part.startswith('+') and part[1:].isdigit() and len(part) > 5) or (part.isdigit()):
            identifiers.append(part)

    if not identifiers:
        await message.reply("Не вдалося розпізнати список ідентифікаторів. Переконайтесь, що вони написані через пробіл.")
        return

    try:
        deleted_count = await delete_users_by_list(identifiers)
        
        if deleted_count > 0:
            await message.reply(f"✅ Успішно видалено **{deleted_count}** користувач(а/ів) з бази даних.", parse_mode='Markdown')
        else:
            await message.reply(f"Не знайдено жодного користувача за вказаними ідентифікаторами.")
            
    except Exception as e:
        logging.error(f"Помилка при виконанні /delete_segment: {e}")
        await message.reply(f"❌ Сталася помилка: {e}")


# --- ХЕНДЛЕРИ FSM (Машини станів) ДЛЯ РОЗСИЛКИ ---

@dp.message(Command("cancel"))
async def cancel_broadcast(message: Message, state: FSMContext):
    """Дозволяє адміну скасувати процес розсилки."""
    if message.from_user.id not in ADMINS:
        return
    current_state = await state.get_state()
    if current_state is None:
        return
    
    logging.info(f"Адмін {message.from_user.id} скасував FSM стан.")
    await state.clear()
    await message.answer("Дію скасовано.")

@dp.message(
    BroadcastStates.waiting_for_content,
    F.content_type.in_({'text', 'photo', 'video', 'document', 'poll', 'audio', 'voice'})
)
async def handle_broadcast_content(message: Message, state: FSMContext):
    """
    Отримує контент для розсилки, визначає його 'заголовок' (title) 
    та зберігає все у FSM, очікуючи вибору папки.
    """
    
    post_title = None
    text_to_check_filter = None 
    
    if message.text:
        post_title = message.text.split('\n')[0]
        text_to_check_filter = message.text
    elif message.caption:
        post_title = message.caption.split('\n')[0]
        text_to_check_filter = message.caption
    elif message.poll:
        post_title = f"ОПИТУВАННЯ: {message.poll.question}"
    elif message.photo:
        post_title = "[Фото]" 
    elif message.video:
        post_title = "[Відео]"
    elif message.document:
        post_title = f"[Документ: {message.document.file_name or 'файл'}]"
    elif message.audio:
        post_title = f"[Аудіо: {message.audio.file_name or 'трек'}]"
    elif message.voice:
        post_title = "[Голосове повідомлення]"
    
    if not post_title:
        post_title = "[Медіа-контент]" 

    await state.update_data(
        content_chat_id=message.chat.id,
        content_message_id=message.message_id,
        post_title=post_title[:100], 
        text_to_check_filter=text_to_check_filter
    )
    
    await message.answer(
        "Контент отримано. Тепер, будь ласка, оберіть 'папку' для збереження:",
        reply_markup=await generate_folder_keyboard(for_admin=True)
    )
    await state.set_state(BroadcastStates.waiting_for_folder)

@dp.message(BroadcastStates.waiting_for_content)
async def handle_broadcast_invalid_content(message: Message, state: FSMContext):
    """Обробляє непідтримуваний контент (стікери тощо) у стані розсилки."""
    await message.answer("Непідтримуваний тип контенту (наприклад, стікер або локація). Будь ласка, надішліть текст, фото, відео, документ або опитування. Або /cancel для відміни.")

@dp.callback_query(BroadcastStates.waiting_for_folder, F.data.startswith('save_to_folder_'))
async def handle_broadcast_folder(callback: CallbackQuery, state: FSMContext):
    """
    Отримує папку, ПУБЛІКУЄ в архів, ЗБЕРІГАЄ в БД, 
    ЗАПУСКАЄ розсилку і чистить стан.
    """
    await callback.answer() 
    
    if not ARCHIVE_CHANNEL_ID:
        await callback.message.edit_text("❌ **Помилка:** Адміністратор не налаштував `ARCHIVE_CHANNEL_ID` у файлі .env. Розсилка неможлива.")
        await state.clear()
        return

    folder_id = int(callback.data.split('_')[-1])
    user_data = await state.get_data()
    
    chat_id = user_data.get('content_chat_id')
    message_id = user_data.get('content_message_id')
    post_title = user_data.get('post_title')
    text_to_check_filter = user_data.get('text_to_check_filter')

    if not chat_id or not message_id or not post_title:
        await callback.message.edit_text("Помилка: Контент розсилки не знайдено (можливо, минув час FSM). Спробуйте /broadcast знову.")
        await state.clear()
        return

    try:
        # 1. Публікуємо в Канал-Архів
        archive_msg = await bot.forward_message(
            chat_id=ARCHIVE_CHANNEL_ID,
            from_chat_id=chat_id,
            message_id=message_id
        )
        archive_message_id = archive_msg.message_id
        
    except Exception as e:
        logging.error(f"Не вдалося опублікувати в архівний канал: {e}")
        await callback.message.edit_text(f"❌ **Помилка:** Не вдалося опублікувати в архівний канал. Перевірте, чи бот є адміністратором каналу.\n\n{e}")
        await state.clear()
        return

    # 2. Визначаємо фільтр (тільки якщо це був текст)
    broadcast_filter = None
    if text_to_check_filter:
        parts = text_to_check_filter.split(maxsplit=1)
        if len(parts) == 2 and not text_to_check_filter.startswith('/'):
            broadcast_filter = parts[0].strip()
            # Оновлюємо заголовок, щоб він не містив фільтра
            post_title = parts[1].strip().split('\n')[0][:100]
            
    # 3. Зберігаємо в БД (якщо обрана папка)
    if folder_id != 0:
        try:
            await save_post(folder_id, post_title, archive_message_id)
            await callback.message.edit_text(f"Пост збережено у папку. Починаю розсилку...")
        except Exception as e:
            logging.error(f"Помилка збереження посту в БД: {e}")
            await callback.message.edit_text(f"Помилка збереження посту: {e}. Розсилка скасована.")
            await state.clear()
            return
    else:
        await callback.message.edit_text("Пост не буде збережено. Починаю розсилку...")
        
    # 4. Запускаємо розсилку (копіюємо з архіву)
    await process_broadcast_message(
        content_chat_id=ARCHIVE_CHANNEL_ID,
        content_message_id=archive_message_id,
        message=callback.message,
        broadcast_filter=broadcast_filter
    )
    
    # 5. Очищуємо стан
    await state.clear()


# --- ХЕНДЛЕРИ ДЛЯ ПЕРЕГЛЯДУ ПАПОК (НОВА ЛОГІКА) ---

async def show_folder_contents(target: types.Message | types.CallbackQuery, folder_id: int, is_admin: bool = False):
    """Відображає список кнопок (постів) у папці."""
    
    if not ARCHIVE_CHANNEL_ID:
        msg_target = target.message if isinstance(target, types.CallbackQuery) else target
        await msg_target.answer("❌ **Помилка:** Адміністратор не налаштував `ARCHIVE_CHANNEL_ID` у файлі .env. Перегляд меню неможливий.")
        if isinstance(target, types.CallbackQuery): await target.answer()
        return
        
    posts = await get_all_posts_by_folder(folder_id)
    
    if not posts:
        text = "Ця папка поки порожня."
        markup = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ До Головного меню", callback_data="back_to_menu")]
        ])
    else:
        text = "<b>Ось матеріали з цього розділу:</b>\n\nНатисніть на пост, щоб переглянути його."
        markup = generate_posts_list_keyboard(posts, is_admin)
            
    try:
        if isinstance(target, types.CallbackQuery):
            await target.message.edit_text(text, reply_markup=markup, parse_mode='HTML')
            await target.answer()
        else:
            await target.answer(text, reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logging.error(f"Помилка відображення списку папки: {e}")
        if isinstance(target, types.CallbackQuery): await target.answer("Помилка відображення.")

@dp.callback_query(F.data.startswith('folder_'))
async def handle_folder_click(callback: CallbackQuery, state: FSMContext):
    """Користувач натиснув на кнопку папки з /menu."""
    folder_id = int(callback.data.split('_')[-1])
    is_admin = callback.from_user.id in ADMINS
    await show_folder_contents(callback, folder_id, is_admin=is_admin)

@dp.callback_query(F.data.startswith('admin_folder_'))
async def handle_admin_folder_click(callback: CallbackQuery, state: FSMContext):
    """Адмін натиснув на кнопку папки з /menu (отримує кнопки видалення)."""
    folder_id = int(callback.data.split('_')[-1])
    await show_folder_contents(callback, folder_id, is_admin=True)

@dp.callback_query(F.data.startswith('view_post_'))
async def handle_view_post_click(callback: CallbackQuery):
    """Надсилає користувачу копію поста з архіву."""
    if not ARCHIVE_CHANNEL_ID:
        await callback.answer("Помилка: Канал-архів не налаштований.", show_alert=True)
        return

    message_id = int(callback.data.split('_')[-1])
    
    try:
        await bot.copy_message(
            chat_id=callback.from_user.id,
            from_chat_id=ARCHIVE_CHANNEL_ID,
            message_id=message_id
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"Не вдалося скопіювати пост {message_id} з архіву: {e}")
        await callback.answer(f"Помилка: Не вдалося завантажити цей пост. Можливо, його було видалено з архіву.", show_alert=True)

@dp.callback_query(F.data.startswith('del_post_'))
async def handle_delete_post_click(callback: CallbackQuery):
    """Видаляє пост з бази даних (але не з архіву)."""
    if callback.from_user.id not in ADMINS:
        await callback.answer("У вас немає прав.", show_alert=True)
        return

    post_id = int(callback.data.split('_')[-1])
    
    try:
        success, folder_id = await delete_post_by_id(post_id) 
        
        if success and folder_id:
            await callback.answer("✅ Пост видалено з меню!", show_alert=False)
            # Оновлюємо список постів у папці
            await show_folder_contents(callback, folder_id, is_admin=True)
        else:
            await callback.answer("❌ Помилка: Пост не знайдено в базі.", show_alert=True)
    except Exception as e:
        logging.error(f"Помилка видалення поста ID {post_id}: {e}")
        await callback.answer("❌ Помилка бази даних.", show_alert=True)


@dp.callback_query(F.data == 'back_to_menu')
async def handle_back_to_menu(callback: CallbackQuery):
    """Обробляє повернення до головного меню папок."""
    is_admin = callback.from_user.id in ADMINS
    await callback.message.edit_text(
        "📂 **Головне меню**\n\nОберіть розділ, який вас цікавить:",
        reply_markup=await generate_folder_keyboard(for_admin=False, is_admin_menu=is_admin),
        parse_mode='Markdown'
    )
    await callback.answer()

@dp.callback_query(F.data == 'ignore')
async def handle_ignore_click(callback: CallbackQuery):
    """Ігноруємо натискання на неактивні кнопки."""
    await callback.answer()


# --- ФІНАЛЬНИЙ УНІВЕРСАЛЬНИЙ ХЕНДЛЕР (обробляє всі не-команди) ---
@dp.message() 
async def handle_all_messages(message: Message, state: FSMContext):
    """
    Обробляє:
    1. Контакти (Найвищий пріоритет)
    2. Відповіді адміна
    3. Натискання кнопки "Меню"
    4. Натискання кнопки "Адмін-панель"
    5. Повідомлення від користувачів (пересилання)
    """
    admin_id = message.from_user.id
    current_state = await state.get_state()

    if current_state is not None:
        return
    
    # 1. ОБРОБКА КОНТАКТУ (Найвищий пріоритет для всіх)
    if message.contact:
        user_id = message.from_user.id
        phone = message.contact.phone_number
        
        global pool
        async with pool.acquire() as conn:
            user_data = await conn.fetchrow("SELECT username, full_name FROM users WHERE user_id = $1", user_id)
        
        username = user_data['username'] if user_data else "Unknown"
        full_name = user_data['full_name'] if user_data else "Невідоме ім'я"
        
        await add_user(user_id, username, full_name, phone)
        
        keyboard = get_admin_keyboard() if user_id in ADMINS else get_menu_only_keyboard()
        
        # ❗ ОНОВЛЕНО: Твоє гарне привітальне повідомлення
        greeting = f"""🌿 Привіт!
Раді вітати тебе у навчальному боті EVA ХРК 💚

Тут ти знайдеш:
📚 корисні матеріали для розвитку,
🗓 актуальні навчальні події,
🧠 опитування для вдосконалення,
і найголовніше — підтримку на твоєму шляху в EVA 🌸

Натисни меню нижче, щоб розпочати 👇"""
        
        await message.answer(
            greeting,
            reply_markup=keyboard
        )
        return

    # 2. ОБРОБКА ВІДПОВІДІ АДМІНА
    if admin_id in ADMINS and message.reply_to_message:
        reply_message = message.reply_to_message
        logging.info("Адмін відповідає. Аналізуємо повідомлення...")

        target_user_id = extract_user_id_from_reply(reply_message)

        if target_user_id:
            try:
                admin_signature = ADMIN_TITLES.get(admin_id, "адміністратора") 
                
                if message.text:
                    safe_admin_text = escape_html(message.text)
                    await bot.send_message(
                        chat_id=target_user_id,
                        text=f"👨‍💻 <b>Відповідь від {admin_signature}:</b>\n\n{safe_admin_text}",
                        parse_mode='HTML'
                    )
                else:
                    await message.copy_to(target_user_id) 
                    await bot.send_message(target_user_id, f"(Відповідь від {admin_signature})")

                await message.answer(f"✅ Відповідь успішно надіслана користувачу з ID: <code>{target_user_id}</code>", parse_mode='HTML')
                
                await close_support_ticket(target_user_id, admin_id)
                return
                
            except TelegramForbiddenError:
                await message.answer(f"❌ Помилка: Користувач з ID <code>{target_user_id}</code> заблокував бота.", parse_mode='HTML')
                return
            except Exception as e:
                logging.exception(f"Помилка при відправці адміністратору → користувачу {target_user_id}: {e}")
                await message.answer(f"❌ Помилка при відправці: {e}")
                return
        else:
            logging.warning(f"Адмін {admin_id} спробував відповісти, але ID не знайдено.")
            await message.answer(
                "❌ <b>Помилка:</b> Не можу знайти користувача. Будь ласка, відповідайте (Reply) "
                "**тільки** на повідомлення-підпис (де вказано ID) або на переслане повідомлення користувача.",
                parse_mode='HTML'
            )
            return

    # 3. ОБРОБКА КНОПКИ "МЕНЮ"
    if message.text == "📂 Меню":
        await cmd_menu(message) 
        return

    # 4. ОБРОБКА КНОПКИ "АДМІН-ПАНЕЛЬ"
    if message.text == "👑 Адмін-панель" and admin_id in ADMINS:
        admin_help_text = """
👑 **Адмін-панель** 👑

**Керування Контентом:**
`/broadcast` - Запустити розсилку та збереження в 'Меню'.
`/add_folder [Назва]` - Створити нову папку.
`/delete_folder "[Назва]"` - Видалити папку (та всі пости в ній).
`/delete_post "[Назва]"` - Видалити 1 пост з папки за його точною назвою.
*(Також видалення доступне кнопками ❌ в 'Меню' для адмінів)*

**Керування Користувачами:**
`/check_db` - Звіт по базі.
`/check_tickets` - Перевірити повідомлення без відповіді.
`/find_user [Запит]` - Знайти користувача.
`/delete_user [ID або Тел.]` - **(ОНОВЛЕНО)** Видалити користувача.
`/delete_segment [Список ID/Тел.]` - Видалити групу користувачів.
`/export_csv` - Отримати .csv файл з базою.

**Цільові Розсилки:**
`/send_to_user [ID або Тел.] [Текст]` - **(ОНОВЛЕНО)** Надіслати повідомлення 1 користувачу.
`/send_segment [Список ID/Тел.] [Текст]` - Надіслати повідомлення групі.
        """
        await message.answer(admin_help_text, parse_mode='Markdown')
        return

    # 5. ПЕРЕСИЛАННЯ ПОВІДОМЛЕНЬ ВІД ЗВИЧАЙНИХ КОРИСТУВАЧІВ
    if message.from_user.id not in ADMINS:
        # Ігноруємо команди
        if message.text and message.text.startswith('/'):
            return 
            
        user_id = message.from_user.id
        user_name = message.from_user.full_name or message.from_user.username or "Невідомий користувач"
        
        pool
        async with pool.acquire() as conn:
            phone_number = await conn.fetchval("SELECT phone_number FROM users WHERE user_id = $1", user_id)
        
        phone_display = phone_number or 'НЕ НАДАНО'
        
        safe_user_name = escape_html(user_name)
        safe_phone = escape_html(phone_display)
        
        # ❗ Створюємо тікет
        if message.text:
            await log_support_ticket(user_id, user_name, message.text[:200]) 
        else:
            await log_support_ticket(user_id, user_name, f"[{message.content_type or 'медіа'}]")

        caption = (
            f"📩 <b>НОВЕ ПОВІДОМЛЕННЯ ВІД КОРИСТУВАЧА</b>\n"
            f"Ім'я: <b>{safe_user_name}</b>\n" 
            f"📞 Телефон: <code>{safe_phone}</code>\n"
            f"🔑 ID: <code>{user_id}</code>\n" 
            f"--- Щоб відповісти, <b>натисніть 'Відповісти'</b> на це повідомлення. ---"
        )

        for target_admin_id in ADMINS:
            try:
                await message.forward(target_admin_id) 
                await bot.send_message(chat_id=target_admin_id, text=caption, parse_mode='HTML')
            except Exception as e:
                logging.error(f"Помилка при пересиланні адміністратору {target_admin_id}: {e}")

        await message.answer("✅ Ваше повідомлення отримано. Адміністратор незабаром відповість вам.")
        return

    # 6. ІНШЕ: Ігноруємо
    pass 


# --- ❗❗❗ ОНОВЛЕНИЙ БЛОК ЗАПУСКУ (WEBHOOK + POLLING) ❗❗❗ ---

    # --- Запуск бота ---

import os
import logging
import asyncpg
import asyncio
from aiohttp import web

# ❗ ВАЖЛИВО:
# ❗ Імпорти 'Bot' і 'Dispatcher' тут ВИДАЛЕНО.
# ❗ Вони мають бути імпортовані ТІЛЬКИ ОДИН РАЗ на самому початку
# ❗ вашого файлу bot.py, там, де ви їх і оголошуєте.
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application


# ❗ ВАЖЛИВО:
# ❗ ВСІ ЦІ РЯДКИ (BOT_TOKEN, bot=, dp=) ТРЕБА ПОВНІСТЮ ВИДАЛИТИ
# ❗ з цього блоку в кінці файлу.
#
# ❗ Вони вже існують у вашому глобальному скоупі (на початку файлу).
# ❗ Повторне оголошення 'dp = Dispatcher()' тут - це і є причина 404.
#
# BOT_TOKEN = os.getenv("BOT_TOKEN")              # <--- ВИДАЛИТИ
# DATABASE_URL = os.getenv("DATABASE_URL")        # <--- ВИДАЛИТИ
# ARCHIVE_CHANNEL_ID = os.getenv("ARCHIVE_CHANNEL_ID") # <--- ВИДАЛИТИ
#
# bot = Bot(token=BOT_TOKEN)                      # <--- ВИДАЛИТИ (критична помилка)
# dp = Dispatcher()                               # <--- ВИДАЛИТИ (критична помилка 404)
# pool= None                                      # <--- ВИДАЛИТИ (вже оголошено глобально)


# ❗ Важливо: ми припускаємо, що у вас ГЛОБАЛЬНО (на початку файлу) вже є:
# 1. bot = Bot(...)
# 2. dp = Dispatcher(...) (з усіма вашими хендлерами)
# 3. pool: asyncpg.Pool = None
# 4. Глобальні функції: async def init_db(), async def populate_folders_if_empty()
# 5. Глобальні змінні з .env:
#    - BOT_TOKEN, DATABASE_URL, ARCHIVE_CHANNEL_ID
#    - BASE_WEBHOOK_URL = os.getenv("RENDER_EXTERNAL_URL")
#    - WEBHOOK_PATH
#    - WEBHOOK_URL
#    - WEB_SERVER_HOST
#    - WEB_SERVER_PORT


async def on_startup(app: web.Application):
    """Виконується ПІД ЧАС запуску aiohttp."""
    global pool # Отримуємо доступ до глобального 'pool'
    
    logging.info("Початок процедури on_startup...")
    
    # 1. Перевіряємо, чи .env завантажено (чи є змінні)
    if not DATABASE_URL:
        logging.critical("❌ DATABASE_URL не знайдено! Перевірте .env")
        raise RuntimeError("DATABASE_URL not set")
    if not WEBHOOK_URL:
        logging.critical("❌ WEBHOOK_URL не знайдено! Переконайтесь, що RENDER_EXTERNAL_URL є в .env")
        raise RuntimeError("WEBHOOK_URL not set")
        
    # 2. Створюємо пул БД
    try:
        pool = await asyncpg.create_pool(DATABASE_URL)
        # ❗ Ініціалізуємо БД ТУТ, ПІСЛЯ створення пулу
        await init_db()
        await populate_folders_if_empty()
        logging.info("✅ Пул бази даних створено та ініціалізовано.")
    except Exception as e:
        logging.critical(f"❌ Помилка підключення/ініціалізації БД: {e}")
        raise # Зупиняємо запуск, якщо БД не працює
        
    # 3. Встановлюємо вебхук
    try:
        # ❗ Використовуємо глобальну змінну WEBHOOK_URL (яка має брати RENDER_EXTERNAL_URL)
        await bot.set_webhook(WEBHOOK_URL, drop_pending_updates=True)
        logging.info(f"📡 Вебхук встановлено: {WEBHOOK_URL}")
    except Exception as e:
        logging.error(f"❌ Помилка встановлення вебхука: {e}")
        raise # Це також критично

async def on_shutdown(app: web.Application):
    """Виконується ПЕРЕД зупинкою aiohttp."""
    global pool # Отримуємо доступ до глобального 'pool'
    
    logging.info("Початок процедури on_shutdown...")
    
    # 1. Видаляємо вебхук
    try:
        await bot.delete_webhook()
        logging.info("🧹 Вебхук видалено")
    except Exception as e:
        logging.error(f"Помилка видалення вебхука: {e}")
        
    # 2. Закриваємо сесію бота
    await bot.session.close()
    logging.info("🧹 Сесію бота закрито")
    
    # 3. Закриваємо пул БД
    if pool:
        try:
            await pool.close()
            logging.info("🧹 Пул бази даних закрито")
        except Exception as e:
            logging.error(f"Помилка закриття пулу БД: {e}")

async def handle_root(request: web.Request) -> web.Response:
    """Для перевірок 'health check' від Render."""
    return web.Response(text="✅ EVA HRK бот активний і працює!", content_type='text/plain')

# [ ВАШІ ФУНКЦІЇ on_startup, on_shutdown, handle_root ЗАЛИШАЮТЬСЯ ТУТ БЕЗ ЗМІН ]

# ❗ ЗАМІНІТЬ ВАШУ 'async def main()' НА ЦЮ (З ВИПРАВЛЕНИМИ ВІДСТУПАМИ)
# [ ВАШІ ФУНКЦІЇ on_startup, on_shutdown, handle_root ЗАЛИШАЮТЬСЯ ТУТ БЕЗ ЗМІН ]
# [ ТАКОЖ ЗАЛИШАЮТЬСЯ БЕЗ ЗМІН ВАШІ ГЛОБАЛЬНІ ЗМІННІ (WEBHOOK_PATH, WEB_SERVER_PORT тощо) ]

## ❗ Ця функція є СИНХРОННОЮ і тримає процес живим на Render.
def start_bot_webhook():
    """
    Головна функція для конфігурації та запуску веб-сервера.
    Використовує стандартний web.run_app() для максимальної сумісності
    з Render.
    """
    
    # 1. Перевірка BOT_TOKEN (це вже мало бути в on_startup, але перевірка не завадить)
    if not BOT_TOKEN:
        logging.critical("❌ Не знайдено BOT_TOKEN. Запуск неможливий.")
        return
            
    # 2. Створюємо AIOHTTP-додаток (залежить від глобального імпорту 'web')
    app = web.Application()

    # 3. Реєструємо хендлери життєвого циклу (async)
    #    Ці функції будуть викликані автоматично web.run_app
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    # 4. Root route для перевірки
    app.router.add_get("/", handle_root)
    
    # 5. Налаштовуємо aiogram (Метод SimpleRequestHandler - ФІКС 404)
    # ❗ Використовуємо глобальні dp та bot
    webhook_handler = SimpleRequestHandler(
        dispatcher=dp,
        bot=bot
    )
    # Реєструємо хендлер у aiohttp, використовуючи глобальний WEBHOOK_PATH
    webhook_handler.register(app, path=WEBHOOK_PATH)
    
    # Налаштовуємо setup_application (для FSM, middleware), але вимикаємо webhooks
    setup_application(app, dp, bot=bot, handle_webhooks=False)
    
    logging.info(f"Хендлер вебхука зареєстровано (SimpleRequestHandler) на шляху: {WEBHOOK_PATH}")
    logging.info(f"======== 🚀 Запуск сервера на http://{WEB_SERVER_HOST}:{WEB_SERVER_PORT} ========")


    # 6. Запуск веб-сервера.
    #    Цей виклик є СИНХРОННИМ і тримає процес живим (ФІКС 1-ХВИЛИНА).
    try:
        web.run_app(
            app,
            host=WEB_SERVER_HOST,
            port=WEB_SERVER_PORT,
            access_log=None 
        )
    except Exception as e:
        logging.critical(f"❌ Критична помилка під час web.run_app: {e}")
        

if __name__ == "__main__":
    try:
        # Викликаємо СИНХРОННУ функцію
        start_bot_webhook()
    except (KeyboardInterrupt, SystemExit, RuntimeError) as e:
        logging.info("Бот зупинено.")
    except Exception as e:
        logging.critical(f"ПОМИЛКА ЗАПУСКУ: {e}")