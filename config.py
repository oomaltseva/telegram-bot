import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8466350127:AAGPONLejutzmB57KI7vj2TlgFkcjPwCXtM")
WEBHOOK_BASE = os.getenv("WEBHOOK_BASE", "")
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = WEBHOOK_BASE + WEBHOOK_PATH if WEBHOOK_BASE else None

ADMINS = os.getenv("ADMINS", "")  # список admin telegram id через кому, наприклад "12345,67890"
ADMINS = [int(x) for x in ADMINS.split(",") if x.strip().isdigit()]

DB_PATH = os.getenv("DB_PATH", "users.db")
