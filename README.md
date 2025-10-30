# EVA Telegram Bot

Бот для внутрішнього навчання EVA ХРК — розсилки, папки з матеріалами, адмін-панель.

## Локальна розробка
1. Створити віртуальне середовище:
   `python -m venv venv`
2. Активувати:
   - Windows: `venv\Scripts\activate`
   - mac/linux: `source venv/bin/activate`
3. Встановити залежності:
   `pip install -r requirements.txt`
4. Створити `.env` на основі `.env.example` і вказати значення.
5. Запустити (на Render ми використовуємо webhook; локально можна тестувати інші частини):
   `uvicorn main:app --reload --port 8000`
