# view_users.py
import sqlite3

DB_PATH = "users.db"

def view_users():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, username FROM users")
    rows = cursor.fetchall()
    conn.close()
    return rows

if __name__ == "__main__":
    users = view_users()
    if not users:
        print("Поки що користувачів немає.")
    else:
        print("Список користувачів:")
        for u in users:
            print(u)
