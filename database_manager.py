import sqlite3
import datetime
import logging
import os # Import os for path operations

logger = logging.getLogger(__name__)
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# --- Database Path Configuration ---
DB_FILENAME = 'pdf_bot_users.db'
DATABASE_DIR = os.environ.get("DATABASE_DIR", ".") # Default to current directory if env var not set
DATABASE_NAME = os.path.join(DATABASE_DIR, DB_FILENAME)
# --- End Database Path Configuration ---

def get_db_connection():
    db_dir = os.path.dirname(DATABASE_NAME)
    if db_dir and not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir, exist_ok=True)
            logger.info(f"Created database directory: {db_dir}")
        except OSError as e:
            logger.error(f"Failed to create database directory {db_dir}: {e}")
            raise # Re-raise error if directory creation fails, as DB connection will likely fail

    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def initialize_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            chat_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            registered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_pdf_sent_date DATE,
            receives_daily_pdf BOOLEAN DEFAULT 1
        )
    ''')
    conn.commit()
    conn.close()
    logger.info(f"Database '{DATABASE_NAME}' initialized/checked.")

def add_user(chat_id: int, username: str, first_name: str) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO users (chat_id, username, first_name, receives_daily_pdf)
            VALUES (?, ?, ?, 1)
        ''', (chat_id, username, first_name))
        conn.commit()
        logger.info(f"User {chat_id} ({username or 'NoUsername'}) added to '{DATABASE_NAME}'.")
        return True
    except sqlite3.IntegrityError:
        logger.info(f"User {chat_id} ({username or 'NoUsername'}) already exists in '{DATABASE_NAME}'.")
        return False
    finally:
        conn.close()

def get_user(chat_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE chat_id = ?", (chat_id,))
    user = cursor.fetchone()
    conn.close()
    return user

def set_user_receives_daily_pdf(chat_id: int, receives: bool):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET receives_daily_pdf = ? WHERE chat_id = ?", (1 if receives else 0, chat_id))
    conn.commit()
    conn.close()
    logger.info(f"User {chat_id} 'receives_daily_pdf' set to {receives} in '{DATABASE_NAME}'.")

def get_users_for_daily_pdf() -> list[int]:
    today_str = datetime.date.today().isoformat()
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        SELECT chat_id FROM users
        WHERE receives_daily_pdf = 1 AND
              (last_pdf_sent_date IS NULL OR last_pdf_sent_date != ?)
    ''', (today_str,))
    users_to_send = [row['chat_id'] for row in cursor.fetchall()]
    conn.close()
    return users_to_send

def update_pdf_sent_date(chat_id: int):
    today_str = datetime.date.today().isoformat()
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET last_pdf_sent_date = ? WHERE chat_id = ?", (today_str, chat_id))
    conn.commit()
    conn.close()
    logger.info(f"Updated last_pdf_sent_date for {chat_id} in '{DATABASE_NAME}'.")

def get_all_users_count() -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM users")
    count = cursor.fetchone()[0]
    conn.close()
    return count

def get_new_users_since(timestamp: datetime.datetime) -> list:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT chat_id, username, first_name FROM users WHERE registered_at > ?", (timestamp,))
    new_users = cursor.fetchall()
    conn.close()
    return new_users

if __name__ == '__main__':
    # For standalone testing, you might want to set DATABASE_DIR or ensure current dir is writable
    # Example: os.environ["DATABASE_DIR"] = "data_test_dbm"
    initialize_db()
    print(f"Database '{DATABASE_NAME}' schema ensured by database_manager.py direct run.")