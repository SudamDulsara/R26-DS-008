import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'news_pipeline.db')

def get_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def initialize_db():
    conn = get_connection()
    cursor = conn.cursor()

    # Table for discovered URLs
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS discovered_urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            source TEXT NOT NULL,
            discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            fetched INTEGER DEFAULT 0
        )
    ''')

    # Table for extracted articles
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            source TEXT NOT NULL,
            title TEXT,
            author TEXT,
            published_date TEXT,
            category TEXT,
            raw_text TEXT,
            clean_text TEXT,
            sinhala_purity REAL,
            content_hash TEXT,
            is_duplicate INTEGER DEFAULT 0,
            crawl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Table for pipeline run logs
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS pipeline_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            source TEXT,
            urls_discovered INTEGER DEFAULT 0,
            articles_fetched INTEGER DEFAULT 0,
            articles_cleaned INTEGER DEFAULT 0,
            duplicates_removed INTEGER DEFAULT 0,
            status TEXT
        )
    ''')

    try:
        cursor.execute('ALTER TABLE discovered_urls ADD COLUMN fetch_attempts INTEGER DEFAULT 0')
    except sqlite3.OperationalError:
        pass

    conn.commit()
    conn.close()
    print("Database initialized successfully.")

if __name__ == "__main__":
    initialize_db()