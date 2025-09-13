"""
Helper utilities for the frontend.
"""

import sqlite3

def create_example_db(db_name="example.db"):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS mytable;")
    cur.execute("CREATE TABLE mytable (id INTEGER PRIMARY KEY, value REAL);")

    values = [(i, float(i) * 1.5) for i in range(1, 11)]
    cur.executemany("INSERT INTO mytable (id, value) VALUES (?, ?);", values)

    conn.commit()
    conn.close()
    print(f"Database '{db_name}' created with sample data.")
