import sqlite3

# Connect to existing DB
conn = sqlite3.connect("articles.db")
cursor = conn.cursor()

# Create authors table if not exists
cursor.execute('''
CREATE TABLE IF NOT EXISTS authors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    source TEXT
)
''')
conn.commit()

# Fetch authors from articles
cursor.execute("SELECT author, source FROM articles WHERE author IS NOT NULL AND author != ''")
authors = cursor.fetchall()

# Insert into authors table (including duplicates)
for name, source in authors:
    cursor.execute("INSERT INTO authors (name, source) VALUES (?, ?)", (name.strip(), source.strip()))

conn.commit()
conn.close()

print("Authors successfully extracted and saved.")
