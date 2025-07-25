import sqlite3

conn = sqlite3.connect("articles.db")
cursor = conn.cursor()

# Create tables
cursor.execute('''
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT,
        title TEXT,
        author TEXT,
        fdate TEXT,
        content TEXT
    )
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS mapped_entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    other_name TEXT,
    entity_type TEXT,
    article_id INTEGER,
    context TEXT
)
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS authors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        source TEXT
    )
''')

# Commit and print all table names
conn.commit()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()

print("Tables created in the database:")
for table in tables:
    print("-", table[0])

conn.close()


###########################################################################################################################################################################


# import sqlite3

# conn = sqlite3.connect("articles.db")
# cursor = conn.cursor()

# # Drop a table
# cursor.execute("DROP TABLE IF EXISTS mapped_entities")
# cursor.execute("DROP TABLE IF EXISTS entities")
# cursor.execute("DROP TABLE IF EXISTS entity")
# import sqlite3

# Rename 'fdate' to 'published_date'
# cursor.execute("""
#     ALTER TABLE entities RENAME COLUMN label TO entity_type
# """)
# # Rename 'fdate' to 'published_date'
# cursor.execute("""
#     ALTER TABLE entities RENAME COLUMN entity TO name
# """)

# cursor.execute("""
#     ALTER TABLE mapped_entities RENAME COLUMN canonical_name TO other_name
# """)
# conn.commit()
# conn.close()

# print("Table 'entity' deleted (if it existed).")
