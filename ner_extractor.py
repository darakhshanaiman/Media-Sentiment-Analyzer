import sqlite3
import spacy
import logging
import string

logging.basicConfig(level=logging.INFO)
nlp = spacy.load("en_core_web_sm")

conn = sqlite3.connect("articles.db")
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS entities")

# Create the entities table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER,
    name TEXT,
    entity_type TEXT,
    FOREIGN KEY(article_id) REFERENCES articles(id)
)
''')

# Fetch all articles
cursor.execute("SELECT id, content FROM articles")
articles = cursor.fetchall()

def normalize(name):
    return name.strip().lower().translate(str.maketrans('', '', string.punctuation))

count = 0
for article_id, content in articles:
    if not content:
        continue

    doc = nlp(content)
    seen_names_in_article = set()

    for ent in doc.ents:
        if ent.label_ in ["PERSON", "ORG", "GPE", "LOC"]:
            name = ent.text.strip()
            norm_name = normalize(name)

            if norm_name not in seen_names_in_article:
                try:
                    cursor.execute('''
                        INSERT INTO entities (article_id, name, entity_type)
                        VALUES (?, ?, ?)
                    ''', (article_id, name, ent.label_))
                    seen_names_in_article.add(norm_name)
                    count += 1
                except sqlite3.Error as e:
                    logging.error(f"Database error: {e}")
                    logging.error(f"Failed to insert entity: article_id={article_id}, name={name}, entity_type={ent.label_}")

cursor.execute('''
    DELETE FROM entities
    WHERE rowid NOT IN (
        SELECT MIN(rowid)
        FROM entities
        GROUP BY article_id, name
    )
''')
conn.commit()
conn.close()

logging.info(f"Entity extraction complete. Unique entities saved per article: {count}")
