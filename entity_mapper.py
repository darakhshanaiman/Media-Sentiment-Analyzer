import sqlite3
import spacy
import re
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO)
nlp = spacy.load("en_core_web_sm")

def get_context(text, entity_text):
    try:
        idx = text.lower().index(entity_text.lower())
        return text[max(0, idx - 100): idx + 100]
    except ValueError:
        return ""

# Connect to DB
conn = sqlite3.connect("articles.db")
cursor = conn.cursor()
cursor.execute("DROP TABLE IF EXISTS mapped_entities")
# Ensure mapped_entities table exists
cursor.execute('''
CREATE TABLE IF NOT EXISTS mapped_entities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER,
    name TEXT,
    other_name TEXT,
    entity_type TEXT,
    article_id INTEGER,
    context TEXT,
    FOREIGN KEY (entity_id) REFERENCES entities(id)
)
''')

# Ensure unique index exists
cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='unique_entity_id'")
unique_index_exists = cursor.fetchone() is not None

if not unique_index_exists:
    logging.info("Creating unique index 'unique_entity_id' on mapped_entities (entity_id)")
    try:
        cursor.execute("CREATE UNIQUE INDEX unique_entity_id ON mapped_entities (entity_id)")
        conn.commit()
        logging.info("Unique index created successfully.")
    except sqlite3.Error as e:
        logging.error(f"Failed to create unique index: {e}")
        raise

# Load article contents into dictionary
cursor.execute("SELECT id, content FROM articles")
article_dict = {id: content for id, content in cursor.fetchall()}

# Load all extracted entities from the *existing* `entities` table
cursor.execute("SELECT id, article_id, name, entity_type FROM entities") # Get entity ID as well
entities = cursor.fetchall()

# Dictionary to organize entity mappings
entity_dict = {}

for entity_id, article_id, name, entity_type in entities: # Include entity_id
    if article_id not in article_dict:
        continue

    content = article_dict[article_id]  # Get the full article content
    context = get_context(content, name)

    if entity_id not in entity_dict:
        entity_dict[entity_id] = {
            'name': name,
            'aliases': set(),
            'entity_type': entity_type,
            'article_ids': set(),
            'contexts': [],
            'all_names': set()
        }

    entity_dict[entity_id]['aliases'].add(name)
    entity_dict[entity_id]['article_ids'].add(article_id)
    entity_dict[entity_id]['contexts'].append((article_id, context))
    entity_dict[entity_id]['all_names'].add(name)

    # Check for contextual alternative names
    doc = nlp(content)
    for ent in doc.ents:
        if ent.label_ in ["PERSON", "ORG", "GPE", "LOC"]: # Only consider the same entity types
            if ent.text != name:
                entity_dict[entity_id]['aliases'].add(ent.text)

# Create a temporary table for new mappings
cursor.execute('''
    CREATE TEMP TABLE temp_mapped_entities (
        entity_id INTEGER,
        name TEXT,
        other_name TEXT,
        entity_type TEXT,
        article_id INTEGER,
        context TEXT
    )
''')

# Insert into the temporary table
for entity_id, data in entity_dict.items():
    all_aliases = sorted(data['aliases'])
    original_name = data['name']
    other_names = ", ".join(alias for alias in all_aliases if alias != original_name)

    # Use one representative article and its context
    article_id = next(iter(data['article_ids']))
    context_for_article = next((c[1] for c in data['contexts'] if c[0] == article_id), "")

    cursor.execute("""
        INSERT INTO temp_mapped_entities (entity_id, name, other_name, entity_type, article_id, context)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (entity_id, original_name, other_names, data['entity_type'], article_id, context_for_article))

# Drop the old table
cursor.execute('DROP TABLE IF EXISTS mapped_entities')

# Create the new table with an autoincrementing ID and the entity_id foreign key
cursor.execute('''
    CREATE TABLE mapped_entities (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_id INTEGER,
        name TEXT,
        other_name TEXT,
        entity_type TEXT,
        article_id INTEGER,
        context TEXT,
        FOREIGN KEY (entity_id) REFERENCES entities(id)
    )
''')

# Copy data from the temporary table to the new table
cursor.execute('''
    INSERT INTO mapped_entities (entity_id, name, other_name, entity_type, article_id, context)
    SELECT entity_id, name, other_name, entity_type, article_id, context
    FROM temp_mapped_entities
''')

# Drop the temporary table
cursor.execute('DROP TABLE temp_mapped_entities')

# Ensure unique index exists
cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='unique_entity_id'")
unique_index_exists = cursor.fetchone() is not None

if not unique_index_exists:
    logging.info("Creating unique index 'unique_entity_id' on mapped_entities (entity_id)")
    try:
        cursor.execute("CREATE UNIQUE INDEX unique_entity_id ON mapped_entities (entity_id)")
        conn.commit()
        logging.info("Unique index created successfully.")
    except sqlite3.Error as e:
        logging.error(f"Failed to create unique index: {e}")
        raise

# Remove duplicates (after data is in the new table)
cursor.execute('''
    DELETE FROM mapped_entities
    WHERE id NOT IN (
        SELECT MIN(id)
        FROM mapped_entities
        GROUP BY entity_id
    )
''')

conn.commit()
conn.close()
print("✅ Canonical entity mapping complete — one row per entity with all aliases in 'other_name'")