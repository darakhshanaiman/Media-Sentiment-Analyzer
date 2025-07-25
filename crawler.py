import requests
from bs4 import BeautifulSoup
from readability import Document
from urllib.parse import urljoin, urlparse
import sqlite3
import logging

logging.basicConfig(level=logging.INFO)

# DB Setup
conn = sqlite3.connect("articles.db")
cursor = conn.cursor()

# Create articles table
cursor.execute('''
CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT,
    source TEXT,
    title TEXT,
    author TEXT,
    author_id INTEGER,
    date TEXT,
    content TEXT
)
''')

# Create authors table (allows duplicates)
cursor.execute('''
CREATE TABLE IF NOT EXISTS authors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    source TEXT
)
''')

conn.commit()

def is_valid_article(html):
    try:
        doc = Document(html)
        summary = BeautifulSoup(doc.summary(), "html.parser").get_text()
        return len(summary.strip()) > 200
    except Exception as e:
        logging.warning(f"Error in is_valid_article: {e}")
        return False

def save_author_and_get_id(name, source):
    # Save even if duplicate
    cursor.execute("INSERT INTO authors (name, source) VALUES (?, ?)", (name, source))
    conn.commit()
    return cursor.lastrowid

def extract_article(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            return None

        html = response.text
        if not is_valid_article(html):
            return None

        doc = Document(html)
        soup = BeautifulSoup(html, "html.parser")

        source = urlparse(url).netloc
        title = doc.title()

        author_tag = soup.find("meta", attrs={"name": "author"}) or soup.find("meta", attrs={"property": "article:author"})
        author = author_tag["content"] if author_tag and "content" in author_tag.attrs else ""

        date_tag = soup.find("meta", attrs={"property": "article:published_time"}) or \
                   soup.find("meta", attrs={"name": "pubdate"}) or soup.find("time")
        date = date_tag["content"] if date_tag and "content" in date_tag.attrs else (date_tag.get_text() if date_tag else "")

        content = BeautifulSoup(doc.summary(), "html.parser").get_text(separator="\n").strip()

        author_id = save_author_and_get_id(author, source)

        return {
            "url": url,
            "source": source,
            "title": title,
            "author": author,
            "author_id": author_id,
            "date": date,
            "content": content
        }

    except Exception as e:
        logging.warning(f"Failed to extract article from {url}: {e}")
        return None

def crawl(start_url, max_depth=1):
    visited = set()
    to_visit = [(start_url, 0)]

    while to_visit:
        current_url, depth = to_visit.pop(0)
        if current_url in visited or depth > max_depth:
            continue
        visited.add(current_url)

        try:
            response = requests.get(current_url, timeout=10)
            if response.status_code != 200:
                continue
            html = response.text
        except Exception as e:
            logging.warning(f"Error fetching {current_url}: {e}")
            continue

        # Extract if article
        article = extract_article(current_url)
        if article and len(article["content"].split()) > 100:
            cursor.execute('''
                INSERT INTO articles (url, source, title, author, author_id, date, content)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (article["url"], article["source"], article["title"],
                  article["author"], article["author_id"], article["date"], article["content"]))
            conn.commit()
            logging.info(f"Saved article: {article['title']}")

        # Follow links
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(skip in href.lower() for skip in ['privacy', 'terms', 'ads', 'login', 'signin', 'contact', 'javascript:void', 'tel:']):
                continue
            if href.startswith("javascript:") or href.endswith(".pdf"):
                continue
            full_url = urljoin(current_url, href)
            if urlparse(full_url).netloc == urlparse(start_url).netloc:
                to_visit.append((full_url, depth + 1))

if __name__ == "__main__":
    with open("urls.txt", "r") as f:
        base_urls = [line.strip() for line in f if line.strip()]
        for url in base_urls:
            crawl(url, max_depth=1)

    conn.close()
    logging.info("Crawling completed. Check articles.db.")
