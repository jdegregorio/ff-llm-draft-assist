import os
import time
import logging
import json
import pickle
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from typing import List
from tqdm import tqdm

# Configuration options
CONFIG = {
    'max_retries': 3,
    'retry_delay': 60,  # seconds
    'checkpoint_db': 'data/checkpoint_scraper.db',
    'output_format': 'json'  # Options: 'json', 'pickle'
}

# Initialize logging
logging.basicConfig(
    filename='scraper.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

def load_player_links():
    """Load player links from the parquet file."""
    return pd.read_parquet('./data/df_player_links.parquet')

def initialize_checkpoint_db():
    """Create the SQLite checkpoint database if it does not exist."""
    with sqlite3.connect(CONFIG['checkpoint_db']) as conn:
        conn.execute('CREATE TABLE IF NOT EXISTS checkpoints (player_name TEXT PRIMARY KEY, articles TEXT)')

def save_checkpoint(player_name, articles):
    """Save a checkpoint to the SQLite database."""
    serialized_articles = ','.join(articles)
    with sqlite3.connect(CONFIG['checkpoint_db']) as conn:
        conn.execute('INSERT OR REPLACE INTO checkpoints VALUES (?, ?)', (player_name, serialized_articles))

def load_checkpoints():
    """Load the checkpoints from the SQLite database."""
    with sqlite3.connect(CONFIG['checkpoint_db']) as conn:
        rows = conn.execute('SELECT * FROM checkpoints').fetchall()

    player_articles = {row[0]: row[1].split(',') for row in rows}
    return player_articles

def fetch_article_text(url: str):
    """Fetch and parse the content of a URL using BeautifulSoup."""
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Remove unnecessary elements (header, footer, etc.)
    for elem in soup(['header', 'footer']):
        elem.decompose()

    # Extract text from the remaining elements
    text = soup.get_text(separator=' ')
    return text

def scrape_articles(urls: List[str]):
    """Scrape URLs and extract text using BeautifulSoup."""
    return [fetch_article_text(url) for url in urls]

def save_output(player_articles):
    """Save the final result in the specified format."""
    if CONFIG['output_format'] == 'json':
        with open('data/player_articles.json', 'w') as f:
            json.dump(player_articles, f)
    elif CONFIG['output_format'] == 'pickle':
        with open('data/player_articles.pkl', 'wb') as f:
            pickle.dump(player_articles, f)
    else:
        logging.error(f"Invalid output format: {CONFIG['output_format']}")

def main():
    player_links = load_player_links()

    # Initialize the checkpoint database
    initialize_checkpoint_db()

    # Load the checkpoint data
    player_articles = load_checkpoints()

    total_urls = sum([len(player_links.loc[player_name, 'link']) for player_name in player_links.index])
    progress_bar = tqdm(total=total_urls, desc="Processing URLs")

    for player_name in player_links.index:
        links = player_links.loc[player_name, 'link']
        if player_name not in player_articles:
            articles = []
            for url in links:
                try:
                    articles.append(fetch_article_text(url))
                    logging.info(f"Scraped and extracted text for {player_name} - {url}")
                except Exception as e:
                    logging.error(f"Error while processing {player_name} - {url}: {e}")
                progress_bar.update(1)

            save_checkpoint(player_name, articles)
            player_articles[player_name] = articles

    progress_bar.close()

    # Save the final result
    save_output(player_articles)

if __name__ == '__main__':
    retries = 0
    retry_delay = CONFIG['retry_delay']

    while retries < CONFIG['max_retries']:
        try:
            main()
            break
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            retries += 1
            retry_delay *= 2  # Exponential backoff
            time.sleep(retry_delay)
