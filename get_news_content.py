import os
import time
import logging
import json
import pickle
from langchain.document_loaders import PlaywrightURLLoader
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
        conn.execute('CREATE TABLE IF NOT EXISTS checkpoints (player_name TEXT PRIMARY KEY, documents TEXT)')

def save_checkpoint(player_name, documents):
    """Save a checkpoint to the SQLite database."""
    serialized_docs = ','.join([json.dumps(doc) for doc in documents])
    with sqlite3.connect(CONFIG['checkpoint_db']) as conn:
        conn.execute('INSERT OR REPLACE INTO checkpoints VALUES (?, ?)', (player_name, serialized_docs))

def load_checkpoints():
    """Load the checkpoints from the SQLite database."""
    with sqlite3.connect(CONFIG['checkpoint_db']) as conn:
        rows = conn.execute('SELECT * FROM checkpoints').fetchall()

    player_documents = {row[0]: [json.loads(doc_str) for doc_str in row[1].split(',')] for row in rows}
    return player_documents

def scrape_and_create_documents(urls: List[str]):
    """Scrape URLs and create Document objects using PlaywrightURLLoader."""
    loader = PlaywrightURLLoader(urls=urls, remove_selectors=["header", "footer"])
    return loader.load()

def save_output(player_documents):
    """Save the final result in the specified format."""
    if CONFIG['output_format'] == 'json':
        with open('data/player_documents.json', 'w') as f:
            json.dump(player_documents, f)
    elif CONFIG['output_format'] == 'pickle':
        with open('data/player_documents.pkl', 'wb') as f:
            pickle.dump(player_documents, f)
    else:
        logging.error(f"Invalid output format: {CONFIG['output_format']}")

def main():
    player_links = load_player_links()

    # Initialize the checkpoint database
    initialize_checkpoint_db()

    # Load the checkpoint data
    player_documents = load_checkpoints()

    for player_name in tqdm(player_links.index, desc="Processing players"):
        links = player_links.loc[player_name, 'link']
        if player_name not in player_documents:
            try:
                documents = scrape_and_create_documents(links)
                save_checkpoint(player_name, documents)
                player_documents[player_name] = documents
                logging.info(f"Scraped and created documents for {player_name}")
            except Exception as e:
                logging.error(f"Error while processing {player_name}: {e}")

    # Save the final result
    save_output(player_documents)

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

