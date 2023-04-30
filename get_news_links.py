import os
import time
import logging
from GoogleNews import GoogleNews
import pandas as pd
import sqlite3
from typing import List
from tqdm import tqdm

# Configuration options
CONFIG = {
    'max_retries': 3,
    'retry_delay': 60,  # seconds
    'checkpoint_db': 'data/checkpoint.db',
    'output_format': 'parquet'  # Options: 'parquet', 'csv', 'json'
}

# Initialize logging
logging.basicConfig(
    filename='script.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

def load_players():
    """Load player data from a parquet file."""
    return pd.read_parquet('data/values_players.parquet')

def get_top_n_players(df, n=500):
    """Get the top N players sorted by value_2qb."""
    return df.sort_values('value_2qb', ascending=False).head(n)

def fetch_news_links(variation: str) -> List[str]:
    """Fetch news article links for a given variation using GoogleNews."""
    googlenews = GoogleNews(lang='en', region='US', period='3w')
    googlenews.search(variation)
    links = googlenews.get_links()
    googlenews.clear()
    return links

def get_player_links(player_name):
    """Retrieve the news article links for a player and their variations."""
    variations = [player_name, f"{player_name} Fantasy Football", f"{player_name} Dynasty Superflex"]
    links = []

    for var in variations:
        try:
            links += fetch_news_links(var)
        except Exception as e:
            logging.warning(f"Failed to fetch links for {var}: {e}")

    return list(set(links))

def initialize_checkpoint_db():
    """Create the SQLite checkpoint database if it does not exist."""
    with sqlite3.connect(CONFIG['checkpoint_db']) as conn:
        conn.execute('CREATE TABLE IF NOT EXISTS checkpoints (player_name TEXT PRIMARY KEY, links TEXT)')

def save_checkpoint(player_name, links):
    """Save a checkpoint to the SQLite database."""
    with sqlite3.connect(CONFIG['checkpoint_db']) as conn:
        conn.execute('INSERT OR REPLACE INTO checkpoints VALUES (?, ?)', (player_name, ','.join(links)))

def load_checkpoints():
    """Load the checkpoints from the SQLite database."""
    with sqlite3.connect(CONFIG['checkpoint_db']) as conn:
        rows = conn.execute('SELECT * FROM checkpoints').fetchall()

    player_links = {row[0]: row[1].split(',') for row in rows}
    return player_links

def save_output(df_links):
    """Save the final result in the specified format."""
    if CONFIG['output_format'] == 'parquet':
        df_links.to_parquet('data/df_player_links.parquet')
    elif CONFIG['output_format'] == 'csv':
        df_links.to_csv('data/df_player_links.csv')
    elif CONFIG['output_format'] == 'json':
        df_links.to_json('data/df_player_links.json', orient='index')
    else:
        logging.error(f"Invalid output format: {CONFIG['output_format']}")

def main():
    df_players = load_players()
    top_N_players = get_top_n_players(df_players)

    # Initialize the checkpoint database
    initialize_checkpoint_db()

    # Load the checkpoint data
    player_links = load_checkpoints()

    # Use tqdm progress bar to display progress
    for player_name in tqdm(top_N_players['player'], desc='Processing players'):
        if player_name not in player_links:
            try:
                player_links[player_name] = get_player_links(player_name)
                save_checkpoint(player_name, player_links[player_name])
                logging.info(f"Retrieved links for {player_name}")
            except Exception as e:
                logging.error(f"Error while processing {player_name}: {e}")

    # Save the final result
    df_links = pd.DataFrame({'link': player_links})
    df_links.index.name = 'player_name'
    save_output(df_links)

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
