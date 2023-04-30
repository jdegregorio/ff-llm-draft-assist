import json

# Load the JSON file
with open('data/player_articles.json', 'r') as f:
    player_articles = json.load(f)

# Initialize the counters
total_articles = 0
article_lengths = []

# Count the number of articles and their character counts
for player_name, articles in player_articles.items():
    total_articles += len(articles)
    for article in articles:
        article_lengths.append(len(article))

# Print the results
print(f"Total articles scraped: {total_articles}")
print(f"Character count for each article: {article_lengths}")
