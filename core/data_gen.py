from faker import Faker
import csv
import time 
import random
import json

fake = Faker()

# Generate 100 unique synthetic user IDs
user_ids = [fake.user_name() for _ in range(100)]

def tweet_generator():
    """
    Generator that yields tweet dictionaries from a CSV file.
    Each tweet includes a tweet_id, user_id, and text.
    """
    csv_file_path = "storage/twitter_dataset.csv"
    with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            tweet_text = row['Text'].strip()
            if not tweet_text:
                continue  # Skip empty tweet texts

            yield {
                'tweet_id': int(time.time() * 1000), #str(uuid.uuid4()),
                'user_id': random.choice(user_ids),
                'text': tweet_text
            }

def file_based_tweet_generator(file_path="storage/live_feed.jsonl"):
    """
    Generator that yields the most recent N tweets from a JSONL file.
    """
    seen = set()
    while True:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    tweet = json.loads(line.strip())
                    tweet_id = tweet["tweet_id"]
                    if tweet_id not in seen:
                        seen.add(tweet_id)
                        yield tweet
        except FileNotFoundError:
            continue

if __name__ == "__main__":
    #csv_file_path = 'tweets.csv'  # Make sure this path is correct
    
    # Test the tweet_generator by printing out a few tweets
    print("Sample generated tweets:\n")
    for i, tweet in enumerate(tweet_generator()):
        print(tweet)
        if i >= 4:  # Just print the first 5 tweets for testing
            break
