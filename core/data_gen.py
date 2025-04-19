from faker import Faker
import csv
import time #import uuid
import random

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
                'tweet_id': hash(str(time.time())), #str(uuid.uuid4()),
                'user_id': random.choice(user_ids),
                'text': tweet_text
            }

if __name__ == "__main__":
    #csv_file_path = 'tweets.csv'  # Make sure this path is correct
    
    # Test the tweet_generator by printing out a few tweets
    print("Sample generated tweets:\n")
    for i, tweet in enumerate(tweet_generator()):
        print(tweet)
        if i >= 4:  # Just print the first 5 tweets for testing
            break
