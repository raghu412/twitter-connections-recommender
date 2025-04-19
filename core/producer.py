import json
import time
import random
from kafka import KafkaProducer
from faker import Faker


# Initialize Faker for generating synthetic data
fake = Faker()

def generate_synthetic_tweet():
    tweet = {
        "user_id": str(fake.random_number(digits=3, fix_len=True)), 
        "tweet_id": hash(str(time.time())),
        "text": fake.sentence(nb_words=12) + " " + " ".join("#" + fake.word() for _ in range(random.randint(0, 3))),
    }
    return tweet

def main():
    # Configure the Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='127.0.0.1:9092',  # Replace with your Kafka broker address if different
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        #metadata_timeout_ms=120000 
    )

    topic = "posts"

    print("Starting synthetic tweet production...")

    # Produce messages continuously
    try:
        while True:
            tweet_data = generate_synthetic_tweet()
            producer.send(topic, tweet_data)
            print(f"Produced tweet: {tweet_data}")
            time.sleep(5)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
