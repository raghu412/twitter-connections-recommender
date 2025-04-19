import json
import time
from kafka import KafkaProducer
from core.data_gen import tweet_generator

def produce_tweets():
    """
    Generator that yields tweets from the tweet_generator in data_gen.py
    """
    for tweet in tweet_generator():
        yield tweet

def main():
    producer = KafkaProducer(
        bootstrap_servers='127.0.0.1:9092', 
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

    topic = "posts" 

    print("🚀 Starting synthetic tweet production...\nPress Ctrl+C to stop.")

    try:
        for tweet_data in produce_tweets():
            producer.send(topic, tweet_data)
            print(f"✅ Produced tweet: {tweet_data['tweet_id']} | User: {tweet_data['user_id']} | Text: {tweet_data['text'][:50]}...")
            time.sleep(1)  # Simulate real-time tweet stream
    except KeyboardInterrupt:
        print("\n🛑 Stopping producer...")
    except Exception as e:
        print(f"❌ Error occurred: {e}")
    finally:
        producer.flush()
        producer.close()
        print("✅ Kafka producer closed cleanly.")

if __name__ == "__main__":
    main()