import json
import time
from kafka import KafkaProducer
from core.data_gen import tweet_generator

def produce_tweets():
    for tweet in tweet_generator():
        yield tweet

def main():
    producer = KafkaProducer(
        bootstrap_servers='127.0.0.1:9092', 
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

    topic = "posts"
    output_file = "storage/live_feed.jsonl"  # Local file to mirror the feed

    print("🚀 Starting synthetic tweet production...\nPress Ctrl+C to stop.")

    try:
        for tweet_data in produce_tweets():
            # Send to Kafka
            producer.send(topic, tweet_data)

            # Append to local file
            with open(output_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(tweet_data) + "\n")

            print(f"✅ Produced tweet: {tweet_data['tweet_id']} | User: {tweet_data['user_id']} | Text: {tweet_data['text'][:50]}...")
            time.sleep(1)
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
