class TweetStore:
    def __init__(self):
        self._store = {}

    def add_tweet(self, faiss_id, tweet):
        self._store[faiss_id] = tweet

    def get_tweet(self, faiss_id):
        return self._store.get(faiss_id)

    def get_all_tweets(self):
        return list(self._store.values())

    def get_all_ids(self):
        return list(self._store.keys())

    def __contains__(self, faiss_id):
        return faiss_id in self._store

# Singleton instance
tweet_store = TweetStore()
