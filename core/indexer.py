import faiss
import numpy as np
from core.store import tweet_store
from sklearn.preprocessing import normalize

_index = None

def get_index(model):
    global _index
    if _index is None:
        dim = model.get_sentence_embedding_dimension()
        _index = faiss.IndexFlatL2(dim)
    return _index

def add_to_index(tweet, model, index):
    emb = model.encode(tweet["text"])
    emb = normalize([emb])[0]  # Normalize the embedding
    print(f"Embedding shape: {emb.shape}")
    faiss_id = index.ntotal
    index.add(np.array([emb]))
    tweet_store.add_tweet(faiss_id, tweet)
    return faiss_id, emb
