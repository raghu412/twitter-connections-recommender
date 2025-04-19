import os
import json
import streamlit as st
import nest_asyncio
import random
import logging
from core.store import tweet_store
from core.recommender import get_recommendations

# Apply necessary asyncio adjustments for Streamlit
nest_asyncio.apply()

# Set Streamlit configuration
os.environ["STREAMLIT_SERVER_ENABLE_FILE_WATCHER"] = "false"
st.set_page_config(page_title="Live Tweet Recommender", layout="wide")

# Constants
TWEET_FILE = "storage/live_feed.jsonl"
USER_ID = "barbaralee"

# Logging setup
logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load tweets from file
def load_all_tweets():
    tweets = []
    try:
        with open(TWEET_FILE, "r", encoding="utf-8") as f:
            for line in f:
                tweet = json.loads(line)
                tweets.append(tweet)
    except FileNotFoundError:
        pass
    return tweets

# Init session state
if "liked_tweets" not in st.session_state:
    st.session_state.liked_tweets = []

if "just_liked" not in st.session_state:
    st.session_state.just_liked = False

if "liked_tweet_id" not in st.session_state:
    st.session_state.liked_tweet_id = None

if "feed_tweets" not in st.session_state:
    st.session_state.feed_tweets = []

# Load tweets
all_tweets = load_all_tweets()

# App title
st.title("🟦 Live Tweet Recommender")

# ─── 1. User Selection ────────────────────────────────────────────────
st.subheader("👤 Select Your User")
users = sorted(set(tweet["user_id"] for tweet in all_tweets))
if not users:
    st.warning("No users found.")
    st.stop()

selected_user = st.selectbox("Choose a user:", users)
st.markdown(f"### You are viewing tweets from **@{selected_user}**")

# ─── 2. Live Feed ─────────────────────────────────────────────────────
st.subheader("✨ Live Feed")

if st.button("🔄 Refresh Feed"):
    st.session_state.feed_tweets = []

# Get 5 random tweets (once)
liked_ids = {tid for tid, _ in st.session_state.liked_tweets}
unliked_tweets = [t for t in all_tweets if t["tweet_id"] not in liked_ids and t["user_id"] != USER_ID]

print(liked_ids)

if not st.session_state.feed_tweets:
    st.session_state.feed_tweets = random.sample(unliked_tweets, min(5, len(unliked_tweets)))

# Display feed
for tweet in st.session_state.feed_tweets:
    tweet_store.add_tweet(tweet["tweet_id"], tweet)

    with st.container():
        cols = st.columns([10, 1])
        cols[0].markdown(f"**@{tweet['user_id']}**: {tweet['text']}")

        if tweet["tweet_id"] not in [tid for tid, _ in st.session_state.liked_tweets]:
            if cols[1].button("❤️", key=f"like_{tweet['tweet_id']}"):
                st.session_state.liked_tweet_id = tweet["tweet_id"]

# ─── Handle Like After UI ─────────────────────────────────────────────
if st.session_state.liked_tweet_id:
    tweet_id = st.session_state.liked_tweet_id
    matched = next((t for t in all_tweets if t["tweet_id"] == tweet_id), None)
    if matched and tweet_id not in [tid for tid, _ in st.session_state.liked_tweets]:
        st.session_state.liked_tweets.append((matched["tweet_id"], matched["user_id"]))
        st.session_state.just_liked = True
        logging.debug(f"Tweet liked: {matched['tweet_id']}")
        logging.debug("Updated liked tweets: %s", st.session_state.liked_tweets)
        print(f"Liked tweet: {matched['tweet_id']}")  # For testing
    st.session_state.liked_tweet_id = None

# ─── 3. Show User's Tweets ─────────────────────────────────────────────
st.subheader(f"📝 Tweets Posted by **@{selected_user}**")
user_tweets = [t for t in all_tweets if t["user_id"] == selected_user]
if user_tweets:
    for tweet in user_tweets:
        st.markdown(f"- **{tweet['tweet_id']}**: {tweet['text']}")
else:
    st.info(f"**@{selected_user}** hasn't posted any tweets.")

# ─── 4. Recommendations ───────────────────────────────────────────────
st.subheader("🔁 Recommended Users to Follow")

logging.debug("Liked Tweets: %s", st.session_state.liked_tweets)

if st.session_state.liked_tweets:
    last_liked_id, _ = st.session_state.liked_tweets[-1]
    print('recommneder:',last_liked_id)
    recommended_users = get_recommendations(tweet_id=last_liked_id, user_id=USER_ID, top_k=5)

    if recommended_users:
        for user in recommended_users:
            st.markdown(f"👤 **@{user}**")
    else:
        st.info("No recommendations found yet. Try liking more tweets.")
else:
    st.info("❤️ Like some tweets above to get personalized user recommendations.")
