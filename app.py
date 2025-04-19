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

# Define constants
TWEET_FILE = "storage/live_feed.jsonl"
USER_ID = "barbaralee"

# Set up logging to log debug information to a file
logging.basicConfig(filename='debug_log.txt', level=logging.DEBUG, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load tweets from the file
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

# Initialize session state if not already set
if "liked_tweets" not in st.session_state:
    st.session_state.liked_tweets = []
if "just_liked" not in st.session_state:
    st.session_state.just_liked = False

# Load all tweets into memory
all_tweets = load_all_tweets()

# Set the app title
st.title("🟦 Live Tweet Recommender")

# ─── 1. User Selection ───────────────────────────────────────
st.subheader("👤 Select Your User")
users = sorted(set(tweet["user_id"] for tweet in all_tweets))
if not users:
    st.warning("No users found.")
    st.stop()

selected_user = st.selectbox("Choose a user:", users)
st.markdown(f"### You are viewing tweets from **@{selected_user}**")

# ─── 2. Live Feed ─────────────────────────────────────────────
st.subheader("✨ Live Feed")

# Button to refresh feed (reloads the component)
if st.button("🔄 Refresh Feed"):
    st.experimental_rerun()

# Filter out already liked tweets
liked_ids = {tid for tid, _ in st.session_state.liked_tweets}
unliked_tweets = [t for t in all_tweets if t["tweet_id"] not in liked_ids and t["user_id"] != USER_ID]

# Select 5 random unliked tweets to display
feed_tweets = random.sample(unliked_tweets, min(5, len(unliked_tweets)))

# Loop through the selected tweets and display them
for tweet in feed_tweets:
    tweet_store.add_tweet(tweet["tweet_id"], tweet)  # Store the tweet for downstream use
    
    with st.container():
        cols = st.columns([10, 1])
        cols[0].markdown(f"**@{tweet['user_id']}**: {tweet['text']}")
        
        # Check if tweet is already liked before allowing to like again
        if tweet["tweet_id"] not in [liked_tweet[0] for liked_tweet in st.session_state.liked_tweets]:
            if cols[1].button("❤️", key=f"like_{tweet['tweet_id']}"):
                st.session_state.liked_tweets.append((tweet["tweet_id"], tweet["user_id"]))
                st.session_state.just_liked = True
                # Log the change in liked tweets (Debug)
                logging.debug(f"Tweet liked: {tweet['tweet_id']}")
                logging.debug("Updated liked tweets: %s", st.session_state.liked_tweets)

# ─── 3. Show User's Tweets ─────────────────────────────────────
st.subheader(f"📝 Tweets Posted by **@{selected_user}**")
user_tweets = [t for t in all_tweets if t["user_id"] == selected_user]
if user_tweets:
    for tweet in user_tweets:
        st.markdown(f"- **{tweet['tweet_id']}**: {tweet['text']}")
else:
    st.info(f"**@{selected_user}** hasn't posted any tweets.")

# ─── 4. Recommendations ────────────────────────────────────────
st.subheader("🔁 Recommended Users to Follow")

# Log the liked tweets state before displaying recommendations
logging.debug("Liked Tweets: %s", st.session_state.liked_tweets)

# Show recommendations only if the user has liked any tweet
if st.session_state.liked_tweets:
    last_liked_id, _ = st.session_state.liked_tweets[-1]
    recommended_users = get_recommendations(tweet_id=last_liked_id, user_id=USER_ID, top_k=5)

    if recommended_users:
        for user in recommended_users:
            st.markdown(f"👤 **@{user}**")
    else:
        st.info("No recommendations found yet. Try liking more tweets.")
else:
    st.info("❤️ Like some tweets above to get personalized user recommendations.")
