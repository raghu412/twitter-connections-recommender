import nest_asyncio
nest_asyncio.apply()


import streamlit as st
from core.data_gen import tweet_generator
from core.embedding import get_model
from core.indexer import get_index, add_to_index
from core.recommender import get_recommendations
import os
os.environ["STREAMLIT_SERVER_ENABLE_FILE_WATCHER"] = "false"


st.set_page_config(page_title="Live Tweet Recommender", layout="wide")

model = get_model()
index = get_index(model)

tweet_gen = tweet_generator() 

if "liked" not in st.session_state:
    st.session_state.liked = []

st.title("🟦 Live Tweet Recommender")

st.markdown("### ✨ Live Feed")
for _ in range(5):
    t = next(tweet_gen)  # Get the next tweet from the generator
    fid, emb = add_to_index(t, model, index)
    with st.container():
        cols = st.columns([10, 1])
        cols[0].markdown(f"**@{t['user_id']}**: {t['text']}")
        if cols[1].button("❤️", key=f"like_{fid}"):
            st.session_state.liked.append((fid, emb))
            print(f"Liked tweets: {st.session_state.liked}")  # Debug statement

st.markdown("### 🔁 Recommended for You")
if st.session_state.liked:
    recs = get_recommendations(st.session_state.liked, index)
    print(f"Recommendations: {recs}")  # Debug statement
    for r in recs:
        st.markdown(f"📌 **@{r['user']}**: {r['text']}")
else:
    st.info("Like some tweets above to see recommendations!")
