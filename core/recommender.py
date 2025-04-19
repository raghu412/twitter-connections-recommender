import sqlite3
from typing import List

def get_recommendations(tweet_id: str, user_id: str, top_k: int = 5) -> List[str]:
    query = """
        SELECT
            tweet_id_1, tweet_id_2,
            user_id_1, user_id_2,
            similarity
        FROM similarities
        WHERE tweet_id_1 = ? OR tweet_id_2 = ?
        ORDER BY similarity DESC
    """

    with sqlite3.connect("storage/database.db") as conn:
        cursor = conn.cursor()
        cursor.execute(query, (tweet_id, tweet_id))
        rows = cursor.fetchall()

    recommended_users = []
    for p1, p2, u1, u2, sim in rows:
        # Determine the recommended user and post owner
        if p1 == tweet_id:
            recommended_user = u2
            post_owner = u1
        else:
            recommended_user = u1
            post_owner = u2

        if recommended_user != user_id and post_owner != user_id:
            recommended_users.append((recommended_user, sim))

    seen = {}
    for uid, sim in recommended_users:
        if uid not in seen or sim > seen[uid]:
            seen[uid] = sim

    top_users = sorted(seen.items(), key=lambda x: x[1], reverse=True)[:top_k]
    return [uid for uid, _ in top_users]


if __name__ == "__main__":
    recs = get_recommendations(tweet_id="1745066698986", user_id =  "barbaralee", top_k=5)
    print("Recommended user IDs:", recs)
   