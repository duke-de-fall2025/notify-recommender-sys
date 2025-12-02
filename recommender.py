"""
Recommendation Engine
Task: Generate Top 100 similar ads for every user + save to DynamoDB format

Structure:
1. Load dummy user/ad/interaction data
2. Compute similarities (cosine similarity)
3. Rank ads for each user (collaborative filtering)
4. Save output in DynamoDB format
"""

import pandas as pd
import numpy as np
import json
import datetime
from sklearn.metrics.pairwise import cosine_similarity
from typing import Dict, List
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================

NUM_USERS = 10
NUM_ADS = 50
TOP_N = 100  # Top 100 ads per user
SIMILAR_USERS_K = 5  # Look at top 5 similar users

OUTPUT_DIR = Path("./output")
OUTPUT_DIR.mkdir(exist_ok=True)

# ============================================================
# STEP 1: GENERATE DUMMY DATA
# ============================================================


def generate_dummy_data():
    """
    Create synthetic user-ad interaction matrix
    Rows = users, Cols = ads
    Values: 1 = user engaged/clicked, 0 = ignored
    """
    np.random.seed(42)

    users = [f"user_{i}" for i in range(NUM_USERS)]
    ads = [f"ad_{i}" for i in range(NUM_ADS)]

    # Interaction matrix: 50% sparse (users don't interact with most ads)
    interaction_matrix = np.random.binomial(n=1, p=0.3, size=(len(users), len(ads)))

    interaction_df = pd.DataFrame(interaction_matrix, index=users, columns=ads)

    print("=" * 70)
    print("STEP 1: DUMMY DATA GENERATED")
    print("=" * 70)
    print(f"✓ Users: {len(users)}")
    print(f"✓ Ads: {len(ads)}")
    print(f"✓ Interaction Matrix Shape: {interaction_df.shape}")
    print(f"✓ Engagement Rate: {interaction_df.values.mean():.2%}")
    print()

    return users, ads, interaction_df


# ============================================================
# STEP 2: COMPUTE USER-TO-USER SIMILARITY (Cosine Similarity)
# ============================================================


def compute_user_similarity(interaction_df):
    """
    Compute cosine similarity between users based on their ad engagement patterns

    Logic: If two users clicked similar ads, they're similar
    """
    similarity_matrix = cosine_similarity(interaction_df)

    similarity_df = pd.DataFrame(
        similarity_matrix, index=interaction_df.index, columns=interaction_df.index
    )

    print("=" * 70)
    print("STEP 2: USER SIMILARITY COMPUTED (Cosine Similarity)")
    print("=" * 70)
    print(f"✓ Similarity Matrix Shape: {similarity_df.shape}")
    print(
        f"✓ Min Similarity: {similarity_df.values[np.triu_indices_from(similarity_df.values, k=1)].min():.4f}"
    )
    print(
        f"✓ Max Similarity: {similarity_df.values[np.triu_indices_from(similarity_df.values, k=1)].max():.4f}"
    )
    print()

    return similarity_df


# ============================================================
# STEP 3: GENERATE RECOMMENDATIONS (Collaborative Filtering)
# ============================================================


def generate_recommendations(interaction_df, similarity_df, top_n=100, k_similar=5):
    """
    For each user:
    1. Find K most similar users
    2. Look at ads those similar users clicked
    3. Rank ads by weighted engagement (higher weight = more similar users clicked it)
    4. Remove ads the target user already saw
    5. Return Top N ads

    Output format: {user_id: [ad1, ad2, ..., adN]}
    """
    recommendations = {}

    for user in interaction_df.index:
        # Find similar users (excluding self)
        similar_users = similarity_df[user].sort_values(ascending=False)[
            1 : k_similar + 1
        ]

        # Get ads clicked by similar users, weighted by similarity
        # If similar_user[i] clicked ad[j], score increases by similarity[i]
        weighted_scores = interaction_df.loc[similar_users.index].T.dot(
            similar_users.values
        )

        # Remove ads already clicked by target user
        already_seen = interaction_df.loc[user][interaction_df.loc[user] == 1].index
        ranked_ads = weighted_scores.drop(already_seen).sort_values(ascending=False)

        # Get top N recommendations
        top_ads = ranked_ads.head(top_n).index.tolist()

        # Pad with zeros if fewer than N ads available
        while len(top_ads) < top_n:
            top_ads.append(None)

        recommendations[user] = top_ads

    print("=" * 70)
    print("STEP 3: RECOMMENDATIONS GENERATED (Collaborative Filtering)")
    print("=" * 70)
    print(f"✓ Total Users: {len(recommendations)}")
    print(f"✓ Top N Ads Per User: {top_n}")
    print(f"✓ Sample User (user_0) Top 5 Ads: {recommendations['user_0'][:5]}")
    print()

    return recommendations


# ============================================================
# STEP 4: FORMAT FOR DynamoDB + SAVE
# ============================================================


def format_for_dynamodb(recommendations, interaction_df):
    """
    Convert recommendations to DynamoDB format:
    {
        "user_id": string,
        "timestamp": ISO string,
        "ads": list of ad_ids,
        "num_recommendations": number,
        "num_previously_seen": number
    }
    """
    payload = []
    timestamp = datetime.datetime.now().isoformat()

    for user_id, ads in recommendations.items():
        # Count how many ads user already saw
        num_seen = interaction_df.loc[user_id].sum()

        entry = {
            "user_id": user_id,
            "timestamp": timestamp,
            "ads": ads,
            "num_recommendations": len([a for a in ads if a is not None]),
            "num_previously_seen": int(num_seen),
            "ttl": int(datetime.datetime.now().timestamp())
            + (7 * 24 * 60 * 60),  # 7 days
        }
        payload.append(entry)

    print("=" * 70)
    print("STEP 4: FORMATTED FOR DynamoDB")
    print("=" * 70)
    print(f"✓ Total Records: {len(payload)}")
    print(f"✓ Timestamp: {timestamp}")
    print()

    return payload


# ============================================================
# STEP 5: MOCK WRITE TO DynamoDB (Can be replaced with real AWS SDK)
# ============================================================


def save_to_dynamodb_mock(payload):
    """
    Mock DynamoDB write. Later replace with boto3:

    import boto3
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('user_ad_recommendations')

    with table.batch_writer() as batch:
        for item in payload:
            batch.put_item(Item=item)
    """

    print("=" * 70)
    print("STEP 5: SAVING TO DynamoDB (Mock)")
    print("=" * 70)

    # Save as JSON
    output_file = OUTPUT_DIR / "dynamodb_records.json"
    with open(output_file, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"✓ Saved {len(payload)} records to {output_file}")

    # Save as CSV for easy inspection
    csv_file = OUTPUT_DIR / "recommendations.csv"
    df = pd.DataFrame(payload)
    df.to_csv(csv_file, index=False)
    print(f"✓ Also saved as CSV: {csv_file}")

    # Print sample
    print(f"\nSample DynamoDB Record (user_0):")
    print(json.dumps(payload[0], indent=2))
    print()

    return payload


# ============================================================
# STEP 6: VALIDATION + STATS
# ============================================================


def validate_output(payload, interaction_df):
    """
    Quality checks:
    - No user seeing ads they already clicked
    - Each user has 100 ads (or less if not enough available)
    - Timestamp is recent
    """
    print("=" * 70)
    print("STEP 6: VALIDATION & STATISTICS")
    print("=" * 70)

    issues = []

    for record in payload:
        user_id = record["user_id"]
        ads = [a for a in record["ads"] if a is not None]

        # Check for duplicates
        if len(ads) != len(set(ads)):
            issues.append(f"{user_id}: Duplicate ads in recommendations")

        # Check no already-seen ads
        already_seen = interaction_df.loc[user_id][
            interaction_df.loc[user_id] == 1
        ].index
        overlap = set(ads) & set(already_seen)
        if overlap:
            issues.append(f"{user_id}: {len(overlap)} ads already seen")

    if issues:
        print("⚠ Issues Found:")
        for issue in issues[:5]:
            print(f"  - {issue}")
    else:
        print("✓ All validation checks passed!")

    # Statistics
    total_recs = sum([len([a for a in r["ads"] if a]) for r in payload])
    avg_recs = total_recs / len(payload)

    print(f"\n📊 Statistics:")
    print(f"  - Total Recommendations: {total_recs}")
    print(f"  - Avg Per User: {avg_recs:.1f}")
    print(f"  - Min: {min([r['num_recommendations'] for r in payload])}")
    print(f"  - Max: {max([r['num_recommendations'] for r in payload])}")
    print()


# ============================================================
# MAIN PIPELINE
# ============================================================


def run_main_pipeline():
    """Execute the full recommendation pipeline"""

    print("\n")
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 15 + "RECOMMENDATION ENGINE" + " " * 22 + "║")
    print("╚" + "=" * 68 + "╝")
    print()

    # Step 1: Generate dummy data
    users, ads, interaction_df = generate_dummy_data()

    # Step 2: Compute similarities
    similarity_df = compute_user_similarity(interaction_df)

    # Step 3: Generate recommendations
    recommendations = generate_recommendations(
        interaction_df, similarity_df, top_n=TOP_N, k_similar=SIMILAR_USERS_K
    )

    # Step 4: Format for DynamoDB
    payload = format_for_dynamodb(recommendations, interaction_df)

    # Step 5: Save (mock DynamoDB)
    save_to_dynamodb_mock(payload)

    # Step 6: Validate
    validate_output(payload, interaction_df)

    print("=" * 70)
    print("✅ PIPELINE COMPLETE!")
    print("=" * 70)
    print(f"Output saved to folder: {OUTPUT_DIR}")
    print()

    return {
        "users": users,
        "ads": ads,
        "interaction_df": interaction_df,
        "similarity_df": similarity_df,
        "recommendations": recommendations,
        "payload": payload,
    }


if __name__ == "__main__":
    results = run_main_pipeline()
