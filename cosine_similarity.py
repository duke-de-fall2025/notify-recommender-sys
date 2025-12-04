import boto3
import json
from decimal import Decimal

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

USERS_TABLE = "notify_users_test"
CAMPAIGNS_TABLE = "notify_campaigns_test"

users_table = dynamodb.Table(USERS_TABLE)
campaigns_table = dynamodb.Table(CAMPAIGNS_TABLE)


class DecimalEncoder(json.JSONEncoder):
    """Custom encoder to handle Decimal types from DynamoDB"""

    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def dot_product(vec1, vec2):
    """Calculate dot product manually"""
    try:
        return sum(float(a) * float(b) for a, b in zip(vec1, vec2))
    except (ValueError, TypeError) as e:
        raise Exception(
            f"Error in dot_product: {str(e)}, vec1={vec1[:5]}, vec2={vec2[:5]}"
        )


def magnitude(vec):
    """Calculate vector magnitude manually"""
    return sum(float(x) ** 2 for x in vec) ** 0.5


def cosine_similarity(vec1, vec2):
    dot = dot_product(vec1, vec2)
    mag1 = magnitude(vec1)
    mag2 = magnitude(vec2)

    if mag1 == 0 or mag2 == 0:
        return 0.0

    return dot / (mag1 * mag2)


def get_user_embedding(user_id):
    """Fetch user embedding from DynamoDB"""
    response = users_table.get_item(Key={"user_id": user_id})

    if "Item" not in response:
        raise Exception(f"User {user_id} not found")

    item = response["Item"]

    # Try different field names
    embedding = (
        item.get("user_embedding") or item.get("u_embeddings") or item.get("embedding")
    )

    if not embedding:
        raise Exception(f"No embedding for user {user_id}")

    # Convert all elements to float (handles both list and Decimal types from DynamoDB)
    embedding = [float(x) for x in embedding]

    return embedding


def get_all_campaigns():
    """Fetch all campaigns from DynamoDB"""
    campaigns = []
    response = campaigns_table.scan()

    campaigns.extend(response.get("Items", []))

    while "LastEvaluatedKey" in response:
        response = campaigns_table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        campaigns.extend(response.get("Items", []))

    return campaigns


def handler(event, context):
    try:
        data = event if "body" not in event else json.loads(event["body"])
        user_id = data.get("user_id")

        if not user_id:
            raise ValueError("Missing user_id")

        user_embedding = get_user_embedding(user_id)
        campaigns = get_all_campaigns()

        ranked = []
        for campaign in campaigns:
            campaign_id = campaign.get("campaign_id") or campaign.get("cid")
            campaign_embedding = (
                campaign.get("campaign_embedding")
                or campaign.get("c_embeddings")
                or campaign.get("embedding")
            )

            if not campaign_embedding:
                continue

            # Convert all elements to float
            campaign_embedding = [float(x) for x in campaign_embedding]

            similarity = cosine_similarity(user_embedding, campaign_embedding)

            ranked.append(
                {
                    "campaign_id": campaign_id,
                    "similarity_score": round(similarity, 4),
                    "name": campaign.get("campaign_name") or campaign.get("name"),
                    "description": campaign.get("description"),
                    "campaign_data": campaign,
                }
            )

        ranked = sorted(ranked, key=lambda x: x["similarity_score"], reverse=True)

        if not ranked:
            raise Exception("No campaigns available")

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "user_id": user_id,
                    "top_recommendation": ranked[0],
                    "top_10": ranked[:10],
                },
                cls=DecimalEncoder,
            ),
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}, cls=DecimalEncoder),
        }
