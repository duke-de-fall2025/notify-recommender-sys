import boto3
import json
import random
from datetime import datetime
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")
schedule_table = dynamodb.Table("notify_user_schedule")

MAX_NOTIFICATIONS = 5
NUM_TIMESLOTS = 24


def get_score(rec):
    """Safely extract similarity score regardless of key naming."""
    return (
        rec.get("similarity_score")
        or rec.get("similarity")
        or rec.get("score")
        or rec.get("similarityScore")
        or 0.0
    )


def lambda_handler(event, context):
    data = event if isinstance(event, dict) else json.loads(event)

    user_id = data["user_id"]
    recommendations = data["recommendations"]

    selected = recommendations[:MAX_NOTIFICATIONS]

    used_slots = set()
    today = datetime.utcnow().date().isoformat()

    schedule_list = []

    for rec in selected:
        available = [i for i in range(NUM_TIMESLOTS) if i not in used_slots]
        slot = random.choice(available)
        used_slots.add(slot)

        score = Decimal(str(get_score(rec)))  # convert float → Decimal

        schedule_list.append(
            {
                "campaign_id": rec.get("campaign_id")
                or rec.get("cid")
                or rec.get("id")
                or "UNKNOWN",
                "timeslot": Decimal(str(slot)),  # also must be Decimal
                "score": score,
            }
        )

    schedule_table.put_item(
        Item={"user_id": user_id, "date": today, "schedule": schedule_list}
    )

    return {
        "status": "saved",
        "user_id": user_id,
        "date": today,
        "schedule": schedule_list,
    }
