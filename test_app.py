import sys
import types
import unittest
from unittest.mock import patch, MagicMock
from io import BytesIO
from PIL import Image
import streamlit as st

# -------------------------------
# Step 1: Pre-create a fake kafka module
# -------------------------------
fake_kafka = types.ModuleType("kafka")
fake_kafka.KafkaProducer = MagicMock(return_value=MagicMock())
fake_kafka.KafkaConsumer = MagicMock(return_value=MagicMock())
sys.modules['kafka'] = fake_kafka

fake_boto3 = types.ModuleType("boto3")
fake_boto3.resource = MagicMock()
fake_boto3.client = MagicMock()

fake_dynamodb = MagicMock()
fake_table = MagicMock()

# prevent table.scan() from calling AWS
fake_table.scan.return_value = {"Items": []}
fake_dynamodb.Table.return_value = fake_table
fake_boto3.resource.return_value = fake_dynamodb

sys.modules["boto3"] = fake_boto3

# -------------------------------
# Step 2: Import app safely
# -------------------------------
from app import load_image_from_s3, create_view_dialog, check_user_alerts, FALLBACK_IMAGE

# -------------------------------
# Step 3: Define your unittest class
# -------------------------------
class TestStreamlitApp(unittest.TestCase):

    @patch("app.s3_client.get_object")
    def test_load_image_from_s3_success(self, mock_get_object):
        img_bytes = BytesIO()
        Image.new("RGB", (10, 10)).save(img_bytes, format="JPEG")
        img_bytes.seek(0)
        mock_get_object.return_value = {"Body": img_bytes}

        img = load_image_from_s3("s3://bucket/test.jpg")
        self.assertIsInstance(img, Image.Image)
        mock_get_object.assert_called_once()

    @patch("app.s3_client.get_object")
    def test_load_image_from_s3_fallback(self, mock_get_object):
        # First call fails, second call returns a valid fallback image
        img_bytes = BytesIO()
        Image.new("RGB", (10, 10)).save(img_bytes, format="JPEG")
        img_bytes.seek(0)
        mock_get_object.side_effect = [Exception("fail"), {"Body": img_bytes}]

        with patch("builtins.print") as mock_print:
            img = load_image_from_s3("s3://bucket/invalid.jpg")
            mock_print.assert_called()  # Should print fallback message
            self.assertIsInstance(img, Image.Image)  # Should return an Image object

    @patch("app.dynamodb")
    def test_check_user_alerts_no_item(self, mock_dynamodb):
        mock_table = MagicMock()
        mock_table.get_item.return_value = {}
        mock_dynamodb.Table.return_value = mock_table

        notification = check_user_alerts("U0023")
        self.assertIsInstance(notification, str)


    def test_kafka_producer_mocked(self):
        """Test that KafkaProducer is mocked and no Kafka connection is required."""
        from app import producer
        self.assertIsInstance(producer, MagicMock)
        producer.send("clickstream", {"event": "test"})
        producer.send.assert_called_with("clickstream", {"event": "test"})


if __name__ == "__main__":
    unittest.main()
