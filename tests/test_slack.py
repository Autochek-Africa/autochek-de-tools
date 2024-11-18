from autochektools.slack import Notification


import unittest
from unittest.mock import patch, MagicMock
from autochektools.slack import Notification
from slack_sdk.errors import SlackApiError


class TestSlackNotification(unittest.TestCase):
    def setUp(self):
        self.slack_token = "fake-slack-token"
        self.channel_id = "test-channel"
        self.pipeline_name = "Test Pipeline"
        self.user_mentions = ["U12345", "U67890"]

    def test_initialization(self):
        """Test initialization of the Notification class."""
        notification = Notification(
            message="Test message",
            status="success",
            pipeline_name=self.pipeline_name,
            slack_token=self.slack_token
        )
        self.assertEqual(notification.status, "success")
        self.assertEqual(notification.pipeline_name, self.pipeline_name)
        self.assertEqual(notification.user_mentions, [])
        self.assertIsNotNone(notification.timestamp)

    @patch("autochektools.slack.WebClient")
    def test_send_to_slack_success(self, MockWebClient):
        """Test successful Slack message sending."""
        mock_client = MockWebClient.return_value
        mock_client.chat_postMessage.return_value = {"ts": "1234567890.123456"}

        notification = Notification(
            message="Test message",
            status="success",
            pipeline_name=self.pipeline_name,
            slack_token=self.slack_token
        )
        notification.send_to_slack(self.channel_id)
        mock_client.chat_postMessage.assert_called_once()

    @patch("autochektools.slack.WebClient")
    def test_send_to_slack_failure(self, MockWebClient):
        """Test failed Slack message sending."""
        mock_client = MockWebClient.return_value
        mock_client.chat_postMessage.side_effect = SlackApiError("test_error", {"error": "invalid_auth"})

        notification = Notification(
            message="Test message",
            status="error",
            pipeline_name=self.pipeline_name,
            slack_token=self.slack_token
        )
        with self.assertLogs(notification.logger, level="ERROR") as log:
            notification.send_to_slack(self.channel_id)
            self.assertIn("Failed to send message: invalid_auth", log.output[0])


if __name__ == "__main__":
    unittest.main()