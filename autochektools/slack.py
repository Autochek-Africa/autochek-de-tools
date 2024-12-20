import json
import logging
from datetime import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


class Notification:
    def __init__(self, message, status, pipeline_name, slack_token, user_mentions=None, timestamp=None):
        """
        Initialize the Notification object.
        
        :param message: The message content (string or dict)
        :param status: The status of the notification (success, warning, or error)
        :param pipeline_name: The name of the pipeline
        :param slack_token: The Slack bot token
        :param user_mentions: List of Slack user IDs to mention in the footer (optional)
        :param timestamp: The timestamp of the event (optional). Defaults to current time.
        """
        self.message = message
        self.status = status
        self.pipeline_name = pipeline_name
        self.timestamp = timestamp or datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        self.slack_client = WebClient(token=slack_token)
        self.user_mentions = user_mentions or []
        self.logger = logging.getLogger(__name__)
    
    def get_color(self):
        """
        Get the color based on the status.
        
        :return: Color code (string)
        """
        status_colors = {
            "success": "#36a64f",  # Green
            "warning": "#FFA500",  # Orange
            "error": "#FF0000",  # Red
        }
        return status_colors.get(self.status.lower(), "#808080")  # Default to gray
    
    def get_emoji(self):
        """
        Get the emoji/icon based on the status.
        
        :return: Emoji string
        """
        status_emojis = {
            "success": "✅",
            "warning": "⚠️",
            "error": "❌",
        }
        return status_emojis.get(self.status.lower(), "ℹ️")
    
    def format_message(self):
        """
        Format the message.
        
        :return: Formatted message as a string
        """
        if isinstance(self.message, dict):
            return json.dumps(self.message, indent=2)
        return self.message
    
    def get_footer(self):
        """
        Build the footer text with user mentions if the status is 'error'.
        
        :return: Footer string
        """
        footer = ":exclamation: Check and address the pipeline status as needed."
        if self.status.lower() == "error" and self.user_mentions:
            mentions = " ".join(f"<@{user_id}>" for user_id in self.user_mentions)
            footer = f"{footer} Notifying: {mentions}"
        return footer

    def format_table(self, data, headers=None):
        """
        Format a list of dictionaries (or list of lists) into a table-like string for Slack.
        Ensures proper column alignment using code block formatting.
        
        :param data: List of dictionaries (or list of lists) representing rows of the table.
        :param headers: Optional list of headers for the table.
        :return: A table-like formatted string wrapped in Slack code block for alignment.
        """
        if not data:
            return "*No data available.*"
        if isinstance(data[0], dict):
            headers = headers or list(data[0].keys())
            rows = [[str(row.get(h, '')) for h in headers] for row in data]
        elif isinstance(data[0], list):
            rows = data
        else:
            raise ValueError("Data should be a list of dictionaries or a list of lists")

        column_widths = [len(header) for header in headers]
        for row in rows:
            for i, cell in enumerate(row):
                column_widths[i] = max(column_widths[i], len(cell))

        def format_row(row):
            return " | ".join(cell.ljust(column_widths[i]) for i, cell in enumerate(row))

        table = []
        table.append(format_row(headers))
        table.append("-+-".join('-' * width for width in column_widths))
        for row in rows:
            table.append(format_row(row))

        return f"```\n" + "\n".join(table) + "\n```"

    def build_payload(self, data_table=None):
        """
        Build the payload in a Slack-like attachment format.
        Optionally includes a table-like message.
        
        :param data_table: Optional table data to include in the message.
        :return: Payload as a dictionary
        """
        formatted_message = self.format_message()
        if data_table:
            table_text = self.format_table(data_table)
            formatted_message += f"\n\n{table_text}"
        
        return {
            "text": f"{self.get_emoji()} *Pipeline Notification*",
            "attachments": [
                {
                    "color": self.get_color(),
                    "fields": [
                        {"title": "Pipeline Name", "value": self.pipeline_name, "short": True},
                        {"title": "Status", "value": self.status.capitalize(), "short": True},
                        {"title": "Message", "value": formatted_message, "short": False},
                        {"title": "Timestamp", "value": self.timestamp, "short": True},
                    ],
                    "footer": self.get_footer(),
                }
            ],
        }

    def send_to_slack(self, channel_id, data_table=None):
        """
        Send the notification to a Slack channel.
        Optionally includes a table-like message.
        
        :param channel_id: The Slack channel ID where the message will be posted.
        :param data_table: Optional table data to include in the message.
        """
        payload = self.build_payload(data_table=data_table)
        try:
            response = self.slack_client.chat_postMessage(
                channel=channel_id,
                text=payload["text"],
                attachments=payload["attachments"]
            )
            self.logger.info(f"Message sent successfully! Timestamp: {response['ts']}")
        except SlackApiError as e:
            self.logger.error(f"Failed to send message: {e.response['error']}")
