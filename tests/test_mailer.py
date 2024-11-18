import unittest
from unittest.mock import patch, MagicMock
from autochektools.mailer import Mail
import pandas as pd
import smtplib


class TestMail(unittest.TestCase):

    @patch("google.cloud.storage.Client")
    @patch("smtplib.SMTP_SSL")
    def test_send_mail_with_gcs_attachments_failure(self, mock_smtp, mock_storage_client):
        mock_smtp_instance = MagicMock()
        mock_smtp.return_value = mock_smtp_instance
        mock_storage_instance = MagicMock()
        mock_storage_client.return_value = mock_storage_instance
        mock_blob = MagicMock()
        mock_storage_instance.bucket.return_value.blob.return_value = mock_blob

        mailer = Mail(username="test@example.com", password="password", host="smtp.example.com", port=465)

        recipients = ["recipient@example.com"]
        subject = "Test Subject"
        body = "Test Body"
        bucket_name = "test-bucket"
        filenames = ["test_file.txt"]

        mock_blob.download_to_filename.side_effect = Exception("Failed to download file")

        result = mailer.send_mail_with_gcs_attachments(recipients, subject, body, bucket_name, filenames)

        self.assertEqual(result["success"], False)
        self.assertEqual(result["error"], "Failed to download file")

if __name__ == "__main__":
    unittest.main()
