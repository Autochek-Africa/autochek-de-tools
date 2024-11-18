from io import BytesIO
import os
import smtplib
import ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import pandas as pd
from google.cloud import storage


class Mail:
    """
    A utility class for sending emails with support for attachments and HTML content.
    """

    def __init__(self, username: str, password: str, host: str, port: int) -> None:
        """
        Initialize the Mail class with SMTP server details.

        Args:
            username (str): Email username for SMTP authentication.
            password (str): Password for SMTP authentication.
            host (str): SMTP server host.
            port (int): SMTP server port.
        """
        self.username = username
        self.password = password
        self.host = host
        self.port = port

    def _create_message(
        self,
        recipients: list,
        subject: str,
        body: str,
        html: str = None,
        attachments: list = None,
    ) -> MIMEMultipart:
        """
        Create an email message with optional HTML and attachments.

        Args:
            recipients (list): List of recipient email addresses.
            subject (str): Email subject.
            body (str): Plain text email body.
            html (str, optional): HTML content for the email body.
            attachments (list, optional): List of file paths to attach.

        Returns:
            MIMEMultipart: The constructed email message.
        """
        message = MIMEMultipart()
        message["From"] = self.username
        message["To"] = ", ".join(recipients)
        message["Subject"] = subject

        message.attach(MIMEText(body, "plain"))

        if html:
            message.attach(MIMEText(html, "html"))

        if attachments:
            for file_path in attachments:
                with open(file_path, "rb") as file:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(file.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f'attachment; filename="{os.path.basename(file_path)}"',
                )
                message.attach(part)

        return message

    def send_mail(
        self,
        recipients: list,
        subject: str,
        body: str,
        html: str = None,
        attachments: list = None,
    ) -> dict:
        """
        Send an email.

        Args:
            recipients (list): List of recipient email addresses.
            subject (str): Email subject.
            body (str): Plain text email body.
            html (str, optional): HTML content for the email body.
            attachments (list, optional): List of file paths to attach.

        Returns:
            dict: A dictionary with the result of the operation.
        """
        context = ssl.create_default_context()
        message = self._create_message(recipients, subject, body, html, attachments)

        try:
            with smtplib.SMTP_SSL(self.host, self.port, context=context) as server:
                server.login(self.username, self.password)
                server.sendmail(self.username, recipients, message.as_string())
            return {"success": True, "message": "Email sent successfully"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def send_mail_with_gcs_attachments(
        self,
        recipients: list,
        subject: str,
        body: str,
        bucket_name: str,
        filenames: list,
        html: str = None,
    ) -> dict:
        """
        Send an email with attachments fetched from a Google Cloud Storage bucket.

        Args:
            recipients (list): List of recipient email addresses.
            subject (str): Email subject.
            body (str): Plain text email body.
            bucket_name (str): Name of the GCS bucket.
            filenames (list): List of filenames in the GCS bucket to attach.
            html (str, optional): HTML content for the email body.

        Returns:
            dict: A dictionary with the result of the operation.
        """
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        local_temp_files = []

        try:
            for filename in filenames:
                local_file = f"temp_{filename}"
                blob = bucket.blob(filename)
                blob.download_to_filename(local_file)
                local_temp_files.append(local_file)

            return self.send_mail(
                recipients, subject, body, html, attachments=local_temp_files
            )
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            for temp_file in local_temp_files:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    @staticmethod
    def df_to_html(df: pd.DataFrame) -> str:
        """
        Convert a pandas DataFrame to an HTML table.

        Args:
            df (pd.DataFrame): The DataFrame to convert.

        Returns:
            str: HTML representation of the DataFrame.
        """
        html = df.to_html(classes="table table-striped", index=False)
        return f"""\
        <html>
            <head>
                <style>
                table, th, td {{
                    border: 1px solid black;
                    border-collapse: collapse;
                }}
                th, td {{
                    padding: 5px;
                    text-align: left;
                }}  
                </style>
            </head>
            <body>
                {html}
            </body>
        </html>
        """

    @staticmethod
    def xero_responses_to_html(responses: list) -> str:
        """
        Convert a list of response dictionaries to an HTML table.

        Args:
            responses (list): List of dictionaries representing responses.

        Returns:
            str: HTML representation of the responses.
        """
        rows = []
        for response in responses:
            rows.append(
                {
                    "Message": response.get("message"),
                    "Code": response.get("code"),
                    "Last Successful Timestamp": response.get("obj", {}).get(
                        "lastSuccessfulRunTimestamp"
                    ),
                    "Last Run Error Count": response.get("obj", {}).get(
                        "lastRunErrorCount"
                    ),
                    "Last Run Successful": response.get("obj", {}).get(
                        "lastRunSuccessful"
                    ),
                    "Last Error Category": response.get("obj", {}).get(
                        "lastRunErrors", [{}]
                    )[-1].get("category", ""),
                    "Last Error Text": response.get("obj", {}).get(
                        "lastRunErrors", [{}]
                    )[-1].get("text", ""),
                }
            )

        df = pd.DataFrame(rows)
        return Mail.df_to_html(df)
    

    def send_mail_with_excel(
        self,
        recipients: list,
        subject: str,
        body: str,
        dataframes: dict,
        excel_filename: str = "data.xlsx",
        html: str = None,
    ) -> dict:
        """
        Send an email with an Excel file attachment where each DataFrame is a worksheet.

        Args:
            recipients (list): List of recipient email addresses.
            subject (str): Email subject.
            body (str): Plain text email body.
            dataframes (dict): Dictionary where keys are sheet names and values are pandas DataFrames.
            excel_filename (str): Name of the Excel file attachment.
            html (str, optional): HTML content for the email body.

        Returns:
            dict: A dictionary with the result of the operation.
        """
        excel_file = BytesIO()
        temp_filepath = None
        try:
            with pd.ExcelWriter(excel_file, engine="xlsxwriter") as writer:
                for sheet_name, dataframe in dataframes.items():
                    dataframe.to_excel(writer, index=False, sheet_name=sheet_name)
            excel_file.seek(0)

            # Save the file temporarily and capture the file path
            temp_filepath = self._save_excel_to_temp(excel_file, excel_filename)
            
            # Send the email
            result = self.send_mail(
                recipients, subject, body, html, attachments=[temp_filepath]
            )

            return result
        except Exception as e:
            return {"success": False, "error": f"Failed to create/send Excel file: {str(e)}"}
        finally:
            # Delete the temporary file after sending email
            if temp_filepath and os.path.exists(temp_filepath):
                try:
                    os.remove(temp_filepath)
                except Exception as delete_error:
                    print(f"Failed to delete temporary file: {str(delete_error)}")
            
            excel_file.close()


    @staticmethod
    def _save_excel_to_temp(excel_file: BytesIO, filename: str) -> str:
        """
        Save the in-memory Excel file to a temporary file.

        Args:
            excel_file (BytesIO): The in-memory Excel file.
            filename (str): Desired filename for the temporary file.

        Returns:
            str: Path to the saved temporary file.
        """
        temp_path = os.path.join(os.getcwd(), filename)
        with open(temp_path, "wb") as temp_file:
            temp_file.write(excel_file.read())
        return temp_path
