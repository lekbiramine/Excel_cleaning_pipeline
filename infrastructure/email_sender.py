# 9
import os
import ssl
import smtplib
import io
import zipfile
from datetime import datetime
from pathlib import Path
from logging import Logger
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()


class EmailSender:
    """
    Sends files via email using SMTP with SSL.
    Infrastructure layer: external system communication.
    """

    def __init__(self, logger: Logger) -> None:
        self.logger = logger

        self.smtp_host = os.getenv("SMTP_HOST")
        self.smtp_port = os.getenv("SMTP_PORT", 465)
        self.sender_email = os.getenv("SENDER_EMAIL")
        self.sender_password = os.getenv("SENDER_PASSWORD")
        self.receiver_email = os.getenv("RECEIVER_EMAIL")
    
    # --------------------------------------------------

    def send(
        self,
        attachments: list[Path],
        subject: str,
        body: str,
        enable: bool = True
    ) -> None:
        """
        Send an email with attachments.

        attachments: list of Path objects
        subject: email subject
        body: email body text
        enable: toggle sending on/off
        """

        if not enable:
            self.logger.info("Email sending disabled.")
            return
        
        # Filter valid attachments
        files = [f for f in attachments if f and f.exists()]
        if not files:
            self.logger.warning("No valid attachments to send.")
            return
        
        msg = EmailMessage()
        msg["From"] = self.sender_email
        msg["To"] = self.receiver_email
        msg["Subject"] = subject
        msg.set_content(body)

        # Bundle all attachments into a single ZIP to reduce size
        zip_buffer = io.BytesIO()
        zip_name = f"pipeline_outputs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"

        with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for path in files:
                # Store file with just its name inside the zip
                zf.write(path, arcname=path.name)

        zip_bytes = zip_buffer.getvalue()

        # Hard safety cap to avoid hitting provider limits (~25MB for Gmail)
        max_bytes = 20 * 1024 * 1024  # 20 MB
        if len(zip_bytes) > max_bytes:
            self.logger.warning(
                f"Compressed attachments are too large to email "
                f"({len(zip_bytes) / (1024 * 1024):.2f} MB). Skipping email send."
            )
            return

        msg.add_attachment(
            zip_bytes,
            maintype="application",
            subtype="zip",
            filename=zip_name,
        )
        
        # Send via SSL
        context = ssl.create_default_context()
        try:
            with smtplib.SMTP_SSL(self.smtp_host, int(self.smtp_port), context=context) as server:
                server.login(self.sender_email, self.sender_password)
                server.send_message(msg)

            self.logger.info(f"Email sent successfully with {len(files)} attachment(s).")
        
        except Exception as e:
            self.logger.error(f"Failed to send email: {e}")