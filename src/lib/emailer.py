from __future__ import annotations

import asyncio
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

GMAIL_USER = os.getenv("GMAIL_USER")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")


def _send_email_sync(to_addr: str, subject: str, html_body: str, attachment_path: Path | None = None) -> None:
    if not GMAIL_USER or not GMAIL_APP_PASSWORD:
        raise RuntimeError("Missing GMAIL_USER or GMAIL_APP_PASSWORD environment variables")

    msg = MIMEMultipart()
    msg["From"] = GMAIL_USER
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    if attachment_path and attachment_path.exists():
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment_path.read_bytes())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename={attachment_path.name}",
        )
        msg.attach(part)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        server.sendmail(GMAIL_USER, [to_addr], msg.as_string())


async def send_result_email(to_addr: str, batch_id: str, csv_path: Path) -> None:
    subject = f"SignalHire Results Ready – {batch_id}"
    html = f"""
    <html><body>
      <h3>SignalHire Results Ready</h3>
      <p>Your batch <b>{batch_id}</b> has completed. The CSV is attached.</p>
      <p>Thank you,<br/>Gary Maus</p>
    </body></html>
    """
    await asyncio.to_thread(_send_email_sync, to_addr, subject, html, csv_path)


async def send_error_email(to_addr: str, batch_id: str, error_msg: str) -> None:
    subject = f"SignalHire Processing Error – {batch_id}"
    html = f"""
    <html><body>
      <h3>SignalHire Processing Error</h3>
      <p>Batch <b>{batch_id}</b> encountered an error:</p>
      <pre>{error_msg}</pre>
      <p>Please try again later or contact support.</p>
    </body></html>
    """
    await asyncio.to_thread(_send_email_sync, to_addr, subject, html, None)
