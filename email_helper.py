import smtplib
from email.message import EmailMessage
from config import SENDER_EMAIL, SENDER_PASSWORD, RECIPIENT_EMAILS
import os
import getpass
import socket


def send_email(subject, body):
    current_dir = os.getcwd()
    folder_name = os.path.basename(current_dir)
    computer_name = socket.gethostname()
    user_name = getpass.getuser()
    new_line = "\n"
    body_with_new_line = (
        f"{body}{new_line}{folder_name} on {computer_name} ({user_name})"
    )
    msg = EmailMessage()
    msg.set_content(body_with_new_line)
    msg["Subject"] = f"{subject} : {folder_name}"
    msg["From"] = SENDER_EMAIL
    msg["To"] = ", ".join(RECIPIENT_EMAILS)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {e}")
