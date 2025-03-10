# Dropship Order Processing System

This project automates the process of fetching order files from an FTP server, validating them, extracting purchase order details, and storing them in a database.

## Features
- Connects to an FTP server to download order files.
- Validates order files using predefined rules.
- Parses and standardizes order data.
- Stores order details in an Azure SQL database.
- Sends email notifications for errors or important updates.
- Moves processed files to appropriate FTP folders.

## Project Structure
project_root/ ├── config.py # Configuration file for database & FTP credentials ├── dropship_db.py # Handles database interactions ├── email_helper.py # Sends email notifications ├── ftp.py # Manages FTP file transfers ├── invalid_file_checker.py # Validates order files ├── main.py # Main script orchestrating the process ├── xlsx_parser.py # Parses Excel/CSV order files

bash
Copy
Edit

## Installation & Setup

### 1. Clone the Repository
git clone https://github.com/your-repo/dropship-processing.git
cd dropship-processing

### 2. Install Dependencies
Ensure you have Python 3 installed, then install dependencies:
pip install -r requirements.txt

### 3. Configure the System
Modify config.py with your database and FTP credentials.

## Example database configuration:
db_config = {
    "ExampleDb": {
        "server": "your.database.windows.net",
        "database": "YourDB",
        "username": "your_user",
        "password": "your_password",
        "driver": "{ODBC Driver 17 for SQL Server}",
    },
}

## Example email configuration:

SENDER_EMAIL = "your_email@example.com"
SENDER_PASSWORD = "your_email_password"

## Usage

Run the main script to start the process:
python main.py

## How It Works

- The script connects to an FTP server and downloads order files.
- It checks for duplicates, correct formatting, and required fields.
- Valid orders are stored in an Azure SQL database.
- Processed files are moved to respective FTP folders.
- If an issue occurs, an email notification is sent.

## Tech Stack

- Python 3
- Azure SQL Database (via pyodbc)
- FTP File Handling (ftplib)
- Email Notifications (smtplib)
- Pandas for data parsing
- TQDM for progress tracking

## Troubleshooting

- If you encounter a database connection issue, verify that ODBC Driver 17 is installed.
- If emails fail to send, ensure you have enabled "Less Secure Apps" in your email settings.

