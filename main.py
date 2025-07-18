import requests
import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv
import re
import mysql.connector

# Load environment variables from .env file
load_dotenv()

# === CONFIG ===
SLACK_API_URL = os.getenv("SLACK_API_URL")
CHANNEL_ID = os.getenv("CHANNEL_ID")
TOKEN = os.getenv("TOKEN")
OUTPUT_FILE = os.getenv("OUTPUT_FILE")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", 3306))
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

def fetch_slack_messages(oldest=None):
    print(f"[{datetime.now()}] Fetching messages...")
    params = {
        "channel": CHANNEL_ID,
        "limit": 200  # Use a higher limit for efficiency
    }
    if oldest:
        params["oldest"] = oldest
    all_messages = []
    cursor = None

    while True:
        if cursor:
            params["cursor"] = cursor
        response = requests.get(SLACK_API_URL, headers=HEADERS, params=params)
        try:
            data = response.json()
        except Exception as e:
            print(f"Error decoding Slack response: {e}")
            break
        if not data.get("ok", True):
            print(f"Slack API error: {data.get('error', 'Unknown error')}")
            break
        messages = data.get("messages", [])
        all_messages.extend(messages)
        cursor = data.get("response_metadata", {}).get("next_cursor", None)
        if not cursor:
            break
    return all_messages

def extract_lead_from_message(msg):
    name = title = company = email = linkedin = location = None
    first_url = first_datetime = None
    profile_image = None
    website = industry = est_employees = est_revenue = None
    blocks = msg.get("blocks", [])
    for block in blocks:
        if block["type"] == "section" and "text" in block and block["text"]["type"] == "mrkdwn":
            text = block["text"]["text"]
            name_match = re.search(r"\*Name\*:\s*([^\n]+)", text)
            if name_match:
                name = name_match.group(1).strip()
            title_match = re.search(r"\*Title\*:\s*([^\n]+)", text)
            if title_match:
                title = title_match.group(1).strip()
            company_match = re.search(r"\*Company\*:\s*([^\n]+)", text)
            if company_match:
                company = company_match.group(1).strip()
            email_match = re.search(r"\*Email\*:\s*([^\n]+)", text)
            if email_match:
                email = email_match.group(1).strip()
            linkedin_match = re.search(r"\*LinkedIn\*:\s*<([^>]+)>", text)
            if linkedin_match:
                linkedin = linkedin_match.group(1).strip()
            location_match = re.search(r"\*Location\*:\s*([^\n]+)", text)
            if location_match:
                location = location_match.group(1).strip()
            if "accessory" in block and block["accessory"]["type"] == "image":
                profile_image = block["accessory"].get("image_url")
        elif block["type"] == "context":
            for el in block.get("elements", []):
                if el["type"] == "mrkdwn":
                    url_match = re.search(r"<([^>|]+)[^>]*>", el["text"])
                    if url_match:
                        first_url = url_match.group(1)
                    date_match = re.search(r"on \*([^*]+)\*", el["text"])
                    if date_match:
                        first_datetime = date_match.group(1)
        elif block["type"] == "section" and "fields" in block:
            for field in block["fields"]:
                ftext = field["text"]
                website_match = re.search(r"\*Website:\*\s*<([^>]+)>", ftext)
                if website_match:
                    website = website_match.group(1)
                industry_match = re.search(r"\*Industry:\*\s*([^\n]+)", ftext)
                if industry_match:
                    industry = industry_match.group(1).strip()
                employees_match = re.search(r"\*Est\. Employees:\*\s*([^\n]+)", ftext)
                if employees_match:
                    est_employees = employees_match.group(1).strip()
                revenue_match = re.search(r"\*Est\. Revenue:\*\s*([^\n]+)", ftext)
                if revenue_match:
                    est_revenue = revenue_match.group(1).strip()
    if not name and "text" in msg:
        text = msg["text"]
        name_match = re.search(r"Name:?\s*([^\n*]+)", text)
        if name_match:
            name = name_match.group(1).strip()
    return {
        "name": name,
        "title": title,
        "company": company,
        "email": email,
        "linkedin": linkedin,
        "location": location,
        "first_identified_url": first_url,
        "first_identified_datetime": first_datetime,
        "profile_image": profile_image,
        "website": website,
        "industry": industry,
        "est_employees": est_employees,
        "est_revenue": est_revenue
    }

def create_db_and_table():
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS leads (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            title VARCHAR(255),
            company VARCHAR(255),
            email VARCHAR(255),
            linkedin VARCHAR(255),
            location VARCHAR(255),
            first_identified_url TEXT,
            first_identified_datetime VARCHAR(255),
            profile_image TEXT,
            website VARCHAR(255),
            industry VARCHAR(255),
            est_employees VARCHAR(255),
            est_revenue VARCHAR(255),
            UNIQUE KEY unique_lead (name, company, email)
        )
    ''')
    # Create etl_progress table for incremental load tracking
    c.execute('''
        CREATE TABLE IF NOT EXISTS etl_progress (
            id INT PRIMARY KEY AUTO_INCREMENT,
            last_processed_ts VARCHAR(32)
        )
    ''')
    conn.commit()
    c.close()
    conn.close()

def get_last_processed_ts():
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    c = conn.cursor()
    c.execute('SELECT last_processed_ts FROM etl_progress ORDER BY id DESC LIMIT 1')
    row = c.fetchone()
    c.close()
    conn.close()
    if row and row[0]:
        return row[0]
    return None

def update_last_processed_ts(ts):
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    c = conn.cursor()
    c.execute('INSERT INTO etl_progress (last_processed_ts) VALUES (%s)', (ts,))
    conn.commit()
    c.close()
    conn.close()

def insert_lead(lead):
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    c = conn.cursor()
    try:
        c.execute('''
            INSERT IGNORE INTO leads (
                name, title, company, email, linkedin, location, first_identified_url, first_identified_datetime, profile_image, website, industry, est_employees, est_revenue
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            lead["name"], lead["title"], lead["company"], lead["email"], lead["linkedin"], lead["location"],
            lead["first_identified_url"], lead["first_identified_datetime"], lead["profile_image"], lead["website"],
            lead["industry"], lead["est_employees"], lead["est_revenue"]
        ))
        conn.commit()
    finally:
        c.close()
        conn.close()

def etl_job():
    create_db_and_table()
    last_ts = get_last_processed_ts()
    print(f"Last processed timestamp: {last_ts}")
    messages = fetch_slack_messages(oldest=last_ts)
    leads = []
    max_ts = last_ts
    for msg in messages:
        lead = extract_lead_from_message(msg)
        if lead["name"] and lead["company"]:
            insert_lead(lead)
            leads.append(lead)
        # Track the max timestamp for incremental load
        msg_ts = msg.get("ts")
        if msg_ts and (max_ts is None or float(msg_ts) > float(max_ts)):
            max_ts = msg_ts
    
    # Update progress table if new messages were processed
    if max_ts and max_ts != last_ts:
        update_last_processed_ts(max_ts)

# === RUN ETL JOB IMMEDIATELY ===
etl_job()

