import requests
import json
import time
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import re
import mysql.connector
import logging

# Set up logs folder and file
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "etl_job.log"),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logging.info("ETL Script started")
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

# Base44 API Configuration
BASE44_API_URL = os.getenv("BASE44_API_URL", "https://preview--rb2b-scaleup-ally-e78ad0c9.base44.app/api/")
BASE44_API_KEY = os.getenv("BASE44_API_KEY", "460e332ba24f491d99ba6e76d87f8bd0")

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

def fetch_slack_messages(oldest=None):
    logging.info(f"[{datetime.now()}] Fetching messages...")
    
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
            logging.error(f"Error decoding Slack response: {e}")
            break
        if not data.get("ok", True):
            logging.error(f"Slack API error: {data.get('error', 'Unknown error')}")
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
    message_date_time = None
    
    # Extract timestamp from message and convert to UTC datetime
    ts = msg.get("ts")
    if ts:
        try:
            # Get first 10 digits before decimal point (Unix timestamp)
            unix_timestamp = int(float(ts))
            # Convert to UTC datetime
            message_date_time = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
        except (ValueError, TypeError) as e:
            logging.warning(f"Failed to parse timestamp {ts}: {str(e)}")
            message_date_time = None
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
        "est_revenue": est_revenue,
        "message_date_time": message_date_time
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
    
    # Check if message_date_time column exists, if not create it
    c.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = %s 
        AND TABLE_NAME = 'leads' 
        AND COLUMN_NAME = 'message_date_time'
    """, (MYSQL_DATABASE,))
    
    column_exists = c.fetchone()[0]
    if column_exists == 0:
        c.execute('ALTER TABLE leads ADD COLUMN message_date_time DATETIME NULL')
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

def insert_lead_to_base44(lead):
    """Insert lead into Base44 API"""
    try:
        # Map lead data to Base44 API format
        base44_lead = {
            "full_name": lead.get("name", ""),
            "email": lead.get("email", ""),
            # "phone": "",  # Not available in current lead structure
            "company": lead.get("company", ""),
            "job_title": lead.get("title", ""),
            "linkedin_url": lead.get("linkedin", ""),
            "location": lead.get("location", ""),
            "company_website": lead.get("website", ""),
            "company_employees": lead.get("est_employees", ""),
            "company_industry": lead.get("industry", ""),
            "company_revenue": lead.get("est_revenue", ""),
            "first_identified_url": lead.get("first_identified_url", ""),
            "first_identified_datetime": lead.get("first_identified_datetime", ""),
            "profile_image": lead.get("profile_image", ""),
            # "tags": None,
            # "enrichment_status": "pending",
            # "enrichment_date": datetime.now().isoformat() + "Z",
            # "enrichment_notes": None,
            # "lead_score": 0,
            "source": "slack_etl",
            "status": "new",
            # "prompt_tokens": 0,
            # "completion_tokens": 0,
            # "batch_id": f"slack-etl-{int(time.time())}"
        }
        
        # Prepare payload
        payload = {
            "leads": [base44_lead],
            "batch_size": 1,
            "delay_ms": 1000
        }
        
        # Prepare headers
        headers = {
            "Content-Type": "application/json",
            "X-API-Key": BASE44_API_KEY
        }

        create_lead = BASE44_API_URL+"functions/publicBatchInsertLeads"
        
        # Make API request
        response = requests.post(create_lead, json=payload, headers=headers)
        
        try:
            response_data = response.json()
            
            # Check if success is true and total_created is 1
            if (response_data.get("success") == True and 
                response_data.get("summary", {}).get("total_created") == 1):
                
                # Log the successful lead creation with lead data
                leads_data = response_data.get("leads", [])
                if leads_data:
                    lead_data = leads_data[0]  # Get the first (and only) lead
                    logging.info(f"Successfully inserted lead to Base44: {lead.get('name', 'Unknown')} from {lead.get('company', 'Unknown')}")
                    logging.info(f"Lead data returned: {json.dumps(lead_data, indent=2)}")
                else:
                    logging.info(f"Successfully inserted lead to Base44: {lead.get('name', 'Unknown')} from {lead.get('company', 'Unknown')}")
                return True
            else:
                logging.warning(f"Lead insertion response indicates issues. Success: {response_data.get('success')}, Total created: {response_data.get('summary', {}).get('total_created')}")
                return False
                
        except json.JSONDecodeError as e:
            logging.error(f"Failed to parse JSON response from Base44 API: {response.text}. Error: {str(e)}")
            return False
        except Exception as e:
            logging.error(f"Error processing Base44 response: {str(e)}")
            return False
            
    except Exception as e:
        logging.error(f"Error inserting lead to Base44: {str(e)}")
        return False

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
                name, title, company, email, linkedin, location, first_identified_url, first_identified_datetime, profile_image, website, industry, est_employees, est_revenue, message_date_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''', (
            lead["name"], lead["title"], lead["company"], lead["email"], lead["linkedin"], lead["location"],
            lead["first_identified_url"], lead["first_identified_datetime"], lead["profile_image"], lead["website"],
            lead["industry"], lead["est_employees"], lead["est_revenue"], lead["message_date_time"]
        ))
        conn.commit()
    finally:
        c.close()
        conn.close()

def etl_job():
    create_db_and_table()
    last_ts = get_last_processed_ts()
    logging.info(f"Last processed timestamp: {last_ts}")
    messages = fetch_slack_messages(oldest=last_ts)
    leads = []
    max_ts = last_ts
    for msg in messages:
        lead = extract_lead_from_message(msg)
        if lead["name"] and lead["company"]:
            insert_lead(lead)
            # Also insert to Base44 API
            insert_lead_to_base44(lead)
            leads.append(lead)
        # Track the max timestamp for incremental load
        msg_ts = msg.get("ts")
        if msg_ts and (max_ts is None or float(msg_ts) > float(max_ts)):
            max_ts = msg_ts
    # Update progress table if new messages were processed
    if max_ts and max_ts != last_ts:
        update_last_processed_ts(max_ts)
    logging.info(f"ETL job completed. Processed {len(leads)} leads.")
# === RUN ETL JOB IMMEDIATELY ===
etl_job()

