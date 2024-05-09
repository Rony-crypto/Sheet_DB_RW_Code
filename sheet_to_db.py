import psycopg2
import os.path
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Google Sheets API scopes
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Google Sheets ID and range
SPREADSHEET_ID = "1Er32SHtvJ444C0IRnbyLZRrGloqDkJ5GTBQGckXvIr0"
SAMPLE_RANGE_NAME = "Sheet1!A:Z"

# PostgreSQL connection parameters
POSTGRES_PARAMS = {
    "dbname": "tallykhata",
    "user": "data_rony",
    "password": "rony",
    "host": "10.192.192.52",
    "port": "5432"
}

# Connect to PostgreSQL database
def connect_to_postgres():
    conn = psycopg2.connect(**POSTGRES_PARAMS)
    return conn

# Create table in PostgreSQL database
def create_table(conn, column_names):
    cursor = conn.cursor()
    # Assuming table name is "sheet_data"
    cursor.execute("DROP TABLE IF EXISTS test.merchant_onboard_test;")
    create_table_query = "CREATE TABLE test.merchant_onboard_test ({});".format(
        ", ".join([f"{name} TEXT" for name in column_names])
    )
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()

# Write data to PostgreSQL database
def write_data_to_postgres(conn, data):
    cursor = conn.cursor()
    for row in data:
        cursor.execute("INSERT INTO test.merchant_onboard_test VALUES (%s);" % ','.join(['%s'] * len(row)), row)
    conn.commit()
    cursor.close()

# Fetch data from Google Sheet
def fetch_data_from_sheet(service):
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID, range=SAMPLE_RANGE_NAME).execute()
        values = result.get('values', [])
        return values
    except HttpError as error:
        print(f"An error occurred while fetching data from Google Sheet: {error}")
        return None

def main():
    # Establish connection to PostgreSQL
    conn = connect_to_postgres()

    # Fetch data from Google Sheet
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    service = build("sheets", "v4", credentials=creds)
    data_from_sheet = fetch_data_from_sheet(service)

    if data_from_sheet:
        # Get column names
        column_names = data_from_sheet[0]
        # Create table in PostgreSQL
        create_table(conn, column_names)
        # Write data to PostgreSQL
        write_data_to_postgres(conn, data_from_sheet[1:])
        print("Data written to PostgreSQL successfully.")

    # Close the connection
    conn.close()

if __name__ == "__main__":
    main()
