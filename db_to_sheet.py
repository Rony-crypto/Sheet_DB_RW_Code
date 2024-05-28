import psycopg2
import os.path
#from google.auth import default
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError



SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = "1Er32SHtvJ444C0IRnbyLZRrGloqDkJ5GTBQGckXvIr0"
SAMPLE_RANGE_NAME = "Sheet1!A:Z"

# Connect to PostgreSQL database
def connect_to_postgres():
    conn = psycopg2.connect(
        dbname="DB_Name",
        user="data_rony",
        password="rony",
        host="10.192.192.52",
        port="5432"
    )
    return conn

# Read data from PostgreSQL database
def read_data_from_postgres(conn):
    cursor = conn.cursor()
    cursor.execute(   
    '''select 
	wallet,
	min(t1.report_date) as first_TDAU,
	t2.min_acc_verification_date::Date as onboarded_date,
	pr_biz_type,
	pr_biz_name,
	pr_full_name,
	count(transaction_number) as tpt,
	sum(amount) as tpv	
from ads_tp.npsb_daily_transaction as t1
inner join cdm_tp.tp_user_profile_dim as t2 on t1.wallet = t2.ac_wallet 
where t1.report_date >= '2024-01-01' and  t1.report_date <'2024-02-01'
and t2.min_acc_verification_date::date between '2023-10-01'::date and '2023-11-30'::date
group by 1,3,4,5,6
limit 50''')


    rows = cursor.fetchall()
    column_names = [i[0] for i in cursor.description]
    cursor.close()
    print (rows)
   # return column_names, rows
    return [column_names] + rows


def update_sheet():
  

  """Shows basic usage of the Sheets API.
  Prints values from a sample spreadsheet.
  """
  creds = None

  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open("token.json", "w") as token:
        token.write(creds.to_json())

    

  try:
        service = build("sheets", "v4", credentials=creds)
        service.spreadsheets().values().clear(spreadsheetId = SPREADSHEET_ID, range=SAMPLE_RANGE_NAME, body={} ).execute()
        print("Sheet cleared successfully.")
  except HttpError as err:
        print(f"An error occurred while clearing the sheet: {error}")
        return err


  try:
    service = build("sheets", "v4", credentials=creds)
    data = [{"range": SAMPLE_RANGE_NAME, "values": values_to_write}]
    value_input_option = "USER_ENTERED"  # Define value_input_option here
    body = {"valueInputOption": value_input_option, "data": data}

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = service.spreadsheets().values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body).execute()
    print(f"{result.get('totalUpdatedCells')} cells updated.")
    return result
   
  except HttpError as error:
    print(f"An error occurred: {error}")
    return error



if __name__ == "__main__":
  
    conn = connect_to_postgres()

    # Read data from PostgreSQL
    data_from_db = read_data_from_postgres(conn)

    # Prepare data for writing to Google Sheets
    values_to_write = [[str(value) for value in row] for row in data_from_db]

    # Write data to Google Sheets
    update_sheet()

    # Close the connection
    conn.close()
