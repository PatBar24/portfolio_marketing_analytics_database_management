import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import sqlite3
import pandas as pd

def load_data_gs():
    # Get the path of the current script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Path to the credentials file
    creds_file = os.path.join(script_dir, "..","..", "google_sheets", "portfolio-etl-workflow-5fc54bbe1301.json")

    # Authenticate using the credentials
    creds = Credentials.from_service_account_file(creds_file, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    print("Authenticated successfully.")
    # Build the service
    service = build("sheets", "v4", credentials=creds)

    # Connect to SQLite database
    sqlite_file = 'marketing_dummy_data.db'
    conn = sqlite3.connect(sqlite_file)
    print("Connected to SQLite database.")
    # Load the data from the database
    query = "SELECT * FROM 'marketing_transformed_data'"
    data = pd.read_sql_query(query, conn)

    # Convert the data to a list of lists
    values = data.values.tolist()

    # Include headers as the first row
    headers = data.columns.tolist()
    values.insert(0, headers)

    # The ID of the spreadsheet to update
    spreadsheet_id = "1APRdB9C_2osC7ErFN3MLEAmP5y_TlloLcbLTLdavzck"
    print(f"Updating Google Sheet with ID: {spreadsheet_id}")
    # The range of the sheet to update
    range_ = "Hoja 1!A1"

    # Update the Google Sheet
    body = {"values": values}

    result = service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=range_, valueInputOption="RAW", body=body).execute()
    print(f"Data loaded into Google Sheets: {result.get('updatedCells')} cells updated.")