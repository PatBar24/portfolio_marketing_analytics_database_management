import requests
import pandas as pd
import json
import os
from kaggle.api.kaggle_api_extended import KaggleApi
import sqlite3
from datetime import datetime

def data_pull():
    print("Starting data refresh process...")

    api = KaggleApi()
    api.authenticate()

    # Define the dataset name
    dataset_name = 'sinderpreet/analyze-the-marketing-spending'

    # Ruta absoluta del directorio donde está el script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Crear la ruta de destino para los datos dentro del directorio del script
    destination_folder = os.path.join(script_dir, 'api_testing_organic_data')

    # Create a folder to download the file into
    os.makedirs(destination_folder, exist_ok=True)

    # Check if folder and file exists or not and download the dataset
    data_file = os.path.join(destination_folder, 'Marketing.csv')  

    # Always download and overwrite the dataset file
    print(f"Downloading {dataset_name} to overwrite the current dataset...")
    api.dataset_download_files(dataset_name, path=destination_folder, unzip=True)
    print(f"Dataset {dataset_name} downloaded successfully.")

    # Update last refresh timestamp to log file
    timestamp_data = pd.DataFrame({"last_refresh": [datetime.now()]})
    timestamp_file = os.path.join(destination_folder, 'last_refresh.csv')
    timestamp_data.to_csv(timestamp_file, index=False)
    print(f"Timestamp saved to {timestamp_file}")

    # Load the dataset
    if os.path.exists(data_file):  
        df = pd.read_csv(data_file)
        # Add a refresh timestamp to the dataframe
        df['refresh_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print("Dataset Sample:")
        print(df.head())
    else:
        print(f"Error: {data_file} not found. Please verify the dataset contents.")

    # Connect to SQLite database
    sqlite_file = 'marketing_dummy_data.db'
    conn = sqlite3.connect(sqlite_file)
    cursor = conn.cursor()

    # Load the data into the database
    table_name = 'marketing_dummy_data_crosschannel'
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Data loaded into SQLite table '{table_name}'.")

    # Verify data in SQL
    cursor.execute('SELECT COUNT(*) FROM marketing_dummy_data_crosschannel')
    row_count = cursor.fetchone()[0] 

    if row_count > 0:
        print(f"Data loaded successfully with {row_count} records.")
    else:
        print("Error: Data not loaded. Please verify the data contents.")
    
    conn.close()

