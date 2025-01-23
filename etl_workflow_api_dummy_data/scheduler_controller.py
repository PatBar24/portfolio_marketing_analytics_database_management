import extract as ex
import transform as tr
import load as ld
from datetime import datetime, timedelta
import schedule
import time


# Define the full ETL process as a function
def run_etl():
    print(f"ETL process started at {datetime.now()}")
    try:
        ex.data_pull()
        print("Data extraction completed.")
        
        tr.transform_data()
        print("Data transformation completed.")
        
        ld.load_data_gs()
        print("Data loading to Google Sheets completed.")
    except Exception as e:
        print(f"ETL process failed: {e}")
    print(f"ETL process finished at {datetime.now()}")


if __name__ == "__main__":
    # Configure auto-refresh
    start_time = datetime.now()
    end_time = start_time + timedelta(days=365)

    # Run the etl function manually
    print("Running ETL process manually for the first time...")
    run_etl()

    # Schedule the job to run weekly on Monday at 3:00 AM
    schedule.every().monday.at("03:00").do(ex.data_pull)
    print("Scheduler is running...")

    # Keep the script running
    while datetime.now() < end_time:
        schedule.run_pending()
        print(f"Sleeping for one week... Current time: {datetime.now()}")
        time.sleep(604800)  # Sleep for one week (in seconds)
    
    print("Scheduler has stopped. One year has passed.")

