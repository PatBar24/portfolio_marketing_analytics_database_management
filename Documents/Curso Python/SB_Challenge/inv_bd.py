import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time

#Crear una base de datos POSTGRESQL con nombre SB_DATABASE

file = pd.read_excel('/Users/patriciabartoloni/Downloads/investment_all_networks.xls')
file_df2 = pd.DataFrame(file)

file_df2['spend_usd'] = file_df2['spend_usd'].astype(float)
file_df2['clicks'] = file_df2['clicks'].astype(float)
file_df2['impressions'] = file_df2['impressions'].astype(float)

db_name = "sb_database"
db_user = "postgres"
db_password = "tuco2025"
db_host = "localhost"
db_port = "5432"

conn = psycopg2.connect(
    dbname='postgres',
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cursor = conn.cursor()

cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}';")
exists = cursor.fetchone()

if not exists:
    try:
        cursor.execute(f"CREATE DATABASE {db_name};")
        print(f"La base de datos '{db_name}' fue creada.")
    except psycopg2.errors.DuplicateDatabase:
        print(f"Database '{db_name}' already exists.")
else:
    print(f"La base de datos '{db_name}' ya existe.")

cursor.close()
conn.close()

conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
)
cursor = conn.cursor()


tabla = "investment_all_networks"

   # Drop the table if it exists
cursor.execute(f"DROP TABLE IF EXISTS {tabla};")
conn.commit()

cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {tabla} (
        id SERIAL PRIMARY KEY,
        date DATE,
        account_id VARCHAR(50),
        account_name VARCHAR(250),
        campaign_id VARCHAR(50),
        country VARCHAR(50),
        campaign_name VARCHAR(250),
        platform VARCHAR(50),
        channel VARCHAR(50),
        campaign_type VARCHAR(50),
        spend_usd DOUBLE PRECISION,
        clicks DOUBLE PRECISION,
        impressions DOUBLE PRECISION
    )
""")
conn.commit()

def to_date(value):
    if pd.isna(value) or value == '' or value == 'NaN':
        return None  
    try:
        return pd.to_datetime(value, errors='coerce').date() if isinstance(value, str) else value
    except:
        return None 

file_df2['date'] = file_df2['date'].apply(to_date)

file_df2 = file_df2.where(pd.notnull(file_df2), None)

for _, row in file_df2.iterrows():
    cursor.execute(f""" INSERT INTO {tabla} (date,
                        account_id,
                        account_name,
                        campaign_id,
                        country,
                        campaign_name,
                        platform,
                        channel,
                        campaign_type,
                        spend_usd,
                        clicks,
                        impressions)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (row['date'], row['account_id'], row['account_name'], row['campaign_id'], row['country'], row['campaign_name'],
          row['platform'], row['channel'], row['campaign_type'], row['spend_usd'],
          row['clicks'], row['impressions']))

conn.commit()

cursor.execute(f"SELECT * FROM {tabla} LIMIT 5;")
rows = cursor.fetchall()
for row in rows:
    print(row)

cursor.close()
conn.close()
