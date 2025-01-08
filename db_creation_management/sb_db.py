import pandas as pd
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time

#Crear una base de datos POSTGRESQL con nombre SB_DATABASE

file = pd.read_csv('/Users/patriciabartoloni/Downloads/ios_subscriber_data.csv')
file_df = pd.DataFrame(file)
file_df['subs_id'] = file_df['subs_id'].astype(str)


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


tabla = "ios_subscribers"

   # Drop the table if it exists
cursor.execute(f"DROP TABLE IF EXISTS {tabla};")
conn.commit()

cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {tabla} (
        id SERIAL PRIMARY KEY,
        subs_id VARCHAR(250),
        event_date DATE,
        app_name VARCHAR(250),
        app_apple_id BIGINT,
        subscription_name VARCHAR(250),
        plan_name VARCHAR(250),
        plan_id VARCHAR(250),
        duration VARCHAR(250),
        subscription_apple_id BIGINT,
        subscription_group_id INT,
        standard_subscription_duration VARCHAR(250),
        subscription_offer_name VARCHAR(250),
        promotional_offer_id VARCHAR(250),
        subscription_offer_type VARCHAR(250),
        subscription_offer_duration VARCHAR(250),
        marketing_optin_duration VARCHAR(250),
        customer_price DOUBLE PRECISION,
        customer_currency VARCHAR(50),
        developer_proceeds INT,
        customer_price_usd DOUBLE PRECISION,
        developer_proceeds_usd INT,
        comm_taxes_usd DOUBLE PRECISION,
        per_comm_taxes DOUBLE PRECISION, 
        device VARCHAR(50),
        country VARCHAR(50),
        subscriber_id BIGINT,
        subscriber_id_reset VARCHAR(50),
        refund VARCHAR(50),
        purchase_date DATE,
        units INT,
        report_date DATE,
        status VARCHAR(50),
        event_type VARCHAR(50)
    )
""")
conn.commit()

cursor.execute(f"""
    ALTER TABLE {tabla} 
        ALTER COLUMN customer_price TYPE DOUBLE PRECISION USING customer_price::TEXT::DOUBLE PRECISION,
        ALTER COLUMN customer_price_usd TYPE DOUBLE PRECISION USING customer_price_usd::TEXT::DOUBLE PRECISION,
        ALTER COLUMN comm_taxes_usd TYPE DOUBLE PRECISION USING comm_taxes_usd::TEXT::DOUBLE PRECISION,
        ALTER COLUMN per_comm_taxes TYPE DOUBLE PRECISION USING per_comm_taxes::TEXT::DOUBLE PRECISION;
""")
conn.commit()

def to_date(value):
    if pd.isna(value) or value == '' or value == 'NaN':
        return None  
    try:
        return pd.to_datetime(value, errors='coerce').date() if isinstance(value, str) else value
    except:
        return None 

file_df['purchase_date'] = file_df['purchase_date'].apply(to_date)

for _, row in file_df.iterrows():
    cursor.execute(f"""
    INSERT INTO {tabla} (subs_id, event_date, app_name, app_apple_id, subscription_name, plan_name, plan_id, duration, 
    subscription_apple_id, subscription_group_id, standard_subscription_duration, subscription_offer_name, 
    promotional_offer_id, subscription_offer_type, subscription_offer_duration, marketing_optin_duration, 
    customer_price, customer_currency, developer_proceeds, customer_price_usd, developer_proceeds_usd, 
    comm_taxes_usd, per_comm_taxes, device, country, subscriber_id, subscriber_id_reset, refund, purchase_date, 
    units, report_date, status, event_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (row['subs_id'], row['event_date'], row['app_name'], row['app_apple_id'], row['subscription_name'], row['plan_name'],
          row['plan_id'], row['duration'], row['subscription_apple_id'], row['subscription_group_id'],
          row['standard_subscription_duration'], row['subscription_offer_name'], row['promotional_offer_id'],
          row['subscription_offer_type'], row['subscription_offer_duration'], row['marketing_optin_duration'],
          row['customer_price'], row['customer_currency'], row['developer_proceeds'], row['customer_price_usd'],
          row['developer_proceeds_usd'], row['comm_taxes_usd'], row['per_comm_taxes'], row['device'], row['country'],
          row['subscriber_id'], row['subscriber_id_reset'], row['refund'], row['purchase_date'], row['units'],
          row['report_date'], row['status'], row['event_type']))

conn.commit()

cursor.execute(f"SELECT * FROM {tabla} LIMIT 5;")
rows = cursor.fetchall()
for row in rows:
    print(row)

cursor.close()
conn.close()
