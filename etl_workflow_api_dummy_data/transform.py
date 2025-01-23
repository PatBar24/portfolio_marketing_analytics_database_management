import sqlite3
import pandas as pd
import os

def transform_data():
    print("Starting data transformation process...")

    # Connect to SQLite database
    sqlite_file = 'marketing_dummy_data.db'
    conn = sqlite3.connect(sqlite_file)
    try:  # Transform data in SQL: Add Channel, Tactic and Segment columns
        query= """with 'segmented_mapping' as (select distinct 
                    case when lower(campaign_name) like '%facebook_tier1%'
                    then 'Facebook National Awareness' 
                    when lower(campaign_name) like '%facebook_tier2%'
                    then 'Facebook Local Awareness'
                    when lower(campaign_name )like '%facebook_lal%'
                    then 'Facebook Lookalikes'
                    when lower(campaign_name )like '%facebook_retargeting%'
                    then 'Facebook Remarketing'
                    when lower(campaign_name) like '%google_wide%'
                    then 'Google Search Non-Brand'
                    when lower(campaign_name) like '%google_hot%'
                    then 'Google Search Brand'
                    when lower(campaign_name) like '%instagram_tier1%'
                    then 'Instagram National Awareness'
                    when lower(campaign_name) like '%instagram_tier2%'
                    then 'Instagram Local Awareness'
                    when lower(campaign_name) like '%instagram_blogger%'
                    then 'Affiliate Influencers'
                    when lower(campaign_name) like '%youtube%'
                    then 'YouTube Video'
                    when lower(campaign_name) like '%banner%' then 'Display Prospecting'
                    else 'Other'
                    end as 'Tactic',
                    case when campaign_name in ('facebook_tier1',
                    'facebOOK_tier2', 'facebook_retargeting', 'facebook_lal', 
                    'instagram_tier1', 'instagram_tier2', 'instagram_blogger') 
                    then 'Social'
                    when campaign_name in ('google_wide', 'google_hot') 
                    then 'Search'
                    when campaign_name in ('youtube_blogger') 
                    then 'Video'
                    when campaign_name in ('banner_partner') 
                    then 'Affiliate' else 'Other'
                    end as 'Channel',
                    case when (campaign_name like '%tier%' or campaign_name like '%lal%'
                    or campaign_name like '%wide%' or campaign_name like '%banner%' or campaign_name like '%blogger%')
                    then 'Prospecting' else 'Remarketing' end as 'Segment',
                    * from marketing_dummy_data_crosschannel)
                    select distinct c_date, channel, segment, tactic, sum(impressions) as impressions, 
                    sum(clicks) as clicks, sum(mark_spent) as spend, sum(leads) as leads, sum(revenue) as revenue,
                    sum(orders) as orders from segmented_mapping group by c_date, channel, tactic, segment"""
        df_transformed = pd.read_sql_query(query, conn)
        transformed_table_name = 'marketing_transformed_data'
        df_transformed.to_sql(transformed_table_name, conn, if_exists='replace', index=False)
        print(f"Data transformation complete. Transformed data saved to table '{transformed_table_name}'.")
        print("Transformed Data Sample:")
        print(df_transformed.head())
    except Exception as e:
        print("Data couldn't be transformed. Error: {e}")
    finally:
        conn.close()
        print('Database connection closed.')



                    
