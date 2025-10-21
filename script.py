import requests
import os
from snowflake import connector
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 100
# Snowflake connection parameters
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE") 
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")   
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA") 
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")           


def run_stock_job():
    DS = datetime.now().strftime('%Y-%m-%d')
    url = "https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit=" + str(LIMIT) + "&sort=ticker&apiKey=" + POLYGON_API_KEY
    response = requests.get(url)
    tickers = []

    data = response.json()
    for ticker in data['results']:
        ticker['ds'] = DS
        tickers.append(ticker)

    i = 0
    while 'next_url' in data and i < 2:
        print("requesting next page", data['next_url'])
        response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        #print(data)
        for ticker in data['results']:
            ticker['ds'] = DS
            tickers.append(ticker)
        i += 1

    example_ticker = {'ticker': 'APOC', 
    'name': 'Innovator Equity Defined Protection ETF - 6mo Apr/Oct', 
    'market': 'stocks',
    'locale': 'us',
    'primary_exchange': 'BATS',
    'type': 'ETF',
    'active': True,
    'currency_name': 'usd',
    'cik': '0001415726',
    'composite_fig': 'BBG01Q0QS2S8', 'share_class_figi': 'BBG01Q0QS4L1', 'last_updated_utc': '2025-10-19T06:05:51.184958137Z'
    ,'ds': DS}

    fieldnames = list(example_ticker.keys())
    load_to_snowflake(tickers, fieldnames)
    print(f"Loaded {len(tickers)} rows to Snowflake")    


#------------------------------------------------------------------------------------------------------------------------
def load_to_snowflake(rows, fieldnames):
    
    connect_kwargs = {
        'user': SNOWFLAKE_USER,
        'password': SNOWFLAKE_PASSWORD,
        }
    account = SNOWFLAKE_ACCOUNT
    if account:
        connect_kwargs['account'] = account
    warehouse = SNOWFLAKE_WAREHOUSE
    database = SNOWFLAKE_DATABASE
    scehma = SNOWFLAKE_SCHEMA
    role = SNOWFLAKE_ROLE
    if warehouse:
        connect_kwargs['warehouse'] = warehouse
    if database:
        connect_kwargs['database'] = database
    if scehma:
        connect_kwargs['schema'] = scehma
    if role:
        connect_kwargs['role'] = role
    
    print(connect_kwargs)

    conn = connector.connect(
        user = connect_kwargs['user'],
        password = connect_kwargs['password'],
        account = connect_kwargs['account'],
        warehouse = connect_kwargs['warehouse'],
        database = connect_kwargs['database'],
        schema = connect_kwargs['schema'],
        role = connect_kwargs['role'],
        session_parameters = {"CLIENT_TELEMETRY_ENABLED": False},
    )


    try:
        # Connect to Snowflake
        cs = conn.cursor()
        
        table_name = os.getenv("SNOWFLAKE_TABLE", "stock_tickers")

        type_overrides = {
            "ticker": "VARCHAR",
            "name": "VARCHAR",
            "market": "VARCHAR",
            "locale": "VARCHAR",
            "primary_exchange": "VARCHAR",
            "type": "VARCHAR",
            "active": "BOOLEAN",
            "currency_name": "VARCHAR",
            "cik": "VARCHAR",
            "composite_fig": "VARCHAR",
            "share_class_figi": "VARCHAR",
            "last_updated_utc": "TIMESTAMP_NTZ",
            "ds": "DATE"
        }

        columns_sql_parts = []
        for col in fieldnames:
            col_type = type_overrides.get(col, "VARCHAR")
            columns_sql_parts.append(f"{col} {col_type}")

        # Create table if it doesn't exist
        create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ( ' + ', '.join(columns_sql_parts) + ', loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() )'
        cs.execute(create_table_sql)
        
        column_list = ', '.join([f'"{c.upper()}"' for c in fieldnames])
        placeholders = ', '.join(f'%{c})s' for c in fieldnames)
        insert_sql = f'INSERT INTO {table_name} ( {column_list} ) VALUES ( ' + ', '.join([f'%({c})s' for c in fieldnames]) + ' )'

        transformed = []

        for t in rows:
            row = {}
            for k in fieldnames:
                row[k] = t.get(k, None)
            transformed.append(row)
        
        if transformed:
            cs.executemany(insert_sql, transformed)
        
    finally:
        cs.close()
        
       

        


if __name__ == "__main__":
    run_stock_job()







