import requests
import os
import csv
from dotenv import load_dotenv
load_dotenv()

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 100

url = "https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit=" + str(LIMIT) + "&sort=ticker&apiKey=" + POLYGON_API_KEY

response = requests.get(url)

tickers = []

data = response.json()
for ticker in data['results']:
    tickers.append(ticker)

i = 0
while 'next_url' in data and i < 3:
    print("requesting next page", data['next_url'])
    response = requests.get(data['next_url'] + f'&apiKey={POLYGON_API_KEY}')
    data = response.json()
    #print(data)
    for ticker in data['results']:
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
 'composite_figi': 'BBG01Q0QS2S8', 'share_class_figi': 'BBG01Q0QS4L1', 'last_updated_utc': '2025-10-19T06:05:51.184958137Z'}

# Define the CSV file path
csv_file_path = 'tickers.csv'

# Define the fieldnames based on example_ticker schema
fieldnames = list(example_ticker.keys())

# Write the tickers data to CSV
with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for ticker in tickers:
        writer.writerow({field: ticker.get(field, '') for field in fieldnames})

print(f"Data has been written to {csv_file_path}")








