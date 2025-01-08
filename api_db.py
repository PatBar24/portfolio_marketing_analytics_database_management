import requests
import json
import os
import pandas as pd

"""
Desde la siguiente API para tomar datos de exchange, generar una API https://exchangerate.host/documentation 
y diseñar un script en python que tome la data más reciente de exchange (/live) y genere un archivo con .csv 
"""

url = 'https://api.exchangerate.host/live?access_key=65a999c178ef023ad01f8411bef6b84c'
try:
    response = requests.get(url)
    response.raise_for_status()  
    data = response.json()
    print('Data obtenida exitosamente')

except requests.exceptions.RequestException as e:
    print(f"Error al realizar la solicitud HTTP: {e}")

output_csv = "/Users/patriciabartoloni/Documents/Curso Python/SB_Challenge/api_data.csv"

if data and 'quotes' in data:
    base_currency = data.get('source', 'USD')
    timestamp = data.get('timestamp', None)
    timestamp = pd.to_datetime(timestamp, unit='s', origin='unix')
    quotes = data['quotes']
    df = pd.DataFrame(list(quotes.items()), columns=['Currency', 'Rate'])
    
    df['Base'] = base_currency
    df['Date'] = timestamp
    
    print('Dataframe creado exitosamente')
    df.to_csv(output_csv, index=False, encoding='utf-8')
else:
    print("No se pudieron obtener los datos.")

if os.path.exists(output_csv):
    df = pd.read_csv(output_csv)
    print(df)
