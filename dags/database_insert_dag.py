#from airflow import DAG
#from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
import os
import requests
import pandas as pd


DAG_ID = "postgres_operator_dag"
pd.set_option('display.float_format', lambda x: '%.2f' % x)
format = 'json'


def db_connection():
    # Zmienne środowiskowe, które próbujemy odczytać
    db_host = os.getenv('POSTGRES_HOST' , 'postgres')
    db_name = os.getenv('POSTGRES_DB', 'airflow')
    db_user = os.getenv('POSTGRES_USER', 'airflow')
    db_password = os.getenv('POSTGRES_PASSWORD', 'airflow')
    db_port = os.getenv('POSTGRES_PORT', 5432) # Pamiętaj, port powinien być integer lub string

    conn = psycopg2.connect(
        host=db_host,
        database=db_name,
        user=db_user,
        password=db_password,
        port=db_port,
        # DODAJ JAWNE KODOWANIE, ABY OMINĄĆ PROBLEM Z SYSTEMOWYMI PARAMETRAMI:
        options='-c client_encoding=UTF8' 
    )
    return conn


def table_creation():
    conn = db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS money_convert (
            id SERIAL PRIMARY KEY,
            currency VARCHAR(50) NOT NULL,
            code VARCHAR(255) NOT NULL,
            mid FLOAT NOT NULL,
            date DATE NOT NULL,
            gold_price_per_gram FLOAT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.commit()
    cursor.close()
    conn.close()

def fetch_data():
    cena_zlota = requests.get('https://api.nbp.pl/api/cenyzlota/')
    waluty = requests.get(f'https://api.nbp.pl/api/exchangerates/tables/A?format={format}')
    gold_price_pln = cena_zlota.json()[0]['cena']

    if waluty.status_code == 200:
        data = waluty.json()
        df = pd.DataFrame(data[0]['rates'])
        df['date'] = data[0]['effectiveDate']
    
    df['gold_price_per_gram'] = gold_price_pln / df['mid']


    return df


def insert_data_to_db():
    df = fetch_data()
    conn = db_connection()
    cursor = conn.cursor()

    for index, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO money_convert (currency, code, mid, date, gold_price_per_gram)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (row['currency'], row['code'], row['mid'], row['date'], row['gold_price_per_gram'])
        )

    conn.commit()
    cursor.close()
    conn.close()

db_connection()
print("Połączono z bazą danych.")
table_creation()
print("Tabela utworzone lub już istnieje")
insert_data_to_db()
print("Dane zostały wstawione do bazy danych.")