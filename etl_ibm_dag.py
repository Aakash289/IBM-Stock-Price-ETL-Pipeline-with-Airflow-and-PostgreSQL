from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import yfinance as yf
import pandas as pd
import psycopg2
import json
import traceback
from datetime import datetime
import os

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

@dag(schedule_interval='@daily', start_date=days_ago(1), catchup=False, default_args=default_args, tags=["etl", "stock"])
def ibm_stock_etl():

    @task()
    def extract_data():
        try:
            ibm = yf.download('IBM', period='1d', interval='1h')
            ibm.columns = ['_'.join(col).strip().lower() if isinstance(col, tuple) else col.strip().lower() for col in ibm.columns]
            ibm.reset_index(inplace=True)
            ibm.columns = [col.strip().lower().replace(' ', '_') for col in ibm.columns]
            return ibm.to_json(orient='records', date_format='iso')
        except Exception as e:
            traceback.print_exc()
            raise

    @task()
    def transform_data(raw_data):
        try:
            data = json.loads(raw_data)
            df = pd.DataFrame(data)

            column_mapping = {}
            normalized_columns = []

            for col in df.columns:
                clean_col = col.strip().lower().replace(' ', '_')
                if clean_col.endswith('_ibm'):
                    base = clean_col.replace('_ibm', '')
                    column_mapping[clean_col] = base
                    clean_col = base
                normalized_columns.append(clean_col)

            df.columns = normalized_columns
            for old, new in column_mapping.items():
                if old in df.columns:
                    df[new] = df[old]
                    if old != new:
                        df.drop(columns=[old], inplace=True)

            datetime_col = next((c for c in ['datetime', 'date', 'timestamp', 'index'] if c in df.columns), None)
            df['datetime'] = pd.to_datetime(df[datetime_col], errors='coerce') if datetime_col else datetime.now()

            price_cols = {
                'open': ['open'],
                'high': ['high'],
                'low': ['low'],
                'close': ['close'],
                'adj_close': ['adj_close', 'adjclose'],
                'volume': ['volume']
            }

            for std_col, options in price_cols.items():
                match = next((col for col in options if col in df.columns), None)
                df[std_col] = pd.to_numeric(df[match], errors='coerce').fillna(0) if match else 0

            df['volume'] = df['volume'].astype(int)
            final_df = df[['datetime', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]
            return final_df.to_json(orient='records', date_format='iso')
        except Exception as e:
            traceback.print_exc()
            fallback = [{
                'datetime': datetime.now().isoformat(),
                'open': 0.0, 'high': 0.0, 'low': 0.0,
                'close': 0.0, 'adj_close': 0.0, 'volume': 0
            }]
            return json.dumps(fallback)

    @task()
    def load_data(transformed_json):
        try:
            df = pd.DataFrame(json.loads(transformed_json))
            df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce').fillna(datetime.now())
            for col in ['open', 'high', 'low', 'close', 'adj_close']:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype(int)
            df = df[['datetime', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]

            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=int(os.getenv("DB_PORT", 5432)),
                dbname=os.getenv("DB_NAME", "postgres"),
                user=os.getenv("DB_USER", "postgres"),
                password=os.getenv("DB_PASSWORD", "password")
            )
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS public.ibm_stock (
                    id SERIAL PRIMARY KEY,
                    datetime TIMESTAMP NOT NULL,
                    open FLOAT NOT NULL,
                    high FLOAT NOT NULL,
                    low FLOAT NOT NULL,
                    close FLOAT NOT NULL,
                    adj_close FLOAT NOT NULL,
                    volume BIGINT NOT NULL
                )
            """)
            conn.commit()

            insert_query = """
                INSERT INTO public.ibm_stock (datetime, open, high, low, close, adj_close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """

            values = [
                (
                    row['datetime'].to_pydatetime() if hasattr(row['datetime'], 'to_pydatetime') else row['datetime'],
                    float(row['open']),
                    float(row['high']),
                    float(row['low']),
                    float(row['close']),
                    float(row['adj_close']),
                    int(row['volume'])
                )
                for _, row in df.iterrows()
            ]

            cursor.executemany(insert_query, values)
            conn.commit()
            cursor.close()
            conn.close()

            return f"Inserted {len(values)} rows."
        except Exception as e:
            traceback.print_exc()
            raise

    raw = extract_data()
    transformed = transform_data(raw)
    load_data(transformed)

dag_instance = ibm_stock_etl()
