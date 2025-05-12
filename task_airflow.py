from airflow import DAG
from datatime import datatime, timedelta
import psycopg2 
import requests 
import json 

with DAG(
    dag_id = 'dag_with_cron_expression',
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily", ##'0 0 * * *'
    catchup=True,
    max_active_runs =1,
    default_args={
        'retries':1,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:
    # Create a task using the TaskFlow API
    @task()
    def hit_polygon_api(**context):
        # Instantiate a list of tickers that will be pulled and looped over
        stock_ticker = "AMZN"
        # Set variables
        polygon_api_key = "<your-api-key>"
        ds = context.get("ds")
        # Create the URL
        url = f"<https://api.polygon.io/v1/open-close/{stock_ticker}/{ds}?adjusted=true&apiKey={polygon_api_key}>"
        response = requests.get(url)
        # Return the raw data
    
    #2. Parse the relevant data fields 
    @task()
    def parse_stock_data(raw_data: dict):
        try:
          return{
              "open": raw_data["open"],
              "high": raw_data["high"],
              "low": raw_data["low"],
              "close": raw_data["close"],
              "volume": raw_data["volume"],
          }
          exccept Exception as e:
            print(e)
            return ValueError(f"Error parsing data: {e}")
    # 3. Store data in PostgreSQL
    @task()
    def store_data_in_postgres(parsed_data: dict):
        try:
          conn = psycopg2.connect(
              host="localhost",
              database="market_data",
              user="user_id",
              password="user_password"
          )

          cursor = conn.cursor()
          cursor.execute(
              "INSERT INTO daily_stock_prices (open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
              (
                  parsed_data["open"],
                  parsed_data["high"],
                  parsed_data["low"],
                  parsed_data["close"],
                  parsed_data["volume"],
              ),
          )
          conn.commit()
          cursor.close()
          conn.close()
        except Exception as e:
            print(e)
            return ValueError(f"Error storing data: {e}")

    #4.Optional : Trigger an alert if stock dropped 
    @task()
    def check_stock_price_drop(parsed_data: dict):
      if parsed_data["close"] < parsed_data["open"]:
          print(f"Stock {parsed_data['ticker']} dropped on {parsed_data['date']}")
      else:
          print(f"Stock {parsed_data['ticker']} did not drop on {parsed_data['date']}")

    # Link tasks with TaskFlow
    raw_data = hit_polygon_api()
    parsed_data = parse_stock_data(raw_data)
    store_in_db(parsed_data)
    check_drop(parsed_data)
