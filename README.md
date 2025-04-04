# IBM-Stock-Price-ETL-Pipeline-with-Airflow-and-PostgreSQL
This project implements a complete ETL (Extract, Transform, Load) pipeline using Apache Airflow, which retrieves hourly IBM stock price data via the yFinance API, processes it, and stores it into a PostgreSQL database.

📌 Features

✅ Automated ETL pipeline scheduled with Airflow's @daily schedule

✅ Data Extraction using Yahoo Finance (yfinance)

✅ Transformation includes:

Column standardization

Type coercion and null handling

✅ Load cleansed data into a PostgreSQL table

✅ Error handling and fallbacks for robustness

Tool/Library	Role

Apache Airflow	Workflow orchestration (ETL DAGs)

yFinance	IBM stock data API

PostgreSQL	Target database
