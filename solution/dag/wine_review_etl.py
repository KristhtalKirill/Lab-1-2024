from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from elasticsearch import Elasticsearch

DATA_PATH = "/opt/airflow/data"
OUTPUT_CSV = f"{DATA_PATH}/result.csv"


def read_and_process_files(**kwargs):
    # Считывание файлов
    all_data = []
    for file in os.listdir(DATA_PATH):
        if file.endswith(".csv"):
            filepath = os.path.join(DATA_PATH, file)
            data = pd.read_csv(filepath)
            all_data.append(data)

    # Объединение данных
    df = pd.concat(all_data)

    # Фильтрация
    df = df[(df['designation'].notnull()) & (df['region_1'].notnull())]

    # Обработка столбца price
    df['price'] = df['price'].fillna(0.0)

    # Сохранение в итоговый CSV
    df.to_csv(OUTPUT_CSV, index=False)


def save_to_elasticsearch(**kwargs):
    es = Elasticsearch(hosts="http://elasticsearch-kibana:9200")

    df = pd.read_csv(OUTPUT_CSV)
    df.fillna('', inplace=True)

    for _, row in df.iterrows():
        es.index(index="wine-data", body=row.to_dict())


with DAG(
        dag_id="csv_to_elasticsearch_pipeline",
        start_date=datetime(2023, 11, 1),
        schedule_interval=None,
        catchup=False
) as dag:
    read_process_task = PythonOperator(
        task_id="read_and_process_files",
        python_callable=read_and_process_files,
    )

    save_to_es_task = PythonOperator(
        task_id="save_to_elasticsearch",
        python_callable=save_to_elasticsearch,
    )

    read_process_task >> save_to_es_task
