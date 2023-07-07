from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from airflow import DAG
from datetime import datetime
from pyspark.sql import SparkSession
from hdfs import InsecureClient


import requests
import json
import os


def extract_vacancies():
    date = datetime.today().strftime('%Y-%m-%d')
    # hhru api`s URL
    url = 'https://api.hh.ru/vacancies'
    params = {
        'text': 'python developer',
        'period': 30,
        'per_page': 100,
        'page': 0
    }
    vacancies = []
    while True:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            vacancies += data['items']
            if data['pages'] == params['page']:
                break
            params['page'] = data['page'] + 1
        else:
            print(f'Error: {response.status_code}')
            break
    if vacancies:
        with open(f'data/raw/{date}_vacancies.json', 'w') as file:
            json.dump(vacancies, file, ensure_ascii=False)


def check_file():
    date = datetime.today().strftime('%Y-%m-%d')
    file_path = f'data/raw/{date}_vacancies.json'
    if os.path.isfile(file_path):
        return {"code": 0}
    else:
        return {"code": 1}


def check_code(**kwargs):
    ti = kwargs['ti']
    code = ti.xcom_pull(task_ids='check_file')['code']
    if code == 0:
        return 'success'
    else:
        return 'failure'

def save_to_hdfs():
    date = datetime.today().strftime('%Y-%m-%d')

    src_path = f'data/raw/{date}_vacancies.json'
    dest_dir = f"/hadoop-data/{date}_vacancies.json"
    client = InsecureClient('http://namenode:9870', user='root')

    if not client.status(dest_dir, strict=False):
        with open(src_path, 'rb') as local_file:
            local_file.seek(0) 
            client.write(dest_dir, local_file)
    
    client._session.close()


args = {
    'owner': 'teenspirit',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 19),
    'retries': 1
}


with DAG(
    'hh_api_vacancies',
    default_args=args,
    description='Extract vacancies from hh.ru API',
    schedule_interval='30 10 * * *',
    catchup=False
) as dag:
    # operator for func execution
    extract_vacancies_operator = PythonOperator(
        task_id='extract_vacancies',
        python_callable=extract_vacancies,
        dag=dag
    )

    # operator for check if json exist
    check_file_operator = PythonOperator(
        task_id='check_file',
        python_callable=check_file,
        dag=dag,
        xcom_push=True
    )

    branch_operator = BranchPythonOperator(
        task_id='branch',
        python_callable=check_code,
        provide_context=True,
        dag=dag
    )

    success_operator = DummyOperator(
        task_id='success',
        dag=dag
    )

    failure_operator = DummyOperator(
        task_id='failure',
        dag=dag
    )

    end_operator = DummyOperator(
        task_id='end',
        dag=dag
    )

    save_to_hdfs_operator = PythonOperator(
        task_id='save_to_hdfs',
        python_callable=save_to_hdfs,
        dag=dag
    )

    spark_submit = SparkSubmitOperator(
        task_id='spark_submit',
       # application='',
        name='DataProcessing',
        conn_id='spark_default',
        verbose=False,
        driver_memory='1g',
        executor_memory='1g',
        num_executors=2,
        executor_cores=1,
        dag=dag
)

extract_vacancies_operator >> check_file_operator >> branch_operator >> [
    success_operator, failure_operator]

success_operator >> save_to_hdfs_operator >> process_data_operator
failure_operator >> end_operator
