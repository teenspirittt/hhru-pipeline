from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime
from pyspark.sql import SparkSession

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
    HADOOP_USER_NAME = 'hdfs'
    HADOOP_HOST = 'hadoop'
    HADOOP_PORT = '9000'
    HADOOP_PREFIX = f'hdfs://{HADOOP_USER_NAME}@{HADOOP_HOST}:{HADOOP_PORT}'
    spark = SparkSession.builder.appName('hh_vacancies').getOrCreate()
    date = datetime.today().strftime('%Y-%m-%d')
    input_file = f'/usr/local/airflow/data/raw/{date}_vacancies.json'
    output_dir = f'{HADOOP_PREFIX}/user/hadoop/hh_vacancies'
    df = spark.read.json(input_file)
    df.write.mode('overwrite').option('compression', 'gzip').json(output_dir)
    spark.stop()


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

extract_vacancies_operator >> check_file_operator >> branch_operator >> [
    success_operator, failure_operator]

success_operator >> save_to_hdfs_operator
failure_operator >> end_operator
