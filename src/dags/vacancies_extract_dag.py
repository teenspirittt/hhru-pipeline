from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import re
from airflow import DAG
from datetime import datetime
from hdfs import InsecureClient


import requests
import json
import os


def extract_vacancies():
    date = datetime.today().strftime('%Y-%m-%d')
    # hhru api's URL
    url = 'https://api.hh.ru/vacancies'
    all_vacancies = []
    tags = [
    'python', 'java', 'sql', 'scala', 'rust',
    'data engineer', 'data scientist',
    'frontend', 'backend',
    'c', 'c++', 'c#', # add kotlin and R
    'android developer', 'ios developer'
    ]
    for tag in tags:
        tag_vacancies = []
        page = 0
        while True:
            params = {
                'text': tag,  # Поиск по текущему тегу
                'period': 30,  # Период поиска в днях
                'per_page': 100,  # Количество результатов на странице (максимум 200)
                'page': page,  # Номер страницы
            }
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                tag_vacancies += data['items']
                if data['pages'] <= page:
                    break
                page += 1
            else:
                print(f'Error for tag "{tag}": {response.status_code}')
                break
        
        all_vacancies += tag_vacancies
    
    if all_vacancies:
        with open(f'data/raw/{date}_vacancies.json', 'w') as file:
            json.dump(all_vacancies, file, ensure_ascii=False)


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


def save_merged_vacancies(output_filename, all_vacancies):
    with open(output_filename, 'w') as output_file:
        json.dump(all_vacancies, output_file, ensure_ascii=False)


def collect_vacancies():
    source_dir = 'data/raw'
    output_filename = 'data/raw/raw_vacancies.json'
    all_vacancies = []
    file_pattern = re.compile(r'\d{4}-\d{2}-\d{2}_vacancies\.json')

    for filename in os.listdir(source_dir):
        if file_pattern.match(filename):
            file_path = os.path.join(source_dir, filename)
            with open(file_path, 'r') as file:
                data = json.load(file)
                all_vacancies.extend(data)

    save_merged_vacancies(output_filename,all_vacancies)


def save_to_hdfs():
    src_path = f'data/raw/raw_vacancies.json'
    dest_dir = f"/hadoop-data/raw_vacancies.json"
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
    extract_vacancies_operator = PythonOperator(
        task_id='extract_vacancies',
        python_callable=extract_vacancies,
        dag=dag
    )

    check_file_operator = PythonOperator(
        task_id='check_file',
        python_callable=check_file,
        dag=dag,
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

    hr_actvity_operator = SparkSubmitOperator(
        task_id='hr_actvity_parquet',
        application='/opt/src/scala/target/scala-2.12/HRActivityAnalysis-assembly-1.0.jar',
        conn_id='spark_default',
        dag=dag
    )

    merge_vacancies = PythonOperator(
        task_id='merge_vacancies',
        python_callable=collect_vacancies,
        dag=dag
    )

extract_vacancies_operator >> check_file_operator >> branch_operator >> [
    success_operator, failure_operator]

success_operator >> merge_vacancies >> save_to_hdfs_operator >> hr_actvity_operator
failure_operator >> end_operator
