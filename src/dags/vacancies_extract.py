import requests
import json
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


def extract_vacancies():
    date = datetime.today().strftime('%Y-%m-%d')
    # hhru api`s URL
    url = 'https://api.hh.ru/vacancies'
    params = {
        'text': 'python developer',
        'area': 1,
        'period': 30,
        'per_page': 100
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        with open(f'{date}_vacancies.json', 'w') as file:
            json.dump(data, file)
    else:
        print(f'Error: {response.status_code}')


args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 19),
    'retries': 1
}


with DAG(
    'hh_api_vacancies',
    default_args=args,
    description='Extract vacancies from hh.ru API',
    schedule_interval='*/2 * * * *',
    catchup=False
) as dag:
    # operator for func execution
    extract_vacancies_operator = PythonOperator(
        task_id='extract_vacancies',
        python_callable=extract_vacancies,
        dag=dag
    )

    # operator for check if json exist
    check_file_operator = BashOperator(
        task_id='check_file',
        bash_command='{{ ds }}_vacancies.json',
        dag=dag
    )

    extract_vacancies_operator >> check_file_operator
