import psycopg2
import json
import os
import textwrap


from db_conf import config


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vacancies (
                id INTEGER PRIMARY KEY,
                name VARCHAR(500) NOT NULL,
                employer VARCHAR(200) NOT NULL,
                area VARCHAR(100) NOT NULL,
                salary_from INTEGER,
                salary_to INTEGER,
                salary_currency VARCHAR(50)
            );
        """)


def load_data(conn):
    with open('data/processed/vacancies.json', 'r') as f:
        vacancies = json.load(f)

    with conn.cursor() as cur:
        for vacancy in vacancies:
            cur.execute("""
                INSERT INTO vacancies (id, name, employer, area, salary_from, salary_to, salary_currency)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """, (
                vacancy['id'],
                '\n'.join(textwrap.wrap(vacancy['name'], width=30)),
                '\n'.join(textwrap.wrap(vacancy['employer'], width=30)),
                vacancy['area'],
                vacancy['salary_from'],
                vacancy['salary_to'],
                vacancy['salary_currency']
            ))

    conn.commit()


def connect():
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)

        create_table(conn)
        load_data(conn)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
