import requests
import json
import os


def save_vacancies(vacancies):
    with open('data/raw/vacancies.json', 'w') as f:
        json.dump(vacancies, f, ensure_ascii=False)


def get_vacancies():
    url = "https://api.hh.ru/vacancies"
    params = {
        "text": "python developer",
        "area": 1,  # код региона, в данном случае Москва
        "per_page": 100,  # количество вакансий на одной странице
        "page": 0,  # номер страницы
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
    }
    vacancies = []

    while True:
        response = requests.get(url, params=params, headers=headers)
        data = response.json()

        if 'items' not in data:
            break

        if not data['items']:
            break

        for item in data['items']:
            vacancy = {
                'id': item['id'],
                'name': item['name'],
                'employer': item['employer']['name'],
                'area': item['area']['name'],
                'salary_from': None,
                'salary_to': None,
                'salary_currency': None
            }

            if item['salary'] and item['salary']['from']:
                vacancy['salary_from'] = item['salary']['from']

            if item['salary'] and item['salary']['to']:
                vacancy['salary_to'] = item['salary']['to']

            if item['salary'] and item['salary']['currency']:
                vacancy['salary_currency'] = item['salary']['currency']

            vacancies.append(vacancy)

        params['page'] += 1

    return vacancies


if __name__ == "__main__":
    vacancies = get_vacancies()
    save_vacancies(vacancies)
