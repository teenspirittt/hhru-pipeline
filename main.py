import requests
import json

url = 'https://api.hh.ru/vacancies'
params = {
    'text': 'python',  # work
    'area': 1,  # region code
    'per_page': 100,
}

response = requests.get(url, params=params)
if response.status_code == 200:
    data = json.loads(response.text)
    vacancies = data['items']
    # обработка полученных данных
else:
    print('Error:', response.status_code)
