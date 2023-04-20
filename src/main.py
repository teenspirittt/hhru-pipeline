
from data_loader import *
from to_db import connect
from data_preprocessing import preprocess_data


if __name__ == "__main__":
    keywords = 'Data Engineer'
    area_id = 1
    page = 0

    vacancies = get_vacancies()
    save_vacancies(vacancies)
    preprocess_data("data/raw", "data/processed")
    connect()


def load_vacancies():
    all_vacansies = []
    page = 0
    while True:
        vacancies = get_vacancies(keywords, area_id, page=page)
        if not vacancies:
            break
        all_vacansies.extend(vacancies)
        page += 1

    parsed_vacancies = [parse_vacansy(vacancy) for vacancy in all_v]
