
from data_loader import *
from to_db import connect
from data_preprocessing import preprocess_data


if __name__ == "__main__":
    vacancies = get_vacancies()
    save_vacancies(vacancies)
    preprocess_data("data/raw", "data/processed")
    connect()
