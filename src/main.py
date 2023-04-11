
from data_loader import *
from to_db import connect


if __name__ == "__main__":
    vacancies = get_vacancies()
    save_vacancies(vacancies)
    connect()
