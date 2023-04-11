import json
import os


def load_data_from_file(file_path):
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data


def remove_duplicates(data, key='id'):
    unique_data = []
    ids = set()
    for item in data:
        if item[key] not in ids:
            unique_data.append(item)
            ids.add(item[key])
    return unique_data


def save_data_to_file(data, file_path):
    with open(file_path, 'w') as f:
        json.dump(data, f)


def preprocess_data(raw_dir, processed_dir):
    if not os.path.exists(processed_dir):
        os.makedirs(processed_dir)

    data = load_data_from_file(os.path.join(raw_dir, 'vacancies.json'))
    print(len(data))
    data = remove_duplicates(data)
    print(len(data))
    save_data_to_file(data, os.path.join(
        processed_dir, 'vacancies_processed.json'))
