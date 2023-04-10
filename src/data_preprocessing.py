import json

with open('data/raw/vacancies.json', 'r') as f:
    raw_data = json.load(f)

unique_data = [dict(t) for t in {tuple(d.items()) for d in raw_data}]

with open('data/processed/vacancies.json', 'w') as f:
    json.dump(unique_data, f)
