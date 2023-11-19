import csv
import json


def create_index(csv_file_path, index_file_path):
    index = {}

    with open(csv_file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            title = row["title"]

            # Indexing the record with the title
            if title not in index:
                index[title] = []

            index[title].append(row)

    with open(index_file_path, 'w') as index_file:
        json.dump(index, index_file)


def load_index(index_file_path):
    with open(index_file_path, 'r') as index_file:
        index = json.load(index_file)
    return index


def get_record(index_file_path):
    loaded_index = load_index(index_file_path)
    # Retrieve data based on title
    title_to_lookup = "Okolo Slovenska"
    records = loaded_index.get(title_to_lookup, [])

    if records:
        print(f"Records for title '{title_to_lookup}':")
        for record in records:
            print(record)
    else:
        print(f"No records found for title '{title_to_lookup}'.")


def run():
    csv_file_path = "parsed/merged.csv"
    index_file_path = "index/index.json"
    create_index(csv_file_path, index_file_path)
    get_record(index_file_path)


if __name__ == '__main__':
    run()