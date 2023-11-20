import csv
import json


# create index for parsed data
def create_index(csv_file_path, index_file_path):
    index = {}
    # open csv file
    with open(csv_file_path, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            title = row["title"]
            year = row["year"]

            # creating a key string (title_year)
            key = f"{title}_{year}"

            # indexing the record with the  key
            if key not in index:
                index[key] = []
            # appending the record to the list of records for the key
            index[key].append(row)
    # save index to json file
    with open(index_file_path, 'w') as index_file:
        json.dump(index, index_file)


# load index from json file
def load_index(index_file_path):
    with open(index_file_path, 'r') as index_file:
        index = json.load(index_file)
    return index


# get record from index - simple search with
def get_record(index_file_path):
    loaded_index = load_index(index_file_path)

    # retrieve data based on title and year from user
    user_input_race_name = input("Enter race name (example: Giro d'Italia): ")
    user_input_year = input("Enter year (example: 2017): ")
    # create key
    key = f"{user_input_race_name}_{user_input_year}"
    # get records for key
    records = loaded_index.get(key, [])
    # print records
    if records:
        print(f"Records for key '{key}':")
        for record in records:
            print(record)
    else:
        print(f"No records found for key '{key}'.")


# driver function
def run():
    csv_file_path = "parsed/merged.csv"
    index_file_path = "index/index.json"
    # create index
    user_input = input("Do you want to create an index?: ")
    if user_input == "y" or user_input == "yes":
        create_index(csv_file_path, index_file_path)
    # search index
    user_input = input("Do you want to perform a search?: ")
    if user_input == "y" or user_input == "yes":
        get_record(index_file_path)


if __name__ == '__main__':
    run()
