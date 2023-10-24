import os
import json
import re
import csv


class Parser:
    def __init__(self):
        self.folder_path = 'data/'
        self.csv_file_path = 'parsed/test_data.csv'
        self.regex_keys = ["title", "year", "date", "startTime", "avgSpeed", "distance", "raceCategory", "profileScore",
                           "verticalMeters", "departure", "arrival", "raceRanking", "startlistScore", "howWon",
                           "avgTemperature", "winnerTime", "stageNumber"]
        self.regex_keys_stage = ["nameRider", "teamRider", "flagRider", "bibNumberRider"]
        self.data_list = []
        with open('regex.json', 'r', encoding='utf-8') as json_file:
            self.regex = json.load(json_file)

    # parse data from downloaded pages
    def parse(self):
        # all files in the folder
        files = os.listdir(self.folder_path)

        # filter .txt files - just to be sure
        txt_files = [file for file in files if file.endswith('.txt')]

        # Process each file
        for txt_file in txt_files:
            file_path = os.path.join(self.folder_path, txt_file)
            # open file to read
            with open(file_path, 'r') as file:
                file_contents = file.read()
                # dictionary to store in csv
                row = {}
                # add default information
                for key in self.regex_keys:
                    try:
                        row[key] = re.findall(self.regex[key], file_contents)[0].strip()
                    except IndexError:
                        row[key] = ''
                # add values based on race type for 10 riders
                for i in range(1, 11):
                    for key in self.regex_keys_stage:
                        # if it is stage - change the regex
                        if row['stageNumber'] != '':
                            try:
                                row[f'{key}_{i}'] = re.findall(self.regex['stage'][key].replace("placeholder", str(i)),
                                                               file_contents)[0]
                            except IndexError:
                                row[f'{key}_{i}'] = ''
                        else:
                            try:
                                row[f'{key}_{i}'] = re.findall(self.regex[key].replace("placeholder", str(i)),
                                                               file_contents)[0]
                            except IndexError:
                                row[f'{key}_{i}'] = ''
                # for each file append to data_list to store at the end to csv file
                self.data_list.append(row)

    # save to csv
    def save_to_csv(self):
        # Define the column names
        fieldnames = self.data_list[0].keys()

        # Write the data to the CSV file
        with open(self.csv_file_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            # write data headers
            writer.writeheader()
            # write the data rows
            writer.writerows(self.data_list)

    def run(self):
        self.parse()
        self.save_to_csv()


if __name__ == '__main__':
    Parser().run()
