import os
import json
import re
import csv
import pandas as pd

class Parser:
    def __init__(self):
        self.folder_path = 'data/'
        self.csv_file_path = 'parsed/'
        self.wiki_files = 'wiki/'
        self.regex_keys = ["title", "year", "date", "startTime", "avgSpeed", "distance", "raceCategory", "profileScore",
                           "verticalMeters", "arrival", "raceRanking", "startlistScore", "howWon",
                           "avgTemperature", "winnerTime", "stageNumber"]
        self.regex_keys_stage = ["nameRider", "teamRider", "flagRider", "bibNumberRider"]
        self.data_list = []
        with open('regex.json', 'r', encoding='utf-8') as json_file:
            self.regex = json.load(json_file)
        self.path_cycling = "data_cycling.csv"
        self.path_wiki = "data_wiki.csv"
        self.path_merged = "merged.csv"

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
                try:
                    title = re.findall(self.regex["title"], file_contents)[0].strip()
                except IndexError:
                    title = ''
                if title != '':
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
    def save_to_csv(self, file_path_type):
        # Define the column names
        fieldnames = self.data_list[0].keys()

        # Write the data to the CSV file
        with open(f"{self.csv_file_path}{file_path_type}", 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            # write data headers
            writer.writeheader()
            # write the data rows
            writer.writerows(self.data_list)

    # parse data from wikipedia
    def wiki_parser(self):
        # all files in the folder
        files = os.listdir(self.wiki_files)

        for file_name in files:
            file_path = os.path.join(self.wiki_files, file_name)

            # check if the item is a file
            if os.path.isfile(file_path):
                with open(file_path, 'r') as file:
                    file_contents = file.read()
                    # regex to find name ( city name ) - {{Infobox settlement\s*\|\s*name\s*=\s*([^|\n|&]*)
                    # regex to find elevation -  {Infobox settlement[^*]*elevation_m\s*=\s*(\d+)
                    # regex to get those information together
                    regex_together = re.findall("{{Infobox settlement\s*\|\s*name\s*=\s*([^|\n|&]*)[^*]*elevation_m\s*=\s*(\d+)",
                                                file_contents)
                    for i in regex_together:
                        # dictionary to store in csv
                        if i[0] == '' or i[1] == '':
                            continue
                        row = {'arrival': i[0], 'elevation': i[1]}
                        # for each file append to data_list to store at the end to csv file
                        self.data_list.append(row)

    # merge files from procyclingstats and wiki
    def merge(self):
        # load cycling data
        df1 = pd.read_csv(f"{self.csv_file_path}{self.path_cycling}")
        # load wiki data
        df2 = pd.read_csv(f"{self.csv_file_path}{self.path_wiki}")

        merged_df = pd.merge(df1, df2, on="arrival", how="left")
        # save the merged DataFrame to a new CSV file
        merged_df.to_csv(f"{self.csv_file_path}merged.csv", index=False)

    def run(self, type_run):
        if type_run == "procyclingstats":
            self.parse()
            self.save_to_csv(self.path_cycling)
        elif type_run == "wiki":
            self.wiki_parser()
            self.save_to_csv(self.path_wiki)
        elif type_run == "merge":
            self.merge()
        else:
            print("wrong type")


if __name__ == '__main__':
    Parser().run(type_run="merge")
