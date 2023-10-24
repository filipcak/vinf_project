import os
import json
import re


class Parser:
    def __init__(self):
        self.folder_path = 'test_data/'
        with open('regex.json', 'r', encoding='utf-8') as json_file:
            self.regex = json.load(json_file)

    def parser(self):
        folder_path = self.folder_path

        # List all files in the folder
        files = os.listdir(folder_path)

        # Filter for .txt files
        txt_files = [file for file in files if file.endswith('.txt')]

        # Process each .txt file
        for i, txt_file in enumerate(txt_files):
            if i >= 5:  # Stop processing after the first 10 files
                break
            file_path = os.path.join(folder_path, txt_file)
            with open(file_path, 'r') as file:
                file_contents = file.read()
                title_match = re.findall(self.regex['title'], file_contents)
                year = re.findall(self.regex['year'], file_contents)
                date = re.findall(self.regex['date'], file_contents)
                start_time = re.findall(self.regex['startTime'], file_contents)
                avg_speed = re.findall(self.regex['avgSpeed'], file_contents)
                distance = re.findall(self.regex['distance'], file_contents)
                race_category = re.findall(self.regex['raceCategory'], file_contents)
                profile_score = re.findall(self.regex['profileScore'], file_contents)
                vertical_meters = re.findall(self.regex['verticalMeters'], file_contents)
                departure = re.findall(self.regex['departure'], file_contents)
                arrival = re.findall(self.regex['arrival'], file_contents)
                race_ranking = re.findall(self.regex['raceRanking'], file_contents)
                startlist_score = re.findall(self.regex['startlistScore'], file_contents)
                how_won = re.findall(self.regex['howWon'], file_contents)
                avg_temperature = re.findall(self.regex['avgTemperature'], file_contents)
                # take just first element of the list
                winner_time = re.findall(self.regex['winnerTime'], file_contents)
                name_place = re.findall(self.regex['namePlace'].replace("placeholder", "9"), file_contents)
                team_place = re.findall(self.regex['teamPlace'].replace("placeholder", "9"), file_contents)
                is_stage = re.findall(self.regex['isStage'], file_contents)
                if is_stage:
                    name_place = re.findall(self.regex['stage']['namePlace'].replace("placeholder", "9"), file_contents)
                    team_place = re.findall(self.regex['stage']['teamPlace'].replace("placeholder", "9"), file_contents)
                #print(title_match, year, date)
                #print(name_place, is_stage)

    def run(self):
        # Initial URL to visit
        self.parser()


if __name__ == '__main__':
    Parser().run()
