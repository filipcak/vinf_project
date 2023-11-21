import os
import json
import re
import csv
import sys
from merge import DataMerger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract


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
        self.path_wiki_regex = "wiki_data.csv"
        self.path_wiki = "wiki_spark.csv"
        self.path_wiki_data = "wiki/enwiki-latest-pages-articles.xml"
        self.wiki_parsed = "parsed/spark"

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
                    regex_together = re.findall("{{Infobox settlement\s*\|\s*name\s*=\s*([^|,<\n|&]*)[^*]*elevation_m\s*=\s*(\d+)",
                                                file_contents)
                    for i in regex_together:
                        # dictionary to store in csv
                        if i[0] == '' or i[1] == '':
                            continue
                        row = {'arrival': i[0], 'elevation': i[1]}
                        # for each file append to data_list to store at the end to csv file
                        self.data_list.append(row)

    # parse wiki data with spark
    def wiki_spark_parser(self):
        # initialize spark session
        spark = SparkSession.builder.appName("XML").getOrCreate()
        # load page to dataframe
        df = spark.read.format("xml").option("rowTag", "page").load(self.path_wiki_data)
        # extract the text field from the xml
        df = df.select("title", "revision.text._VALUE")
        # rename the column for easier access
        df = df.withColumnRenamed("_VALUE", "text")

        parsed_df = df\
            .withColumn("arrival", regexp_extract("text", r'Infobox settlement\s*\|\s*name\s*=\s*([^|,<\n|&]*)', 1))\
            .withColumn("elevation", regexp_extract("text", r'elevation_m\s*=\s*(\d+)', 1))
        # drop rows with empty values
        filtered_df = parsed_df.filter((col("arrival") != '') & (col("elevation") != ''))
        # select only arrival and elevation columns
        select_df = filtered_df.select("arrival", "elevation")

        # save to csv
        select_df.coalesce(1).write.csv(self.wiki_parsed, header=True, mode="overwrite")
        # stop the spark session
        spark.stop()

    # parse wiki data with spark
    def wiki_spark_parser_v2(self):
        # initialize Spark session
        spark = SparkSession.builder.appName("XML").getOrCreate()
        # load page to dataframe
        df = spark.read.format("xml").option("rowTag", "page").load(self.path_wiki_data)
        # extract the text field from the xml
        df = df.select("title", "revision.text._VALUE")
        # rename the column for easier access
        df = df.withColumnRenamed("_VALUE", "text")

        # convert dataframe to RDD for processing
        rdd = df.rdd
        # use map with lambda to apply a function to each row
        parsed_rdd = rdd.map(lambda row: (
            re.findall(r'{{Infobox settlement\s*\|\s*name\s*=\s*([^|,<\n|&]*)[^*]*elevation_m\s*=\s*(\d+)',
                       row.text) if row.text else [('', '')]
        ))
        parsed_rdd = parsed_rdd.flatMap(lambda x: x)

        # convert the resulting RDD to a dataframe
        result_df = spark.createDataFrame(parsed_rdd, ["arrival", "elevation"])
        # filter out rows with empty values
        result_df = result_df.filter(col("arrival") != '').filter(col("elevation") != '')

        # save to csv
        result_df.coalesce(1).write.csv(self.wiki_parsed, header=True, mode="overwrite")
        # stop the spark session
        spark.stop()

    def run(self):
        # user input - if p - parse, if m - merge, else - exit
        user_input = sys.argv[1] if len(sys.argv) > 1 else None
        # user_input_type - if procyclingstats - parse procyclingstats, if wiki - parse wiki, else - exit
        user_input_type = sys.argv[2] if len(sys.argv) > 2 else None
        if user_input == "p":
            if user_input_type == "procyclingstats":
                self.parse()
                self.save_to_csv(self.path_cycling)
                print("Parsed procyclingstats.")
            elif user_input_type == "wiki":
                self.wiki_spark_parser_v2()
                print("Parsed wiki.")
            else:
                print("Wrong type, will not parse.")
        elif user_input == "m":
            DataMerger().run()
            print("Merged.")
        else:
            print("Will not merge or parse.")


if __name__ == '__main__':
    Parser().run()
