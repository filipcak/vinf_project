import lucene
import pandas as pd
import os
from datetime import datetime
from rich.console import Console
from rich.table import Table

from java.nio.file import Paths
from org.apache.lucene.document import Document, Field, FieldType, IntPoint
from org.apache.lucene.index import IndexOptions, IndexWriter, IndexWriterConfig, DirectoryReader, Term
from org.apache.lucene.search import IndexSearcher, BooleanClause, BooleanQuery, TermQuery, PointRangeQuery
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.analysis.core import KeywordAnalyzer


class Indexer:
    def __init__(self):
        self.enviroment = lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        self.directory = MMapDirectory(Paths.get('index'))
        self.parsed_data_path = "/workspaces/vinf_project/parsed/merged.csv"
        self.analyzer = KeywordAnalyzer()
        self.index_folder = '/workspaces/vinf_project/index'

    @staticmethod
    def create_field_type():
        field_type = FieldType()
        field_type.setStored(True)
        field_type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)
        return field_type

    @staticmethod
    def delete_files_in_subfolder(self):
        try:
            # list all files in the subfolder
            files = os.listdir(self.index_folder)

            # iterate over the files and delete each one
            for file_name in files:
                file_path = os.path.join(self.index_folder, file_name)
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"Deleted file: {file_path}")

            print("All files in the subfolder have been deleted.")

        except Exception as e:
            print(f"An error occurred: {str(e)}")

    # create index for parsed data
    def indexer(self):
        self.delete_files_in_subfolder(self)
        try:
            # load parsed data
            csv_data = pd.read_csv(self.parsed_data_path)
            print(f"Loaded {csv_data.shape[0]} rows from {self.parsed_data_path}")

            # lucene indexer
            writer = IndexWriter(self.directory, IndexWriterConfig(self.analyzer))

            # set field type
            field_type = self.create_field_type()

            # iterate over CSV 
            for idx, row in csv_data.iterrows():
                # lucene document
                doc = Document()

                # add fields
                # row 0 - title (name of the race), row 1 - year, row 9 - arrival (place), -1 - elevation
                # row 15 - stage, row 16 - winner, row 2 - date, row 5 - distance
                # row 17 - winner team, row 19 - winner bib, row 20 - 2nd place, row 21 - 2nd place team
                # row 23 - second place bib, row 24 - 3rd place, row 25 - 3rd place team, row 27 - 3rd place bib
                for i in [1, -1, 9, 0, 15, 16, 5, 17, 19, 20, 21, 23, 24, 25, 27, 2]:
                    column_name = csv_data.columns[i]
                    if column_name == "elevation":
                        elevation_value = int(row[column_name]) if not pd.isna(row[column_name]) else -1
                        doc.add(IntPoint(column_name, elevation_value))
                        doc.add(Field("elevation", str(row[column_name]), field_type))
                    else:
                        doc.add(Field(column_name, str(row[column_name]), field_type))

                # add the document to the index
                writer.addDocument(doc)

            print(f"indexed")
            writer.commit()
            writer.close()

        except Exception as e:
            print(f"An error occurred: {str(e)}")

    def search(self):
        year = 1875
        elevation_start = 9000
        elevation_end = 9000
        reader = DirectoryReader.open(self.directory)
        searcher = IndexSearcher(reader)

        # get year and elevation from user
        while year < 1876 or year > datetime.now().year:
            print("The year must be between 1876 and ", datetime.now().year, "ideally between 1900 and ",
                  datetime.now().year)
            try:
                year = int(input("Type what year do you want to search for: "))
            except ValueError:
                print("The year must be a number")
        while elevation_start < 0 or elevation_start > 3000:
            print("The elevation must be between 0 annd 3000")
            try:
                elevation_start = int(input("Type what elevation START range do you want to seach for: "))
            except ValueError:
                print("The elevation must be a number")
        while elevation_end < 0 or elevation_end > 3000:
            print("The elevation must be between 0 and 3000")
            try:
                elevation_end = int(input("Type what elevation END range do you want to seach for: "))
            except ValueError:
                print("The elevation must be a number")

        boolean_query = BooleanQuery.Builder()

        # specify the fields and terms you want to search for
        year_query = TermQuery(Term("year", str(year)))
        elevation_query = IntPoint.newRangeQuery("elevation", elevation_start, elevation_end)

        # add the queries to the BooleanQuery
        boolean_query.add(year_query, BooleanClause.Occur.MUST)
        boolean_query.add(elevation_query, BooleanClause.Occur.MUST)

        # perform the search
        records = searcher.search(boolean_query.build(), 1000).scoreDocs

        if len(records) == 0:
            print("No results found")
        else:
            table = Table(show_header=True, header_style="bold blue", show_lines=True)
            # prepare table headers
            headers = ["Elevation", "Arrival City", "Year", "Race", "Race distance", "Stage No.", "Date",
                       "1st Rider Name", "1st Rider Team", "1st Rider Bib", "2nd Rider Name", "2nd Rider Team",
                       "2nd Rider Bib", "3rd Rider Name", "3rd Rider Team", "3rd Rider Bib"]
            for header in headers:
                table.add_column(header)

            # prepare table data
            table_data = []
            unique_records = set()  # Use a set to store unique records
            for record in records:
                record_doc = searcher.doc(record.doc)

                # create a tuple of relevant fields
                unique_key = (
                    record_doc['elevation'],
                    record_doc['arrival'],
                    record_doc['year'],
                    record_doc['title'],
                    record_doc['distance'],
                    '' if str(record_doc['stageNumber']).lower() == 'nan' else str(record_doc['stageNumber']),
                    record_doc['date'],
                    '' if str(record_doc['nameRider_1']).lower() == 'nan' else str(record_doc['nameRider_1']),
                    '' if str(record_doc['teamRider_1']).lower() == 'nan' else str(record_doc['teamRider_1']),
                    '' if str(record_doc['bibNumberRider_1']).lower() == 'nan' else str(
                        int(float(record_doc['bibNumberRider_1']))),
                    '' if str(record_doc['nameRider_2']).lower() == 'nan' else str(record_doc['nameRider_2']),
                    '' if str(record_doc['teamRider_2']).lower() == 'nan' else str(record_doc['teamRider_2']),
                    '' if str(record_doc['bibNumberRider_2']).lower() == 'nan' else str(
                        int(float(record_doc['bibNumberRider_2']))),
                    '' if str(record_doc['nameRider_3']).lower() == 'nan' else str(record_doc['nameRider_3']),
                    '' if str(record_doc['teamRider_3']).lower() == 'nan' else str(record_doc['teamRider_3']),
                    '' if str(record_doc['bibNumberRider_3']).lower() == 'nan' else str(
                        int(float(record_doc['bibNumberRider_3']))),
                )

                # check if the record is unique before adding it to the table_data
                if unique_key not in unique_records:
                    table_data.append(list(unique_key))
                    unique_records.add(unique_key)
            # sort based on elevation
            table_data.sort(key=lambda x: float(x[0]))

            # add to table for better display
            for row in table_data:
                table.add_row(*row)

            # display the table
            console = Console()
            console.print(table)

            print('Number of results found: ', len(table_data))


if __name__ == '__main__':
    index = Indexer()
    is_index = input("Do you want to use index?: (y/n)")
    if is_index == 'y':
        index.indexer()
    index.search()
