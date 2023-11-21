import lucene
import pandas as pd
import os
from datetime import datetime

from java.nio.file import Paths
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.index import IndexOptions, IndexWriter, IndexWriterConfig, DirectoryReader
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.search import BooleanClause, BooleanQuery, TermQuery, TermRangeQuery
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.analysis.core import KeywordAnalyzer
from org.apache.lucene.index import Term


class Indexer:
    def __init__(self):
        self.enviroment = lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        self.directory = MMapDirectory(Paths.get('index'))
        self.parsed_data_path = "/workspaces/vinf_project/parsed/merged.csv"
        self.analyzer = KeywordAnalyzer()
        self.index_folder = '/workspaces/vinf_project/index'

    @staticmethod
    def create_field_type(self):
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
            field_type = self.create_field_type(self)

            # iterate over CSV 
            for idx, row in csv_data.iterrows():
                # lucene document
                doc = Document()

                # add fields
                # row 0 - title (name of the race), row 1 - year, row 9 - arrival (place), -1 - elevation
                # row 15 - stage, row 16 - winner, row 2 - date, row 5 - distance
                for i in [0, 1, 9, -1, 15, 16, 2, 5]:
                    column_name = csv_data.columns[i]
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
            print("The year must be between 1876 and ", datetime.now().year, "ideally between 1900 and ", datetime.now().year)
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
        title_query = TermQuery(Term("year", str(year)))
        year_query = TermRangeQuery.newStringRange("elevation", str(elevation_start), str(elevation_end), True, True)

        # add the queries to the BooleanQuery
        boolean_query.add(title_query, BooleanClause.Occur.MUST)
        boolean_query.add(year_query, BooleanClause.Occur.MUST)

        # perform the search
        records = searcher.search(boolean_query.build(), 1000).scoreDocs

        if len(records) == 0:
            print("No results found")
        else:
            # iterate through results:
            for record in records:
                record_doc = searcher.doc(record.doc)
                print(record_doc['title'], record_doc['stageNumber'], "distance: ", record_doc['distance'],
                      "vertical metres -", record_doc['elevation'], "in arrival city:", record_doc['arrival'])

        print('Number of results found: ', len(records))


if __name__ == '__main__':
    index = Indexer()
    is_index = input("Do you want to use index?: (y/n)")
    if is_index == 'y':
        index.indexer()
    index.search()
