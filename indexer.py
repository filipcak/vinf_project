import lucene
import pandas as pd
import os

from java.nio.file import Paths
from org.apache.lucene.analysis.core import KeywordAnalyzer
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.index import IndexOptions, IndexWriter, IndexWriterConfig, DirectoryReader
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.search import BooleanClause, BooleanQuery, TermQuery
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.store import MMapDirectory
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.analysis.core import KeywordAnalyzer
from org.apache.lucene.index import Term


class Indexer:
    def __init__(self):
        self.enviroment = lucene.initVM(vmargs=['-Djava.awt.headless=true'])
        self.directory = MMapDirectory(Paths.get('index'))
        self.parsed_data_path = "/workspaces/vinf_project/parsed/test_data.csv"
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
                # row 0 - title (name of the race), row 1 - year, row 9 - departure (place), row 10 - arrival (place)
                for i in [0, 1, 9, 10]:
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
        reader = DirectoryReader.open(self.directory)
        searcher = IndexSearcher(reader)

        parser = QueryParser("title", KeywordAnalyzer())
        # get race name from user 
        print("potential races: Tour de France, Giro d'Italia, Vuelta a España, Okolo Slovenska,... ")
        race_name = input("Enter the name of the race: ")
        query = parser.parse(race_name)

        records = searcher.search(query, 1500).scoreDocs

        boolean_query = BooleanQuery.Builder()

        # Specify the fields and terms you want to search for
        title_query = TermQuery(Term("title", "Volta Ciclista a Catalunya"))
        year_query = TermQuery(Term("year", "2022"))

        # Add the queries to the BooleanQuery
        boolean_query.add(title_query, BooleanClause.Occur.MUST)
        boolean_query.add(year_query, BooleanClause.Occur.MUST)

        # Perform the search
        records = searcher.search(boolean_query.build(), 1000).scoreDocs

        # iterate through results:
        for record in records:
            recordDoc = searcher.doc(record.doc)
            if recordDoc['departure'] != 'nan':
                print(recordDoc['title'], recordDoc['year'], "departure: ", recordDoc['departure'])

        print('Number of results found for the race',len(records))


if __name__ == '__main__':
    index = Indexer()
    is_index = input("Do you want to use index?: (y/n)")
    if is_index == 'y':
        index.indexer()
    index.search()
