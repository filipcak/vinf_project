import pandas as pd


# merge files from procyclingstats and wiki
class DataMerger:
    def __init__(self):
        self.csv_file_path = 'parsed/'
        self.path_cycling = "data_cycling.csv"
        self.path_wiki = "wiki_spark.csv"
        self.path_merged = "merged.csv"

    def merge(self):
        # load cycling data
        df1 = pd.read_csv(f"{self.csv_file_path}{self.path_cycling}")
        # load wiki data
        df2 = pd.read_csv(f"{self.csv_file_path}{self.path_wiki}")
        # merge dataframes
        merged_df = pd.merge(df1, df2, on="arrival", how="left")
        # save the merged DataFrame to a new CSV file
        merged_df.to_csv(f"{self.csv_file_path}{self.path_merged}", index=False)

    def run(self):
        self.merge()


if __name__ == '__main__':
    DataMerger().run()
