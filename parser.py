import os


def parser():
    folder_path = 'data_test/'

    # List all files in the folder
    files = os.listdir(folder_path)

    # Filter for .txt files
    txt_files = [file for file in files if file.endswith('.txt')]

    # Process each .txt file
    for txt_file in txt_files:
        file_path = os.path.join(folder_path, txt_file)
        with open(file_path, 'r') as file:
            file_contents = file.read()
            print(file_contents)


def run():
    # Initial URL to visit
    initial_url = 'https://procyclingstats.com/'
    max_urls = 1000


if __name__ == '__main__':
    run()
