import requests
import re
import json
from datetime import datetime


# Function to download and store a webpage
def download_and_store(url, visited_pages, initial_url, max_urls, found_pages):
    try:
        # Check if URL already visited
        if url not in visited_pages or url == initial_url:
            params = {
                "contact": "true",
                "email": "xcak@stuba.sk",
                "project": "VINF",
                "university": "Slovak University of Technology",
            }
            # Make the GET request
            response = requests.get(url, params=params)

            # Check if the request was successful
            if response.status_code == 200:
                # Access the content of the webpage
                webpage_content = response.text

                # Store the HTML content in a txt file
                filename = f'page_{datetime.utcnow().strftime("%Y%m%d_%H%M%S%f")}.txt'
                with open("data/" + filename, 'w+', encoding='utf-8') as file:
                    file.write(webpage_content)
                print(f"Stored '{filename}'")

                # Mark the URL as visited to avoid revisiting it
                visited_pages.append(url)

                # Use regular expressions to find links with "race" in href attribute
                url_pattern = re.compile(r'href=["\'](race[^\s"\'<>]*)')
                matched_urls = url_pattern.findall(webpage_content)

                for matched_url in matched_urls:
                    if matched_url not in found_pages:
                        found_pages.append(matched_url)

                # Recursively visit linked pages
                for matched_url in matched_urls:
                    matched_url = initial_url + matched_url
                    # Check if maximum pages to visit has been reached
                    if len(visited_pages) < max_urls:
                        download_and_store(matched_url, visited_pages, initial_url, max_urls, found_pages)
            else:
                print(f"Failed to retrieve webpage. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")


def run():
    # Initial URL to visit
    initial_url = 'https://procyclingstats.com/'
    max_urls = 10

    try:
        with open('visited_urls.json', 'r', encoding='utf-8') as json_file:
            visited_pages = json.load(json_file)
            max_urls += len(visited_pages)
    except FileNotFoundError:
        visited_pages = []

    try:
        with open('found_urls.json', 'r', encoding='utf-8') as json_file:
            found_pages = json.load(json_file)
    except FileNotFoundError:
        found_pages = []

    # Start downloading and storing pages from the initial URL, limiting to 5 URLs
    download_and_store(initial_url, visited_pages, initial_url, max_urls, found_pages)

    # Store the visited URLs in a JSON file
    with open('visited_urls.json', 'w+', encoding='utf-8') as json_file:
        json.dump(list(visited_pages), json_file, indent=4)

        # Store the visited URLs in a JSON file
    with open('found_urls.json', 'w', encoding='utf-8') as json_file:
        json.dump(list(found_pages), json_file, indent=4)


if __name__ == '__main__':
    run()