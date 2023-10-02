import requests
import re
import json
from datetime import datetime
from collections import deque
import time


# Function to download and store a webpage
def download_and_store(url, visited_pages, initial_url, max_urls, found_pages):
    queue = deque([url])
    counter = 0
    params = {
        "contact": "true",
        "email": "xcak@stuba.sk",
        "project": "VINF",
        "university": "Slovak University of Technology",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
    }

    for found_page in found_pages:
        if found_page not in visited_pages:
            queue.append(initial_url + found_page)

    while queue and len(visited_pages) < max_urls:
        url = queue.popleft()

        # Check if URL already visited
        if url not in visited_pages or url == initial_url:
            # Increment the counter
            counter += 1
            # Check if the counter is a multiple of 990
            if counter % 200 == 0:
                print("Sleeping for 30 seconds...")
                time.sleep(30)
            # Make the GET request
            try:
                print(url)
                response = requests.get(url, headers=headers, params=params, timeout=10)
                print(response.status_code)
                # Check if the request was successful
                if response.status_code == 200:
                    # Access the content of the webpage
                    webpage_content = response.text

                    # Store the HTML content in a txt file
                    if url != initial_url:
                        filename = f'page_{datetime.utcnow().strftime("%Y%m%d_%H%M%S%f")}.txt'
                        with open("data/" + filename, 'w+', encoding='utf-8') as file:
                            file.write(webpage_content)
                        print(f"Stored '{filename}'")

                    # Mark the URL as visited to avoid revisiting it
                    visited_pages.add(url)

                    # Use regular expressions to find links with "race" in href attribute
                    url_pattern = re.compile(r'href=["\'](race/[^\s"\'<>]*)')
                    matched_urls = url_pattern.findall(webpage_content)

                    # Add matched URLs to the queue for further processing
                    for matched_url in matched_urls:
                        if matched_url not in found_pages:
                            found_pages.add(matched_url)
                            matched_url = initial_url + matched_url
                            queue.append(matched_url)
                else:
                    print(f"Failed to retrieve webpage. Status code: {response.status_code}")
            except Exception as e:
                print(f"Request error: {e}")


def run():
    # Initial URL to visit
    initial_url = 'https://procyclingstats.com/'
    max_urls = 5000

    try:
        with open('visited_urls.json', 'r', encoding='utf-8') as json_file:
            visited_pages = set(json.load(json_file))
            max_urls += len(visited_pages)
    except FileNotFoundError:
        visited_pages = set()

    try:
        with open('found_urls.json', 'r', encoding='utf-8') as json_file:
            found_pages = set(json.load(json_file))
    except FileNotFoundError:
        found_pages = set()

    # Start downloading and storing pages from the initial URL, limiting to 5 URLs
    download_and_store(initial_url, visited_pages, initial_url, max_urls, found_pages)

    # Store the visited URLs in a JSON file
    with open('visited_urls.json', 'w', encoding='utf-8') as json_file:
        json.dump(list(visited_pages), json_file, indent=4)

    # Store the found URLs in a JSON file
    with open('found_urls.json', 'w', encoding='utf-8') as json_file:
        json.dump(list(found_pages), json_file, indent=4)


if __name__ == '__main__':
    run()
