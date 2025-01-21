import urllib.request
from html.parser import HTMLParser
import re
import os
import logging

import tarfile

from concurrent.futures import ThreadPoolExecutor
import requests

def download_file(url):
    """
    Fetch data from the url via requests method.
    """
    response = requests.get(url)
    filename = url.split("/")[-1]
    with open(filename, mode="wb") as file:
        file.write(response.content) #Save the file within the cwd
    logger.info(f"Downloaded file {filename}")

class MyHTMLParser(HTMLParser):
    def __init__(self):
        super(MyHTMLParser, self).__init__()
        self.data = []
        self.filenames = []

    def handle_data(self, data):
        #Recover data
        self.data += [data]

class Get_data():
    def __init__(self, url):

        self.url = url
        self.content = None

        self.archive_list = None
        self.files_path = None #Store the path of all archives

        self.path_to_dir = None #Path to the directory storing the archives

    def load_html_page(self):
        """
        Load the content of the HTML Page from the site via its URL (Uniform Resource Locator)
        """
        opener = urllib.request.FancyURLopener({})
        f = opener.open(self.url)
        self.content = f.read().decode(encoding = 'utf-8')

    def parse_html(self):
        """
        Parse it via the library HTMLParser. Collect all tar.gz files from the HTML.
        """
        Parser = MyHTMLParser()
        Parser.feed(self.content)

        self.files_path = []
        self.archive_list = []
        self.filenames = Parser.data

        pattern = r'.tar.gz'
        for string in Parser.data:
            if string.endswith(pattern):
                self.files_path += [self.url + string]
                self.archive_list += [string]

    def download_http_request(self):
        """
        Download archive files from the webpage located at url.
        """

        self.parse_html()
        files = []
        with ThreadPoolExecutor() as executor:
            executor.map(download_file, self.files_path)

    def unzip_files(self):
        """
        Unzip file via tar library.
        """
        if not len(self.files_path):
            raise ValueError("No archive to unzip.")

        cwd = os.getcwd() #Get the current working directory

        for file in self.archive_list:
            tar = tarfile.open(cwd + '\\' + file) #Open the tarfile
            tar.extractall() #Extract the data from it.
            tar.close() #Close the tar opener
            os.remove(file) #Delete the host archive
    def run(self):
        self.load_html_page()
        self.download_http_request()
        self.unzip_files()

logging.basicConfig(filename = 'myapp.log', level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logger.info("Started")
    url = "https://echanges.dila.gouv.fr/OPENDATA/CASS/"
    get = Get_data(url)
    get.run()
    logger.info("Finished")