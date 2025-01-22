import urllib.request
from html.parser import HTMLParser
import re
import os
import logging
from urllib.parse import urljoin
import io

import tarfile

from concurrent.futures import ThreadPoolExecutor
import requests

class MyHTMLParser(HTMLParser):
    def __init__(self):
        super(MyHTMLParser, self).__init__()
        self.data = []
        self.filenames = []

    def handle_data(self, data):
        self.data += [data]

def scrap_archive_path(url, timeout: int):
    """
    Fetch data from the HTML webpage via a HTTP request.
    Args:
        - url: URL of the HTML page to scrap
        - timeout: HTTP request timeout. This is the maximum time the client will attempt to connect to the site.
    """
    resp = requests.get(url = url, timeout = timeout)

    parser = MyHTMLParser()
    parser.feed(resp.content.decode('utf-8'))
    parser.close()
    return filter(lambda url: url.endswith(".tar.gz"), parser.data) #Return an iterator

def main(timeout: int):

    logger=logging.getLogger(__name__)
    logging.basicConfig(filename = 'stdout.log', level=logging.INFO)
    logger.info("Starting extraction...")
    url="https://echanges.dila.gouv.fr/OPENDATA/CASS/"

    timeout = timeout or 10

    for archive_name in scrap_archive_path(url, timeout):
        if not archive_name.startswith("CASS_"):
            logger.info(f"Skipping {path_archive}, it does not start with 'CASS_'.")
            continue

        logger.info(f"Downloading archive {archive_name}...")
        path_archive = urljoin(url, archive_name)
        resp = requests.get(path_archive, timeout = timeout)

        tar_io = io.BytesIO(resp.content) #Bytes suited for streaming

        logger.info(f"archive successfully downloaded, extracting XML file...")

        with tarfile.open(fileobj=tar_io, mode="r:gz") as archive:
            for xml_info in filter(lambda arc: arc.name.endswith(".xml"), archive):
                logger.info(f"XML file {xml_info.name} found, extracting file...")

                archive.extractall(xml_info.name) #Extract file and 

                logger.info(f"Extraction of {xml_info.name} is done.")

    logger.info("The extraction is finished.")

if __name__ == '__main__':
    main(None)