import concurrent.futures
import functools
import os

import requests
import xmltodict

from utils.logging import getLogger

_logger = getLogger(__name__)


class SplFile:
    """
    Used to an SPL index file by its page number at
    https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.xml?page={page_num}
    """

    BASE_URL = "https://dailymed.nlm.nih.gov/dailymed/services/v2/spls.xml"

    def __init__(self, page_number):
        if page_number is None:
            raise ValueError("Page number is not defined")
        self.page_number = page_number
        # Attributes to store processed data
        self.metadata = {}
        self.spls = []
        # Process
        self.__fetch_and_process()

    def get_max_page_number(self):
        """Returns the max page number for the SPL index data from the metadata of the
        processed page.

        Returns:
            int: max page number for the SPL index data
        """
        return int(self.metadata["total_pages"])

    def __fetch_and_process(self):
        """
        Fetches the spl file and processes it. The parsed data is stored in the
        spls attribute.
        """
        url = f"{SplFile.BASE_URL}?page={self.page_number}"
        r = requests.get(url, allow_redirects=True)
        try:
            dict_data = xmltodict.parse(r.content)
            self.metadata = dict_data["spls"]["metadata"]
            self.spls = dict_data["spls"]["spl"]
        except Exception as e:
            _logger.error(
                f"Unable to parse XML data from file {self.filepath},"
                f"chunk {self.chunk_num}: {e}"
            )


def get_spls(page_num):
    return SplFile(page_number=page_num).spls


def process_paginated_spls(start_page, num_pages=None):
    """Fetches index pages in the applicable range, from start_page.

    Args:
        start_page (int): the page number from which to start downloading the SPL index
        num_pages (int, optional): the number of pages of the index data to download. If left unset, it will
                                   download all pages available from the starting page number. Defaults to None.

    Raises:
        ValueError: When start_date is not set
        ValueError: When start_date is not int or less than 1
        ValueError: When num_pages is set but is not a positive integer

    Returns:
        (list[dict], int): The list of spls processed and the last spl index page number processed
    """
    if not start_page:
        raise ValueError("SPL index start page is not set")
    if (not isinstance(start_page, int)) or start_page < 1:
        raise ValueError("SPL index start page must be a positive integer")
    if num_pages is not None:
        if num_pages is not None and (
            not isinstance(num_pages, int) or num_pages < 1
        ):
            raise ValueError("SPL index start page must be a positive integer")

    # Get max page number available
    first_spl_file = SplFile(page_number=1)
    max_page_number = first_spl_file.get_max_page_number()

    if start_page > max_page_number:
        # Nothing to process
        return [], None

    # Set end_page according to max_page_number available and num_pages to download
    end_page = (
        max_page_number
        if num_pages is None
        else min(start_page + num_pages - 1, max_page_number)
    )

    # Fetch and process all pages in parallel
    all_spls = []
    page_nums = range(start_page, end_page + 1)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for page_num, spls in zip(
            page_nums,
            executor.map(get_spls, page_nums),
        ):
            _logger.info(f"Processed page {page_num}")
            all_spls.extend(spls)

    # Return all the spls from the index and the last page number downloaded
    return all_spls, end_page
