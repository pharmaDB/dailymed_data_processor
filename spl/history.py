import concurrent.futures
import os

import requests

from utils.logging import getLogger

_logger = getLogger(__name__)


class SplHistoryResponse:
    """
    Used to retrieve and process a DailyMed setid's history JSON data.
    https://dailymed.nlm.nih.gov/dailymed/services/v2/spls/{set_id}/history
    """

    BASE_URL = "https://dailymed.nlm.nih.gov/dailymed/services/v2/spls"
    RESOURCE_PATH = "history"

    def __init__(self, set_id):
        if set_id is None:
            raise ValueError("Set ID is not defined")
        self.set_id = set_id
        # Attributes to store processed data
        self.data = {}
        # Process
        self.__fetch_and_process()

    def get_total_pages(self):
        """Returns the value of the total pages of set id history, from its
        metadata.

        Returns:
            int: max page number for the SPL history data
        """
        return int(self.data["metadata"]["total_pages"])

    def __fetch_and_process(self):
        """
        Fetches the spl history and processes it. The parsed data is stored in the
        spl attribute.
        """
        url = f"{SplHistoryResponse.BASE_URL}/{self.set_id}/{SplHistoryResponse.RESOURCE_PATH}"
        r = requests.get(url, allow_redirects=True)
        try:
            self.data = r.json()
        except Exception as e:
            _logger.error(
                f"Unable to parse JSON data from set ID {self.set_id}"
            )


def get_spl(set_id):
    spl_history = SplHistoryResponse(set_id=set_id)
    # TODO: Handle more than 1 page of set id history data.
    # For now, simply report it.
    total_pages = spl_history.get_total_pages()
    if total_pages > 1:
        _logger.error(
            f"Unhandled: there are {total_pages} pages of set_id history"
        )
    return spl_history.data


def process_spl_history(index_spls):
    """Fetches the history of the unique set_ids in the input data and returns
    the list.

    Args:
        index_spls (list[dict]): a list of spls from the SPL index. It is expected that
                                 minimally, each object is a dict with the key "setid".

    Raises:
        ValueError: When index_spls is not set or is not a list

    Returns:
        (list[dict]): The list of spls processed from the set_id's history
    """
    if index_spls is not None and not isinstance(index_spls, list):
        raise ValueError("SPL index data provided is incompatible.")

    # Get unique setids
    set_ids = set(
        filter(lambda x: bool(x), map(lambda x: x["setid"], index_spls))
    )

    # Fetch and process all set IDs in parallel
    _logger.info(f"Fetching and processing {len(set_ids)} unique set IDs")
    spls = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for set_id, spl in zip(
            set_ids,
            executor.map(get_spl, set_ids),
        ):
            _logger.info(f"Processed set ID {set_id}")
            spls.append(spl)

    # Return the history data of the spls
    return spls
