import concurrent.futures
import os
from pathlib import Path
import re
import shutil
import zipfile

from bs4 import BeautifulSoup as bs, Tag, NavigableString
import cleantext
import requests
import unicodedata

from db.mongo import connect_mongo, MongoClient
from utils.logging import getLogger

_logger = getLogger(__name__)

_mongo_client = MongoClient(connect_mongo())

MONGO_COLLECTION_NAME = "labels"


class SplHistoricalLabels:
    """
    Used to retrieve and process labels attached to every specified version of
    a set_id. The set_id and version data are passed in as a part of the input
    to the init function, which is the processed data from the
    SplHistoryResponse object.

    A URL of the following format downloads a zip file of the label for the
    set_id + version.
    https://dailymed.nlm.nih.gov/dailymed/getFile.cfm?type=zip&setid={set_id}&version={version}
    """

    BASE_URL = "https://dailymed.nlm.nih.gov/dailymed/getFile.cfm?type=zip"

    # LABEL_SECTIONS includes titles of interest and their variants
    # LABEL_SECTIONS does not include subtitles. Subtitles are identified by
    # __get_label_text(), since they differ from label to label.
    #
    # Example of title/subtitle variants include:
    # 1. https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=762b51be-1893-4cd1-9511-e645fc420d3a&version=2
    # 2. https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=762b51be-1893-4cd1-9511-e645fc420d3a&version=6
    # 3. https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=762b51be-1893-4cd1-9511-e645fc420d3a&version=13
    # 4. https://dailymed.nlm.nih.gov/dailymed/lookup.cfm?setid=1b5e2860-6855-4a65-8bbc-e064172a1adf&version=1
    # 5. https://dailymed.nlm.nih.gov/dailymed/getFile.cfm?type=zip&setid=762b51be-1893-4cd1-9511-e645fc420d3a&version=14
    #
    # As further note:
    # https://dailymed.nlm.nih.gov/dailymed/lookup.cfm is not entirely comprehensive

    LABEL_SECTIONS = [
        "INDICATIONS AND USAGE",
        "DOSAGE AND ADMINISTRATION",
        "DOSAGE FORMS AND STRENGTHS",
        "USE IN SPECIFIC POPULATIONS",
        "DESCRIPTION",
        "CLINICAL PHARMACOLOGY",
        "CLINICAL STUDIES",
        # the following are taken from example 1 above
        "CLINICAL TRIALS",
        "INDICATIONS",
        # the following are taken from example 4 above
        "ACTIVE INGREDIENT",
        "INACTIVE INGREDIENTS",
        "PURPOSE",
        "DIRECTIONS",
        "USE",
    ]

    def __init__(self, spl, download_path):
        if not isinstance(spl, dict):
            raise ValueError(
                "Expected spl data to a dict with the history data"
            )
        if download_path is None:
            raise ValueError("Download path is not defined")
        if not os.path.exists(download_path):
            os.mkdir(download_path)
        # Init attributes
        self.download_path = download_path
        try:
            self.set_id = spl["data"]["spl"]["setid"]
            self.spl_versions = list(
                map(lambda x: x["spl_version"], spl["data"]["history"])
            )
        except Exception as e:
            raise ValueError(f"Bad SPL data passed to SplLabelFile: {e}")
        # Attributes to store processed data
        self.nda_found = False
        self.spl_label_versions = []
        # Process
        self.__fetch_and_process()

    def __fetch_and_process(self):
        """
        Fetches the spl version label data and processes it. The parsed data is
        stored in the spl_label_versions attribute. If any version has an
        association with an NDA number, nda_found is set to True.
        """
        for version in self.spl_versions:
            url = f"{SplHistoricalLabels.BASE_URL}&setid={self.set_id}&version={version}"
            folder_path = os.path.join(
                self.download_path, f"{self.set_id}_{version}"
            )
            if not os.path.exists(folder_path):
                os.mkdir(folder_path)
            file_path = os.path.join(folder_path, "zipfile.zip")

            # Download and save label as zip file
            r = requests.get(url, allow_redirects=True)
            with open(file_path, "wb+") as f:
                f.write(r.content)

            # Extract all the contents of zip file in different directory
            try:
                with zipfile.ZipFile(file_path, "r") as zip_obj:
                    zip_obj.extractall(folder_path)
            except Exception as e:
                _logger.error(f"Unable to extract zip file {file_path}: {e}")
                return

            # Parse the XML file and delete the other files extracted
            for dir_name, _, files in os.walk(folder_path):
                for file_name in files:
                    if file_name.endswith(".xml"):
                        self.__process_label(os.path.join(dir_name, file_name))

            # Delete the folder (the original zip file and the extracted data)
            shutil.rmtree(folder_path)

    def __process_label(self, xml_file_name):
        """
        Processes the data in the given file name, extracting the required data
        and saving them to the spl_label_versions attribute. Also checks for
        the presence of an NDA association in the label data and sets the
        nda_found flag to True, if found.

        Args:
            file_name (str): the full name (inclusive of the absolute path) of
                             the label file name to process
        """
        if not os.path.exists(xml_file_name):
            raise ValueError(f"File not found at: {xml_file_name}")
        try:
            with open(xml_file_name) as f:
                # Init BeautifulSoup object with the contents
                bs_content = bs(f.read(), "lxml")
                # Get Set ID
                set_id = bs_content.document.setid["root"]
                # Get Application Number
                application_numbers = self.__get_application_numbers(
                    set_id, bs_content
                )
                if application_numbers:
                    self.nda_found = True
                # Get other required properties and make label version data
                self.spl_label_versions.append(
                    {
                        "application_numbers": application_numbers,
                        "set_id": set_id,
                        "spl_id": Path(xml_file_name).name[:-4],
                        "spl_version": self.__get_spl_version(bs_content),
                        "published_date": self.__get_published_date(bs_content),
                        "sections": self.__get_label_text(set_id, bs_content),
                    }
                )
        except Exception as e:
            _logger.error(f"Unable to parse XML data from file: {e}")

    def __get_spl_version(self, label_data):
        return label_data.document.versionnumber["value"]

    def __get_published_date(self, label_data):
        date = label_data.document.effectivetime["value"]
        return f"{date[:4]}-{date[4:6]}-{date[-2:]}"

    def __get_application_numbers(self, set_id, bs_content):
        application_numbers = set()
        try:
            components = bs_content.document.component.structuredbody.component
            for item in components.find_all("approval"):
                if hasattr(item, "code") and item.code["displayname"] == "NDA":
                    # Capture the corresponding NDA number
                    appln_num = (
                        item.id["extension"][3:]
                        if item.id["extension"].startswith("NDA")
                        else item.id["extension"]
                    )
                    application_numbers.add("NDA" + appln_num.zfill(6))
        except Exception as e:
            _logger.error(
                f"Error in __get_application_numbers for set ID {set_id}: {e}"
            )
        return list(application_numbers)

    def __get_label_text(self, set_id, bs_content):
        def get_xml_text(text):
            # Process the label text; normalize remove nbsp
            text = unicodedata.normalize("NFKC", text.lstrip().rstrip())
            text = re.sub(r"\n\s*\n", "\n", text)
            return text

        def get_xml_title(text):
            # strip extra \n, \t, and spaces in titles, but keep number
            text = get_xml_text(text)
            text = re.sub(r"\.\s+", ".", text)
            text = re.sub(r"\s+", " ", text)
            return text

        # Get and process all "title" tags
        titles = bs_content.find_all("title")
        labels = []

        for title in titles:
            # For matching:
            # * Convert title text to upper case
            # * Replace & with AND, tab with space
            # * Clean up Unicode characters, digits, punctuation
            title_match_text = cleantext.clean(
                title.text.upper().replace("&", "AND").replace("\t", " "),
                lower=False,
                no_line_breaks=True,
                no_punct=True,
                no_numbers=True,
                no_digits=True,
                replace_with_number="",
                replace_with_digit="",
            )

            # Exact match against label section titles
            if title_match_text in SplHistoricalLabels.LABEL_SECTIONS:
                title_name, title_text = get_xml_title(
                    title.text
                ), get_xml_text(
                    title.parent.find("text").text,
                )
                # Get sub-sections of the label
                subtitles = title.parent.find_all("title")
                sub_section_labels = []

                # The first match is the title itself, so check len > 1
                if len(subtitles) > 1:
                    # Make sub-section labels list
                    sub_section_labels = list(
                        map(
                            lambda x: {
                                "name": get_xml_title(x.text),
                                "text": get_xml_text(
                                    x.parent.find("text").text,
                                ),
                                "parent": title_name,
                            },
                            subtitles[1:],
                        )
                    )
                    # Replace title text with sibling text if there are sub-sections
                    title_text = (
                        title.find_next_sibling("text").text
                        if title.find_next_sibling("text")
                        else ""
                    )

                # Add the section info and sub-sections to the labels list
                labels.extend(
                    [
                        {
                            "name": title_name,
                            "text": title_text,
                            "parent": None,
                        }
                    ]
                    + sub_section_labels
                )
        return labels


def process_labels_for_set_id(set_id_history):
    labels = SplHistoricalLabels(
        spl=set_id_history, download_path=set_id_history["download_path"]
    )
    if labels.nda_found:
        # Upsert to MongoDB
        for label in labels.spl_label_versions:
            _mongo_client.upsert(
                MONGO_COLLECTION_NAME,
                {"spl_id": label["spl_id"], "set_id": label["set_id"]},
                label,
            )
        return True
    return False


def process_historical_labels(all_setid_history, download_path):
    """
    Fetches the detailed label text for all spl versions of the set_id.
    If any version of a given set_id has an association with one or more
    NDA numbers, the data will be processed further and saved to MongoDB.

    Args:
        all_setid_history (list[dict]): A list of history records for a set_id
                                        as created by the SplHistoryResponse
                                        object, after processing.
        download_path (str): Temporary folder to store the label data
    """
    # If the download_path does not exist yet, create it.
    if not os.path.exists(download_path):
        os.mkdir(download_path)

    # Associate download_path with the set_id_history before the parallel
    # processing, as it cannot be passed as a second argument easily.
    for obj in all_setid_history:
        obj["download_path"] = download_path

    # Process each set_id's historical label data in parallel
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for set_id_history, _ in zip(
            all_setid_history,
            executor.map(
                process_labels_for_set_id,
                all_setid_history,
            ),
        ):
            set_id = set_id_history["data"]["spl"]["setid"]
            _logger.info(f"Processed labels for set ID {set_id}")
