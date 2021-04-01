import argparse
import json
import os

from spl.index import process_paginated_index
from spl.history import process_spl_history
from utils.logging import getLogger

_logger = getLogger("main")

TEMP_DATA_FOLDER = "tempdata"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Process the arguments to the application."
    )
    parser.add_argument(
        "--start_page",
        type=int,
        help="The page number at which to start the SPL index data processing",
    )
    parser.add_argument(
        "--num_pages",
        type=int,
        help="The number of pages of SPL index data to process",
    )
    parser.add_argument(
        "--write_index_data",
        action=argparse.BooleanOptionalAction,
        help="Whether to save the downloaded index data as a json file",
    )
    parser.add_argument(
        "--write_history_data",
        action=argparse.BooleanOptionalAction,
        help="Whether to save the downloaded SPL history data as a json file",
    )
    return parser.parse_args()


if __name__ == "__main__":
    # Note: For ongoing pipeline, the start_page value can be set based on the
    # page number that was processed in the last run.
    args = parse_args()
    _logger.info(f"Running with args: {args}")

    # Create temp data folder if not exists
    if not os.path.exists(TEMP_DATA_FOLDER):
        os.mkdir(TEMP_DATA_FOLDER)

    # Get SPL index data
    all_spls, end_page = process_paginated_index(
        start_page=args.start_page, num_pages=args.num_pages
    )

    # Write data obtained into a json file
    if args.write_index_data:
        with open(
            os.path.join(
                TEMP_DATA_FOLDER,
                f"spl_index_pages_{args.start_page}_to_{end_page}.json",
            ),
            "w+",
        ) as f:
            f.write(json.dumps(all_spls))

    # Get SetID history, for all unique setids retrieved
    all_setid_history = process_spl_history(all_spls)

    # Write data obtained into a json file
    if args.write_history_data:
        with open(
            os.path.join(
                TEMP_DATA_FOLDER,
                f"spl_history_pages_{args.start_page}_to_{end_page}.json",
            ),
            "w+",
        ) as f:
            f.write(json.dumps(all_setid_history))
