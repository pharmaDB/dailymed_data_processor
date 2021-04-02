import argparse
import json
import os

from spl.index import process_paginated_index
from spl.history import process_spl_history
from spl.labels import process_historical_labels
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
        nargs="?",
        help=(
            "The page number at which to start the SPL index data processing. "
            "Use this together with --num_pages. "
            "Alternatively, supply --set_ids_from_file."
        ),
    )
    parser.add_argument(
        "--num_pages",
        type=int,
        nargs="?",
        help=(
            "The number of pages of SPL index data to process. "
            "Use this together with --start_page. "
            "Alternatively, supply --set_ids_from_file."
        ),
    )
    parser.add_argument(
        "--set_ids_from_file",
        type=str,
        nargs="?",
        help=(
            "The path to the JSON file containing the set ids to process, "
            "instead of all set ids in the index. "
            "If this is set, it takes priority over --start_page and --num_pages."
        ),
    )
    parser.add_argument(
        "--write_index_data",
        action=argparse.BooleanOptionalAction,
        help="Whether to save the downloaded index data as a json file",
    )
    parser.add_argument(
        "--write_history_data",
        action=argparse.BooleanOptionalAction,
        help=(
            "Whether to save the downloaded SPL history data as a json file. "
            "Not applicable when set_ids_from_file is used."
        ),
    )
    return parser.parse_args()


def get_set_ids_from_file(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)
    with open(file_path) as f:
        set_id_list = json.loads(f.read())
        return list(set(set_id_list))


if __name__ == "__main__":
    # Note: For ongoing pipeline, the start_page value can be set based on the
    # page number that was processed in the last run.
    args = parse_args()
    _logger.info(f"Running with args: {args}")

    # Create temp data folder if not exists
    if not os.path.exists(TEMP_DATA_FOLDER):
        os.mkdir(TEMP_DATA_FOLDER)

    # Fetch set_ids
    all_set_ids = []
    if args.set_ids_from_file:
        # Read set ids from the resource file
        all_set_ids = get_set_ids_from_file(args.set_ids_from_file)
    elif args.start_page and args.num_pages:
        # Get SPL index data
        all_spls, end_page = process_paginated_index(
            start_page=args.start_page, num_pages=args.num_pages
        )
        # Get unique setids, for the subsequent steps
        all_set_ids = list(set(map(lambda x: x["setid"], all_spls)))
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
    all_setid_history = process_spl_history(all_set_ids)

    # Write data obtained into a json file
    if args.write_history_data:
        with open(
            os.path.join(
                TEMP_DATA_FOLDER,
                f"spl_history_pages.json",
            ),
            "w+",
        ) as f:
            f.write(json.dumps(all_setid_history))

    # Get label text for each SPL version and write to MongoDB if any
    # version contains an association with and NDA number.
    process_historical_labels(
        all_setid_history, os.path.join(TEMP_DATA_FOLDER, "label_data")
    )
