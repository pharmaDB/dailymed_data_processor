# dailymed_data_processor
Scripts to download in bulk and process the drug labels data from DailyMed. For a given SPL index start page and number of pages of the index to process, the scripts parse the index data and obtain the historical set id data.

## TBD
* Obtain label data for every SPL version in the historical set id data
* Save to MongoDB the ones that are related to the OrangeBook (restrict by input FDA application numbers)

## Running the Code
Requires a minimum python version of `3.6` to run.
1. `pip3 install -r requirements.txt`
2. For usage and args, run `python3 main.py -h`

## Code Formatting
It is recommended to use the [Black Code Formatter](https://github.com/psf/black) which can be installed as a plugin for most IDEs. `pyproject.toml` holds the formatter settings.
