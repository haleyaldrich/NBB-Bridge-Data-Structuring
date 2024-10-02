from datetime import datetime
from dotenv import load_dotenv
import json
import logging
import os
import sys

load_dotenv(override=True)

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src import utils, openground

# Set up logging to a file with the current timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", f"{timestamp}.log")
)
logging.basicConfig(filename=log_filename, level=logging.INFO)

PROJECT_CLOUD_ID = os.getenv("PROJECT_CLOUD_ID")

if __name__ == "__main__":

    # Load the data from the input file
    input_path = os.path.join(os.path.dirname(__file__), "data", "cpt.json")

    with open(input_path, "r") as f:
        data = json.load(f)


    # Process each CPT
    for cpt in data:

        filepath = f"\\{cpt['source_file']}"
        name = cpt["id"]
        location_type = cpt["location_type"]
        logging.info(f"Parsing CPT {name} from {filepath}")
        cpt, cpt_data = utils.parse_conetec(filepath, name)

        existing_locations = openground.get_project_locations(PROJECT_CLOUD_ID)
        existing_cpts = openground.get_static_cone_general_records(PROJECT_CLOUD_ID)

        # Check if CPT already exists and correctly loaded
        if name in existing_cpts:
            loaded_records = utils.get_number_cpt_records(PROJECT_CLOUD_ID, name)
            if loaded_records == len(cpt_data.data):
                logging.info(f"CPT {name} already exists with {loaded_records} records, skipping")
                continue
            else:
                logging.info(f'Start loading for CPT {name}')

        # Insert Location if it does not exist
        if name not in existing_locations:
            logging.info(f"Location {name} does not exist, creating")
            utils.insert_location_from_cpt_test(cpt, PROJECT_CLOUD_ID, location_type)

        try:

            if name in existing_cpts:
                logging.info(f"CPT {name} already exists, deleting")
                openground.delete_cpt_by_id(PROJECT_CLOUD_ID, existing_cpts[name])

            # Insert CPT test
            logging.info(f"Inserting CPT {name}")
            utils.insert_cpt_test(cpt, PROJECT_CLOUD_ID)

            # Insert CPT data
            logging.info(f"Inserting CPT data for {name}")
            utils.insert_cpt_data(cpt_data, PROJECT_CLOUD_ID)

        except Exception as e:
            logging.error(f"Error inserting CPT {name}: {e}")

            # Delete location and by extension the CPT test
            logging.info(f"Deleting location {name}")
            location = openground.get_project_locations(PROJECT_CLOUD_ID)[name]
            openground.delete_location_by_id(PROJECT_CLOUD_ID, location)