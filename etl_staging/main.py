import os
import logging
from scripts.create_tables import create_staging_table
from scripts.loading_to_staging import load_file_to_staging
from config.settings import DATA_DIR, LOG_FILE

# Configure logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    logging.info("ETL Process started.")

    # Step 1: Create staging table
    logging.info("Creating staging table.")
    create_staging_table()

    # Step 2: Load data into staging table
    for file in os.listdir(DATA_DIR):
        if file.endswith(".csv"):
            logging.info(f"Loading file: {file}")
            load_file_to_staging(file)

    logging.info("ETL Process completed.")


if __name__ == "__main__":
    main()
