import os
import pandas as pd
from sqlalchemy import create_engine
from config.db_config import get_db_config
from config.settings import DATA_DIR


def get_engine():
    """Create a SQLAlchemy engine for the PostgreSQL database."""
    db_config = get_db_config()
    connection_string = (
        f"postgresql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )
    return create_engine(connection_string)


def read_file(file_path):
    """Read a file into a Pandas DataFrame."""
    data = pd.read_csv(
        file_path,
        sep="|",
        skiprows=1,  # Skip the header row
        header=None
    )
    # Define column names based on the file structure
    data.columns = [
        "record_type",
        "customer_name",
        "customer_id",
        "open_date",
        "last_consulted_date",
        "vaccination_id",
        "dr_name",
        "state",
        "country",
        "dob",
        "is_active",
    ]
    return data


def process_data(data):
    """Clean and process the data."""
    # Filter only detail records (record_type = 'D')
    data = data[data["record_type"] == "D"].drop(columns=["record_type"])

    # Convert date columns to datetime format
    date_columns = ["open_date", "last_consulted_date", "dob"]
    for col in date_columns:
        data[col] = pd.to_datetime(data[col], format="%Y%m%d", errors="coerce")

    return data


def write_to_database(data):
    """Write a DataFrame to the staging table."""
    engine = get_engine()
    data.to_sql(
        "staging_customers",
        con=engine,
        if_exists="append",
        index=False,
        chunksize=1000  # Write in chunks for large datasets
    )


def load_file_to_staging(file_name):
    """Read, process, and write a file into the staging table."""
    file_path = os.path.join(DATA_DIR, file_name)
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    try:
        # Read the file into a DataFrame
        data = read_file(file_path)

        # Process and clean the data
        data = process_data(data)

        # Write the data to the database
        write_to_database(data)

        print(f"Data from {file_name} loaded successfully into staging table.")
    except Exception as e:
        print(f"Error processing file {file_name}: {e}")
