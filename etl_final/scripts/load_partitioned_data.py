from config.database_config import get_engine
from scripts.utils import create_partition_for_country, log_event
import pandas as pd
from sqlalchemy import text


def get_countries_from_staging():
    """Retrieve distinct countries from the staging table."""
    query = "SELECT DISTINCT country FROM staging_customers"
    with get_engine().connect() as conn:
        result = conn.execute(text(query))
        countries = [row["country"] for row in result]
    return countries


def fetch_data_for_country(country):
    """Fetch all records for a specific country from the staging table."""
    query = f"SELECT * FROM staging_customers WHERE country = :country"
    with get_engine().connect() as conn:
        data = pd.read_sql(text(query), conn, params={"country": country})
    return data


def validate_and_transform(data):
    """Validate and transform the data."""
    # Drop rows with missing mandatory fields
    data = data.dropna(subset=["customer_name", "customer_id", "open_date", "country"])

    # Convert date fields
    today = pd.Timestamp.now()
    data["dob"] = pd.to_datetime(data["dob"], errors="coerce")
    data["last_consulted_date"] = pd.to_datetime(data["last_consulted_date"], errors="coerce")
    data["open_date"] = pd.to_datetime(data["open_date"], errors="coerce")

    # Compute derived columns
    data["age"] = data["dob"].apply(lambda x: (today - x).days // 365 if pd.notnull(x) else None)
    data["days_since_last_consulted"] = data["last_consulted_date"].apply(
        lambda x: (today - x).days if pd.notnull(x) else None
    )

    # Remove invalid ages
    data = data[data["age"] >= 0]
    return data


def load_to_final_table():
    """Load data from staging to the partitioned final table."""
    engine = get_engine()
    countries = get_countries_from_staging()

    for country in countries:
        log_event(f"Processing data for country: {country}")

        # Create partition for the country if it doesn't exist
        create_partition_for_country(country)

        # Fetch data for the country
        country_data = fetch_data_for_country(country)

        # Validate and transform data
        validated_data = validate_and_transform(country_data)

        # Write to the respective partition
        validated_data.to_sql(
            "final_customer",
            con=engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000,
        )
        log_event(f"Data for country {country} loaded successfully.")
