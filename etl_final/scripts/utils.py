from config.database_config import get_engine

def create_partition_for_country(country):
    """Create a partition for a specific country."""
    partition_query = f"""
    CREATE TABLE IF NOT EXISTS final_customer_{country.lower()}
    PARTITION OF final_customer
    FOR VALUES IN ('{country}');
    """
    with get_engine().connect() as conn:
        conn.execute(partition_query)
        print(f"Partition for country {country} created.")

def log_event(message):
    """Log events to the ETL log file."""
    with open("logs/etl.log", "a") as log_file:
        log_file.write(f"{message}\n")
