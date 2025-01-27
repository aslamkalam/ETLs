from scripts.create_final_table import create_final_partitioned_table
from scripts.load_partitioned_data import load_to_final_table
from scripts.utils import log_event


def main():
    """Main function to orchestrate the ETL process."""
    log_event("ETL process started.")

    try:
        # Step 1: Create Final Partitioned Table
        log_event("Creating final partitioned table.")
        create_final_partitioned_table()

        # Step 2: Load Data from Staging to Partitions
        log_event("Loading data from staging to final table with partitions.")
        load_to_final_table()

        log_event("ETL process completed successfully.")
    except Exception as e:
        log_event(f"ETL process failed: {e}")
        raise


if __name__ == "__main__":
    main()
