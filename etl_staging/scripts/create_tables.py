from sqlalchemy import create_engine
from config.db_config import get_db_config


def create_staging_table():
    """Create the staging_customers table in PostgreSQL with partitioning and indexing."""
    db_config = get_db_config()
    engine = create_engine(
        f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
    )

    # Main table creation query with partitioning
    create_main_table_query = """
    CREATE TABLE IF NOT EXISTS staging_customers (
        customer_name VARCHAR(255),
        customer_id VARCHAR(18),
        open_date DATE,
        last_consulted_date DATE,
        vaccination_id CHAR(5),
        dr_name VARCHAR(255),
        state CHAR(5),
        country CHAR(5),
        dob DATE,
        is_active CHAR(1)
    );
    """


    # Index creation queries
    create_index_queries = [
        "CREATE INDEX IF NOT EXISTS idx_customer_id ON staging_customers (customer_id);",
        "CREATE INDEX IF NOT EXISTS idx_last_consulted_date ON staging_customers (last_consulted_date);",
    ]

    # Execute queries
    with engine.connect() as connection:
        connection.execute(create_main_table_query)
        for index_query in create_index_queries:
            connection.execute(index_query)

    print("Staging table with indexes created successfully.")
