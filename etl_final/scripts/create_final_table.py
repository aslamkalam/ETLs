from config.database_config import get_engine

def create_final_partitioned_table():
    """Create the final partitioned table with a default partition."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS final_customer (
        customer_name VARCHAR(255) NOT NULL,
        customer_id VARCHAR(18) NOT NULL PRIMARY KEY,
        open_date DATE NOT NULL,
        last_consulted_date DATE,
        vaccination_id CHAR(5),
        dr_name VARCHAR(255),
        state CHAR(5),
        country CHAR(5) NOT NULL,
        dob DATE,
        is_active CHAR(1),
        age INT,
        days_since_last_consulted INT
    ) PARTITION BY LIST (country);
    """

    create_default_partition = """
    CREATE TABLE IF NOT EXISTS final_customer_default
    PARTITION OF final_customer
    DEFAULT;
    """

    with get_engine().connect() as conn:
        conn.execute(create_table_query)
        conn.execute(create_default_partition)
        print("Final partitioned table and default partition created.")
