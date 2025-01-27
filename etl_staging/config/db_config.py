from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def get_db_config():
    """Fetch database configuration from environment variables."""
    return {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": os.getenv("DB_PORT"),
    }
