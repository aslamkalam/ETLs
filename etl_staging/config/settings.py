import os

# Path to input data
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data", "input")

# Path to log file
LOG_FILE = os.path.join(BASE_DIR, "logs", "etl.log")
