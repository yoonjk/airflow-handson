import os 

TOMTOM_API = os.getenv("TOMTOM_API", "https://api.midway.tomtom.com/ranking/liveHourly/IDN_jakarta")
CSV_FILE_DIR=os.getenv("CSV_FILE_DIR","/opt/airflow/data/datasets/tomtom" )

PSQL_DB=os.getenv("PSQL_DB", "airflw")
PSQL_USER = os.getenv("PSQL_USER", "airflow")
PSQL_PASSWORD = os.getenv("PSQL_PASSWORD", "airflow")
PSQL_PORT = os.getenv("PSQL_PORT", "30100")
PSQL_HOST = os.getenv("PSQL_HOST", "169.51.195.164")