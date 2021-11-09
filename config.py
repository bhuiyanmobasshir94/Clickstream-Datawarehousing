import os
import ssl

from dotenv import load_dotenv

ssl._create_default_https_context = ssl._create_unverified_context
load_dotenv()

## Get the details from the .env file
USE_POSTGRESQL = os.getenv("USE_POSTGRESQL") == "True"
POSTGRESQL_HOST = os.getenv("POSTGRESQL_HOST")
POSTGRESQL_PORT = int(os.getenv("POSTGRESQL_PORT"))
POSTGRESQL_USER = os.getenv("POSTGRESQL_USER")
POSTGRESQL_PASSWORD = os.getenv("POSTGRESQL_PASSWORD")
POSTGRESQL_DB = os.getenv("POSTGRESQL_DB")
USE_DATALAKE = os.getenv("USE_DATALAKE") == "True"
DATALAKE_DIR = os.getenv("DATALAKE_DIR")
DATACATALOG_TABLE = os.getenv("DATACATALOG_TABLE")
DATAWAREHOUSE_TABLE = os.getenv("DATAWAREHOUSE_TABLE")
FOLDER_NAMES = (
    list(os.getenv("FOLDER_NAMES").split(","))
    if os.getenv("FOLDER_NAMES") is not None
    else []
)
FILE_NAMES = (
    list(os.getenv("FILE_NAMES").split(","))
    if os.getenv("FILE_NAMES") is not None
    else []
)
USE_DF_CHUNK = os.getenv("USE_DF_CHUNK") == "True"
USE_DF_CHUNK_SIZE = int(os.getenv("USE_DF_CHUNK_SIZE"))
