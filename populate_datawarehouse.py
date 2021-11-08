import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from database_clients import PostgreSQLClient, SQLiteClient

load_dotenv()

USE_POSTGRESQL = os.getenv("USE_POSTGRESQL") == "True"
POSTGRESQL_HOST = os.getenv("POSTGRESQL_HOST")
POSTGRESQL_PORT = int(os.getenv("POSTGRESQL_PORT"))
POSTGRESQL_USER = os.getenv("POSTGRESQL_USER")
POSTGRESQL_PASSWORD = os.getenv("POSTGRESQL_PASSWORD")
POSTGRESQL_DB = os.getenv("POSTGRESQL_DB")

if USE_POSTGRESQL:
    print("Using PostgreSQL")
    client = PostgreSQLClient(
        dbname=POSTGRESQL_DB,
        user=POSTGRESQL_USER,
        password=POSTGRESQL_PASSWORD,
        host=POSTGRESQL_HOST,
        port=POSTGRESQL_PORT,
    )
    client.connect()
    client_engine = create_engine(
        f"postgresql://{POSTGRESQL_USER}:{POSTGRESQL_PASSWORD}@{POSTGRESQL_HOST}:{POSTGRESQL_PORT}/{POSTGRESQL_DB}"
    )
else:
    print("Using SQLite")
    client = SQLiteClient("datawarehouse.sqlite")
    client_engine = client.connect()

condition = ("2017", "2021")  # year range

datacatalog_number_of_rows = pd.read_sql(
    "SELECT COUNT(*) FROM datacatalog", client_engine
).iloc[0, 0]

if datacatalog_number_of_rows > 0:
    print("Datacatalog already populated.")
    datacatalog = pd.read_sql("SELECT * FROM datacatalog", client_engine)
    columns = ["referrer", "resource", "type", "number_of_occurrences"]

    for index, row in datacatalog.iterrows():
        folder_name = row["folder_name"]
        file_name = row["file_name"]
        resource_url = row["resource_url"]
        size_in_bytes = row["size_in_bytes"]
        data_inserted = bool(row["data_inserted"])

        if condition and not folder_name.startswith(condition):
            # print(f"Skipping {folder_name}")
            continue

        if not data_inserted and size_in_bytes > 0:
            file_path = os.path.join("data", folder_name, file_name)
            if os.path.isfile(file_path):
                print(f"Processing {file_name} to ingest.")
                # df = pd.read_table(file_path, compression="gzip")
                df = pd.read_table(
                    file_path,
                    compression="gzip",
                    usecols=range(0, 4),
                    header=None,
                    low_memory=False,
                )
                df.columns = columns
                df["Date"] = str(datetime.strptime(folder_name, "%Y-%m").date())
                df.to_sql(
                    name="clickstream",
                    con=client_engine,
                    if_exists="append",
                    index=False,
                )
                # data_catalog.loc[index, "data_inserted"] = True
                client.execute(
                    f"UPDATE datacatalog SET data_inserted = True WHERE file_name = '{file_name}'"
                )
                # data_catalog.loc[index, "number_of_rows"] = df.shape[0]
                client.execute(
                    f"UPDATE datacatalog SET number_of_rows = {df.shape[0]} WHERE file_name = '{file_name}'"
                )
                print(f"{df.shape[0]} rows ingested from {file_name}.")

                if index % 10 == 0:
                    print(f"At index {index}.")
else:
    import create_datalake

client.disconnect()
