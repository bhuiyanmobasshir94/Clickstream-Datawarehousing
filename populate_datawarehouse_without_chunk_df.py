import os
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from database_clients import PostgreSQLClient, SQLiteClient

populate_start = datetime.now()
print(f"Started populating datawarehouse at {populate_start}")

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
                    # low_memory=False,
                    error_bad_lines=False,
                    warn_bad_lines=True,
                    # chunksize=1000_000,
                    dtype={"0": str, "1": str, "2": str, "3": int},
                )
                df_shape = df.shape[0]
                chunk_start = datetime.now()
                df.columns = columns
                df["date"] = datetime.strptime(folder_name, "%Y-%m").date()
                df["wiki"] = str(file_name.split("-")[1])
                df.to_sql(
                    name="clickstream",
                    con=client_engine,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=10_000,  # 10,000 rows per insert with 5 parameters for constraint 65535
                )
                chunk_end = datetime.now() - chunk_start
                # data_catalog.loc[index, "data_inserted"] = True
                client.execute(
                    f"UPDATE datacatalog SET data_inserted = True WHERE file_name = '{file_name}'"
                )
                # data_catalog.loc[index, "number_of_rows"] = df.shape[0]
                client.execute(
                    f"UPDATE datacatalog SET number_of_rows = {df_shape} WHERE file_name = '{file_name}'"
                )
                print(f"{df_shape} rows ingested in {chunk_end} from {file_name}.")

                if index % 10 == 0:
                    print(f"At index {index}.")
else:
    import create_datalake

client.disconnect()

populate_end = datetime.now() - populate_start
print(f"Finished in {populate_end}.")
