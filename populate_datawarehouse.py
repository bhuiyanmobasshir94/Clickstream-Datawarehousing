import os
from datetime import date, datetime

import pandas as pd
from sqlalchemy import create_engine

from config import *
from database_clients import PostgreSQLClient, SQLiteClient


def create_client_engine():
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
        client = SQLiteClient(os.path.join(DATALAKE_DIR, "db", "datawarehouse.sqlite"))
        client_engine = client.connect()

    return client, client_engine


def populate_datawarehouse():
    folder_names, file_names = FOLDER_NAMES, FILE_NAMES
    client, client_engine = create_client_engine()

    datacatalog = pd.read_sql(f"SELECT * FROM {DATACATALOG_TABLE}", client_engine)
    datacatalog_number_of_rows = datacatalog.shape[0]

    if datacatalog_number_of_rows > 0:
        populate_start = datetime.now()
        print(f"Started populating datawarehouse at {populate_start}")
        columns = ["referrer", "resource", "type", "number_of_occurrences"]

        for index, row in datacatalog.iterrows():
            folder_name = row["folder_name"]
            file_name = row["file_name"]
            resource_url = row["resource_url"]
            size_in_bytes = row["size_in_bytes"]
            number_of_rows = row["number_of_rows"]
            data_inserted = bool(row["data_inserted"])
            date = datetime.strptime(folder_name, "%Y-%m").date()
            wiki = str(file_name.split("-")[1])

            if (
                folder_names
                and len(folder_names) > 0
                and not folder_name in folder_names
            ) or (file_names and len(file_names) > 0 and not file_name in file_names):
                continue

            if not data_inserted and number_of_rows == 0:
                ## TODO: Check if database has any rows, then delete thos first
                if (
                    pd.read_sql(
                        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{DATAWAREHOUSE_TABLE}' AND table_schema = 'public'",
                        client_engine,
                    ).iloc[0, 0]
                    == 1
                ):
                    print(f"Deleting if any rows mistakenly ingested")
                    client.execute(
                        f"DELETE FROM {DATAWAREHOUSE_TABLE} WHERE date = '{date}' AND wiki = '{wiki}'"
                    )
                if USE_DATALAKE:
                    print(f"Reading {folder_name}/{file_name} from datalake")
                    file_path = (
                        os.path.join(DATALAKE_DIR, folder_name, file_name)
                        if os.path.isfile(
                            os.path.join(DATALAKE_DIR, folder_name, file_name)
                        )
                        else resource_url
                    )
                else:
                    print(f"Reading {folder_name}/{file_name} from resource url")
                    file_path = resource_url

                print(f"Processing {file_name} to ingest.")
                df_shape = 0
                df_bytes = 0
                if USE_DF_CHUNK:
                    df = pd.read_table(
                        file_path,
                        compression="gzip",
                        usecols=range(0, 4),
                        header=None,
                        # low_memory=False,
                        error_bad_lines=False,
                        warn_bad_lines=True,
                        chunksize=USE_DF_CHUNK_SIZE,
                        dtype={"0": str, "1": str, "2": str, "3": int},
                    )
                    chunk_start = datetime.now()
                    for chunk in df:
                        df_shape += chunk.shape[0]
                        df_bytes += chunk.memory_usage(deep=True).sum()
                        chunk.columns = columns
                        chunk["date"] = date
                        chunk["wiki"] = wiki
                        print(f"Inserting chunk of {chunk.shape[0]} rows")
                        chunk.to_sql(
                            name=DATAWAREHOUSE_TABLE,
                            con=client_engine,
                            if_exists="append",
                            index=False,
                            method="multi",
                            chunksize=10_000,  # 10,000 rows per insert with 5 parameters for constraint 65535
                        )
                        chunk_end = datetime.now() - chunk_start
                        print(
                            f"{df_shape} rows and {df_bytes} bytes processed in {chunk_end}"
                        )
                else:
                    df = pd.read_table(
                        file_path,
                        compression="gzip",
                        usecols=range(0, 4),
                        header=None,
                        # low_memory=False,
                        error_bad_lines=False,
                        warn_bad_lines=True,
                        dtype={"0": str, "1": str, "2": str, "3": int},
                    )
                    chunk_start = datetime.now()
                    df_shape = df.shape[0]
                    df_bytes = df.memory_usage(deep=True).sum()
                    df.columns = columns
                    df["date"] = date
                    df["wiki"] = wiki
                    print(f"Inserting {df_shape} rows")
                    df.to_sql(
                        name=DATAWAREHOUSE_TABLE,
                        con=client_engine,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=10_000,  # 10,000 rows per insert with 5 parameters for constraint 65535
                    )
                    chunk_end = datetime.now() - chunk_start
                    print(
                        f"{df_shape} rows and {df_bytes} bytes processed in {chunk_end}"
                    )
                client.execute(
                    f"UPDATE {DATACATALOG_TABLE} SET data_inserted = True, number_of_rows = {df_shape}, size_in_bytes = {df_bytes} WHERE file_name = '{file_name}'"
                )
                print(f"{df_shape} rows ingested from {file_name}.")

        populate_end = datetime.now() - populate_start
        print(f"Finished in {populate_end}.")

    else:
        """
        TODO: Add a check to see if the datacatalog table is empty.
        """
        pass

    client.disconnect()


if __name__ == "__main__":
    populate_datawarehouse()
