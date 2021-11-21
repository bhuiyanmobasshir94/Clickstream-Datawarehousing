import os
from collections import defaultdict
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from bs4 import BeautifulSoup

from config import *
from extractor import extract
from utils import create_client_engine, validate_df

# from sqlalchemy import create_engine
# from iterator import copy_string_iterator


def create_staging_table():
    client, client_engine = create_client_engine()

    client.execute(
        """
        CREATE UNLOGGED TABLE IF NOT EXISTS staging_{DATAWAREHOUSE_TABLE} (
            referrer VARCHAR(500) NOT NULL,
            resource VARCHAR(500) NOT NULL,
            type VARCHAR(200) NOT NULL,
            number_of_occurrences BIGINT NOT NULL,
            date DATE NOT NULL,
            wiki VARCHAR(200) NOT NULL
        );
        """.format(
            DATAWAREHOUSE_TABLE=DATAWAREHOUSE_TABLE
        )
    )
    client.disconnect()
    logger.info("Staging table created.")


def create_tables():
    client, client_engine = create_client_engine()
    client.execute(
        """
        CREATE TABLE IF NOT EXISTS {DATACATALOG_TABLE} (
            folder_name VARCHAR(200) NOT NULL,
            file_name VARCHAR(200) NOT NULL,
            resource_url VARCHAR(200) NOT NULL,
            size_in_bytes BIGINT NOT NULL,
            data_inserted BOOLEAN NOT NULL,
            number_of_rows BIGINT NOT NULL,
            CONSTRAINT {DATACATALOG_TABLE}_pk PRIMARY KEY (folder_name, file_name)
        )
        """.format(
            DATACATALOG_TABLE=DATACATALOG_TABLE
        )
    )
    client.execute(
        """
        CREATE TABLE IF NOT EXISTS {DATAWAREHOUSE_TABLE} (
            referrer VARCHAR(500) NOT NULL,
            resource VARCHAR(500) NOT NULL,
            type VARCHAR(200) NOT NULL,
            number_of_occurrences BIGINT NOT NULL,
            date DATE NOT NULL,
            wiki VARCHAR(200) NOT NULL
        )
        """.format(
            DATAWAREHOUSE_TABLE=DATAWAREHOUSE_TABLE
        )
    )
    client.disconnect()
    logger.info("Tables created.")


def populate_datacatalog():
    client, client_engine = create_client_engine()

    datacatalog = pd.read_sql_table(DATACATALOG_TABLE, client_engine)
    datacatalog_number_of_rows = datacatalog.shape[0]

    if datacatalog_number_of_rows == 0:
        logger.info("Creating datacatalog.")
        html = requests.get(DATA_VENDOR_URL).text
        soup = BeautifulSoup(html, "lxml")
        parent_links = [
            a["href"]
            for a in soup.findAll("a")
            if a.has_attr("href")
            and not (a["href"].startswith("../") or a["href"].startswith("readme.html"))
        ]
        link_catalog = defaultdict(list)
        for parent_link in parent_links:
            folder_name = parent_link.split("/")[0]
            parent_link = DATA_VENDOR_URL + parent_link
            html = requests.get(parent_link).text
            soup = BeautifulSoup(html, "lxml")
            child_links = [
                parent_link + a["href"]
                for a in soup.findAll("a")
                if a.has_attr("href")
                and not (
                    a["href"].startswith("../") or a["href"].startswith("readme.html")
                )
            ]
            link_catalog[folder_name].extend(child_links)
        link_catalog = dict(link_catalog)

        folder_names = []
        file_names = []
        resource_urls = []
        for folder_name, urls in link_catalog.items():
            for url in urls:
                file_name = url.split("/")[-1]
                folder_names.append(folder_name)
                resource_urls.append(url)
                file_names.append(file_name)

        data_catalog = {
            "folder_name": folder_names,
            "file_name": file_names,
            "resource_url": resource_urls,
            "size_in_bytes": 0,
            "data_inserted": False,
            "number_of_rows": 0,
        }
        data_catalog = pd.DataFrame(
            data_catalog,
            columns=[
                "folder_name",
                "file_name",
                "resource_url",
                "size_in_bytes",
                "data_inserted",
                "number_of_rows",
            ],
        )
        try:
            data_catalog.to_sql(
                schema="public",
                name=DATACATALOG_TABLE,
                con=client_engine,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=10_000,
            )
            logger.info("Datacatalog created.")
        except Exception as e:
            logger.error(e)
    else:
        logger.info("Datacatalog already exists.")
    client.disconnect()


def create_datalake():
    client, client_engine = create_client_engine()
    folder_names, file_names = FOLDER_NAMES, FILE_NAMES

    datacatalog = pd.read_sql_table(DATACATALOG_TABLE, client_engine)
    datacatalog_number_of_rows = datacatalog.shape[0]

    if datacatalog_number_of_rows > 0:
        for index, row in datacatalog.iterrows():
            folder_name = row["folder_name"]
            file_name = row["file_name"]
            resource_url = row["resource_url"]
            size_in_bytes = row["size_in_bytes"]
            data_inserted = bool(row["data_inserted"])
            number_of_rows = row["number_of_rows"]

            if (
                folder_names
                and len(folder_names) > 0
                and not folder_name in folder_names
            ) or (file_names and len(file_names) > 0 and not file_name in file_names):
                continue

            folder_path = os.path.join(DATALAKE_DIR, folder_name)
            os.makedirs(folder_path, exist_ok=True)

            gz_file_path = os.path.join(DATALAKE_DIR, folder_name, file_name)
            file_path = gz_file_path.replace(".gz", "")

            if (
                os.path.exists(gz_file_path) and not os.path.getsize(gz_file_path) > 0
            ) or not os.path.exists(gz_file_path):
                try:
                    r = requests.get(resource_url, stream=True)
                    with open(gz_file_path, "wb") as f:
                        for chunk in r.raw.stream(1024, decode_content=False):
                            if chunk:
                                f.write(chunk)

                    with open(file_path, "wb") as f:
                        with requests.get(
                            resource_url, allow_redirects=True, stream=True
                        ) as resp:
                            for chunk in resp.iter_content(chunk_size=1024):
                                if chunk:
                                    f.write(chunk)

                    # with open(file_path, "wb") as f:
                    #     for chunk in r.iter_content(chunk_size=1024):
                    #         if chunk:
                    #             f.write(chunk)

                    logger.info(
                        "Data downloaded into datalake for {}.".format(file_name)
                    )
                except Exception as e:
                    logger.error(e)
            elif (
                os.path.exists(gz_file_path)
                and os.path.getsize(gz_file_path) > 0
                and (
                    not os.path.exists(file_path) or not os.path.getsize(file_path) > 0
                )
            ):
                try:
                    extract(folder_path, gz_file_path)
                    logger.info(
                        "Data extracted into datalake for {}.".format(file_name)
                    )
                except Exception as e:
                    logger.error(e)
            else:
                logger.info("Data already exists in datalake for {}.".format(file_name))

            size_in_bytes = os.path.getsize(file_path)
            client.execute(
                """
                UPDATE {DATACATALOG_TABLE}
                SET size_in_bytes = {size_in_bytes}
                WHERE folder_name = '{folder_name}'
                    AND resource_url = '{resource_url}'
                """.format(
                    DATACATALOG_TABLE=DATACATALOG_TABLE,
                    folder_name=folder_name,
                    size_in_bytes=size_in_bytes,
                    resource_url=resource_url,
                )
            )
    client.disconnect()


def extract_transform_load():
    client, client_engine = create_client_engine()
    folder_names, file_names = FOLDER_NAMES, FILE_NAMES

    datacatalog = pd.read_sql_table(DATACATALOG_TABLE, client_engine)
    datacatalog_number_of_rows = datacatalog.shape[0]

    columns = ["referrer", "resource", "type", "number_of_occurrences"]

    if datacatalog_number_of_rows > 0:
        for index, row in datacatalog.iterrows():
            folder_name = row["folder_name"]
            file_name = row["file_name"]
            resource_url = row["resource_url"]
            size_in_bytes = row["size_in_bytes"]
            data_inserted = bool(row["data_inserted"])
            number_of_rows = row["number_of_rows"]
            date = datetime.strptime(folder_name, "%Y-%m").date()
            wiki = str(file_name.split("-")[1])

            if (
                folder_names
                and len(folder_names) > 0
                and not folder_name in folder_names
            ) or (file_names and len(file_names) > 0 and not file_name in file_names):
                continue

            folder_path = os.path.join(DATALAKE_DIR, folder_name)
            gz_file_path = os.path.join(DATALAKE_DIR, folder_name, file_name)
            tsv_file_path = gz_file_path.replace(".gz", "")
            parquet_file_path = tsv_file_path.replace(".tsv", ".parquet")

            if not os.path.exists(parquet_file_path):
                try:
                    df = pd.read_csv(
                        tsv_file_path,
                        header=None,
                        sep="\t",
                        error_bad_lines=False,
                        warn_bad_lines=True,
                    )
                    df.columns = columns
                    df = validate_df(df)
                    df["date"] = date
                    df["wiki"] = wiki

                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, parquet_file_path, compression="BROTLI")
                    logger.info(
                        f"Created parquet file {os.path.basename(parquet_file_path)}"
                    )
                except Exception as e:
                    logger.error(
                        f"file_name: {os.path.basename(parquet_file_path)}, error: {e}"
                    )
            else:
                df = pd.read_parquet(parquet_file_path, engine="fastparquet")
                logger.info(f"Read parquet file {os.path.basename(parquet_file_path)}")

            if not data_inserted and number_of_rows == 0:
                try:
                    df.to_sql(
                        schema="public",
                        name=DATAWAREHOUSE_TABLE,
                        con=client_engine,
                        if_exists="append",
                        index=False,
                        method="multi",
                        chunksize=10_000,  # 10,000 rows per insert with 5 parameters for constraint 65535
                    )
                    logger.info(
                        f"Inserted data into {DATAWAREHOUSE_TABLE} for {os.path.basename(parquet_file_path)} with {df.shape[0]} rows."
                    )
                    client.execute(
                        f"UPDATE {DATACATALOG_TABLE} SET data_inserted = True, number_of_rows = {df.shape[0]} WHERE resource_url = '{resource_url}'"
                    )
                except Exception as e:
                    logger.error(e)
    client.disconnect()
