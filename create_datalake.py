import os
from collections import defaultdict
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from database_clients import SQLiteClient

load_dotenv()

DATA_VENDOR_URL = os.getenv("DATA_VENDOR_URL")

sqlite = SQLiteClient("datawarehouse.sqlite")
sqlite_engine = sqlite.connect()
sqlite.execute(
    """
    CREATE TABLE IF NOT EXISTS `datacatalog` (
        `folder_name` VARCHAR(200) NOT NULL,
        `file_name` VARCHAR(200) NOT NULL,
        `resource_url` VARCHAR(200) NOT NULL,
        `size_in_bytes` INTEGER NOT NULL,
        `data_inserted` BOOLEAN NOT NULL,
        `number_of_rows` INTEGER NOT NULL,
        CONSTRAINT `datacatalog_pk` PRIMARY KEY (`file_name`)
    )
    """
)

# print(sqlite.execute("SELECT COUNT(*) FROM datacatalog"))
# print(sqlite.fetch_all())  # [(0,)]
# print(sqlite.fetch_one())  # (0,)

datacatalog_number_of_rows = pd.read_sql(
    "SELECT COUNT(*) FROM datacatalog", sqlite_engine
).iloc[0, 0]

if datacatalog_number_of_rows == 0:
    print("Creating datacatalog...")
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
            and not (a["href"].startswith("../") or a["href"].startswith("readme.html"))
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
    data_catalog.to_sql(
        name="datacatalog", con=sqlite_engine, if_exists="append", index=False
    )
    print("Datacatalog created.")

data_catalog = pd.read_sql("SELECT * FROM datacatalog", sqlite_engine)
print(f"{data_catalog.shape[0]} files to process.")

condition = ("2017", "2021")

for index, row in data_catalog.iterrows():
    folder_name = row["folder_name"]
    file_name = row["file_name"]
    resource_url = row["resource_url"]

    if condition and not folder_name.startswith(condition):
        # print(f"Skipping {folder_name}")
        continue

    if not os.path.exists(f"data/{folder_name}"):
        os.mkdir(f"data/{folder_name}")
    file_path = os.path.join("data", folder_name, file_name)
    if not os.path.exists(file_path):
        with open(file_path, "wb") as f:
            response = requests.get(resource_url)
            f.write(response.content)
        # data_catalog.loc[index, "size_in_bytes"] = os.stat(file_path).st_size
        sqlite.execute(
            f"UPDATE datacatalog SET size_in_bytes = {os.stat(file_path).st_size} WHERE file_name = '{file_name}'"
        )
        print(f"The file {file_name} now contains {os.stat(file_path).st_size} bytes")
    else:
        if row["size_in_bytes"] == 0:
            if os.stat(file_path).st_size > 0:
                # data_catalog.loc[index, "size_in_bytes"] = os.stat(file_path).st_size
                sqlite.execute(
                    f"UPDATE datacatalog SET size_in_bytes = {os.stat(file_path).st_size} WHERE file_name = '{file_name}'"
                )
                print(
                    f"The file {file_name} now contains {os.stat(file_path).st_size} bytes"
                )
            else:
                with open(file_path, "wb") as f:
                    response = requests.get(resource_url)
                    f.write(response.content)
                # data_catalog.loc[index, "size_in_bytes"] = os.stat(file_path).st_size
                sqlite.execute(
                    f"UPDATE datacatalog SET size_in_bytes = {os.stat(file_path).st_size} WHERE file_name = '{file_name}'"
                )
                print(
                    f"The file {file_name} now contains {os.stat(file_path).st_size} bytes"
                )
print(f"Datalake created.")
sqlite.disconnect()
