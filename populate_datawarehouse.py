import os
from datetime import datetime

import pandas as pd

from database_clients import SQLiteClient

sqlite = SQLiteClient("datawarehouse.sqlite")
sqlite_engine = sqlite.connect()

condition = ("2017", "2021")  # year range

datacatalog_number_of_rows = pd.read_sql(
    "SELECT COUNT(*) FROM datacatalog", sqlite_engine
).iloc[0, 0]

if datacatalog_number_of_rows > 0:
    print("Datacatalog already populated.")
    datacatalog = pd.read_sql("SELECT * FROM datacatalog", sqlite_engine)
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
                df = pd.read_table(file_path, compression="gzip")
                df.columns = columns
                df["Date"] = str(datetime.strptime(folder_name, "%Y-%m").date())
                df.to_sql(
                    name="clickstream",
                    con=sqlite_engine,
                    if_exists="append",
                    index=False,
                )
                # data_catalog.loc[index, "data_inserted"] = True
                sqlite.execute(
                    f"UPDATE datacatalog SET data_inserted = True WHERE file_name = '{file_name}'"
                )
                # data_catalog.loc[index, "number_of_rows"] = df.shape[0]
                sqlite.execute(
                    f"UPDATE datacatalog SET number_of_rows = {df.shape[0]} WHERE file_name = '{file_name}'"
                )
                print(f"{df.shape[0]} rows ingested from {file_name}.")
else:
    import create_datalake

sqlite.disconnect()
