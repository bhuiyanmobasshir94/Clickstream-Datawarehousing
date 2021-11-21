import io

import pandas as pd
from sqlalchemy import create_engine

from config import *
from database_clients import PostgreSQLClient, SQLiteClient


def create_client_engine():
    if USE_POSTGRESQL:
        logger.info("Using PostgreSQL")
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
        logger.info("Using SQLite")
        client = SQLiteClient(os.path.join(DATALAKE_DIR, "db", "datawarehouse.sqlite"))
        client_engine = client.connect()

    return client, client_engine


def validate_df(df: pd.DataFrame) -> pd.DataFrame:
    # df_with_null = df[df.isnull().any(axis=1)]
    df["number_of_occurrences"] = pd.to_numeric(
        df["number_of_occurrences"], errors="coerce"
    )
    df_without_null = df.dropna()
    df_with_null = df.drop(df_without_null.index)
    df_with_null["referrer"].fillna("no referrer", inplace=True)
    df_with_null["resource"].fillna("no resource", inplace=True)
    df_with_null["type"].fillna("no type", inplace=True)
    df_with_null["number_of_occurrences"].fillna(0, inplace=True)
    df = pd.concat([df_without_null, df_with_null], ignore_index=True)
    df = df.astype(
        {"referrer": str, "resource": str, "type": str, "number_of_occurrences": int,}
    )
    for col in df.columns:
        if df[col].dtype == object and (df[col].str.len()).max() > 500:
            logger.critical(f"{col} has length {(df[col].str.len()).max()}")
            pdf = df.loc[(df[col].str.len() > 500), col]
            df.drop(pdf.index, inplace=True)
            logger.critical(f"Skipping {len(pdf.index)} indexes: {str(pdf.index)} ")
            for index, value in pdf.items():
                file = io.StringIO(value)
                error_file_path = os.path.join(
                    LOGS_DIR, f"skipped_col_{col}_index_{index}.txt"
                )
                if os.path.exists(error_file_path):
                    os.remove(error_file_path)
                with open(error_file_path, "w") as f:
                    f.write(file.read())

    return df
