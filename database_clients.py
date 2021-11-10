import os
import sqlite3
import sys
from datetime import datetime

import psycopg2

from config import *

# from utils import write_log


def write_log(line, file_path):
    try:
        with open(file_path, "a+") as f:
            f.write(line)
    except Exception as e:
        print(e)


class PostgreSQLClient:
    def __init__(self, dbname, user, password, host, port):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
            )
            self.cursor = self.conn.cursor()
            return self.conn
        except psycopg2.Error as e:
            print("Error connecting to database: ", e)
            write_log(f"{e} - {datetime.now()}", os.path.join(LOG_DIR, LOG_FILE_NAME))
            sys.exit(1)

    def disconnect(self):
        if self.conn:
            self.conn.close()

    def execute(self, query, params=None):
        if not self.cursor:
            self.connect()
        try:
            self.cursor.execute(query, params)
            self.conn.commit()
        except psycopg2.Error as e:
            print("Error executing query: ", e)
            write_log(f"{e} - {datetime.now()}", os.path.join(LOG_DIR, LOG_FILE_NAME))
            # sys.exit(1)

    def fetch_all(self):
        if not self.cursor:
            self.connect()
        try:
            return self.cursor.fetchall()
        except psycopg2.Error as e:
            print("Error fetching results: ", e)
            write_log(f"{e} - {datetime.now()}", os.path.join(LOG_DIR, LOG_FILE_NAME))
            # sys.exit(1)

    def fetch_one(self):
        if not self.cursor:
            self.connect()
        try:
            return self.cursor.fetchone()  # returns a tuple
        except psycopg2.Error as e:
            print("Error fetching results: ", e)
            write_log(f"{e} - {datetime.now()}", os.path.join(LOG_DIR, LOG_FILE_NAME))
            # sys.exit(1)


class SQLiteClient:
    def __init__(self, dbname):
        self.dbname = dbname
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.conn = sqlite3.connect(self.dbname)
            self.cursor = self.conn.cursor()
            return self.conn
        except sqlite3.Error as e:
            print("Error connecting to database: ", e)
            write_log(f"{e} - {datetime.now()}", os.path.join(LOG_DIR, LOG_FILE_NAME))
            sys.exit(1)

    def disconnect(self):
        if self.conn:
            self.conn.close()

    def execute(self, query):
        if not self.cursor:
            self.connect()
        try:
            self.cursor.execute(query)
            self.conn.commit()
        except sqlite3.Error as e:
            print("Error executing query: ", e)
            write_log(f"{e} - {datetime.now()}", os.path.join(LOG_DIR, LOG_FILE_NAME))
            # sys.exit(1)

    def fetch_all(self):
        if not self.cursor:
            self.connect()
        try:
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            print("Error fetching results: ", e)
            write_log(f"{e} - {datetime.now()}", os.path.join(LOG_DIR, LOG_FILE_NAME))
            # sys.exit(1)

    def fetch_one(self):
        if not self.cursor:
            self.connect()
        try:
            return self.cursor.fetchone()  # returns a tuple
        except sqlite3.Error as e:
            print("Error fetching results: ", e)
            write_log(f"{e} - {datetime.now()}", os.path.join(LOG_DIR, LOG_FILE_NAME))
            # sys.exit(1)
