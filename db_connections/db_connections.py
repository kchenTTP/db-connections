import os
import sqlite3
from typing import Literal

import pandas as pd
import psycopg2

from ._logging_config import _get_logger

logger = _get_logger()


class PostgresConnection:
    def __init__(self, conn_string: str | None) -> None:
        self._conn = psycopg2.connect(conn_string)
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        try:
            self.close()
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
        if exc_type:
            raise exc_type(exc_value, exc_tb)

    @property
    def connection(self) -> psycopg2.extensions.connection:
        return self._conn

    @property
    def cursor(self) -> psycopg2.extensions.cursor:
        return self._cursor

    def check_table_exists(self, table_name: str) -> bool:
        sql = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');"
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchone()[0] == 1  # type: ignore
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            raise

    def commit(self) -> None:
        self.connection.commit()

    def close(self, commit=True) -> None:
        if commit:
            self.commit()
        self.connection.close()

    def create_table(self, table_name: str, schema: dict[str, str]) -> None:
        if self.check_table_exists(table_name):
            logger.info(f"Table: {table_name} already exists, skipping creation")
            return

        column_defs = []

        for name, definition in schema.items():
            column_defs.append(f"{name} {definition}")
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(column_defs)})"

        try:
            self.execute(sql)
            self.commit()
            logger.info(f"Table: {table_name} created")
        except Exception as e:
            self.rollback()
            logger.error(f"Error creating table: {e}")
            raise

    def execute(self, sql: str, params: dict = {}) -> None:
        self.cursor.execute(sql, params or ())

    def executemany(self, sql: str, vars_list: list[tuple]) -> None:
        self.cursor.executemany(sql, vars_list)

    def fetchall(self) -> list[tuple]:
        return self.cursor.fetchall()

    def insert_dataframe(self, table_name: str, df: pd.DataFrame) -> None:
        columns = df.columns.tolist()
        # Placeholders for all columns, [:-1] remove trailing comma
        placeholders = ("%s," * len(columns))[:-1]
        sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
        logger.debug(sql)

        data = df.to_dict(orient="records")
        data = [tuple(dict.values()) for dict in data]
        logger.debug(data[:5])

        try:
            self.executemany(sql, data)
            self.commit()
        except Exception as e:
            self.rollback()
            logger.error(f"Error inserting dataframe: {e}")
            raise

    def rollback(self) -> None:
        self.connection.rollback()


class SQLiteConnection:
    def __init__(self, database: str | os.PathLike | Literal[":memory:"]) -> None:
        self._conn = sqlite3.connect(database)
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        try:
            self.close()
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
        if exc_type:
            raise exc_type(exc_value, exc_tb)

    @property
    def connection(self) -> sqlite3.Connection:
        return self._conn

    @property
    def cursor(self) -> sqlite3.Cursor:
        return self._cursor

    def check_table_exists(self, table_name: str) -> bool:
        sql = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}');"
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchone()[0] == 1  # type: ignore
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            raise

    def commit(self) -> None:
        self.connection.commit()

    def close(self, commit=True) -> None:
        if commit:
            self.commit()
        self.connection.close()

    def create_table(self, table_name: str, schema: dict[str, str]) -> None:
        if self.check_table_exists(table_name):
            logger.info(f"Table: {table_name} already exists, skipping creation")
            return

        column_defs = []

        for name, definition in schema.items():
            column_defs.append(f"{name} {definition}")
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(column_defs)})"

        try:
            self.execute(sql)
            self.commit()
            logger.info(f"Table: {table_name} created")
        except Exception as e:
            self.rollback()
            logger.error(f"Error creating table: {e}")
            raise

    def execute(self, sql: str, params: dict = {}) -> None:
        self.cursor.execute(sql, params or ())

    def executemany(self, sql: str, vars_list: list[tuple]) -> None:
        self.cursor.executemany(sql, vars_list)

    def fetchall(self) -> list[tuple]:
        return self.cursor.fetchall()

    def insert_dataframe(self, table_name: str, df: pd.DataFrame) -> None:
        columns = df.columns.tolist()
        # Placeholders for all columns, [:-1] remove trailing comma
        placeholders = ("%s," * len(columns))[:-1]
        sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
        logger.debug(sql)

        data = df.to_dict(orient="records")
        data = [tuple(dict.values()) for dict in data]
        logger.debug(data[:5])

        try:
            self.executemany(sql, data)
            self.commit()
        except Exception as e:
            self.rollback()
            logger.error(f"Error inserting dataframe: {e}")
            raise

    def rollback(self) -> None:
        self.connection.rollback()
