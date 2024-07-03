import os
import sqlite3
from typing import Literal

import libsql_experimental as libsql
import pandas as pd
import psycopg2

from ._logging_config import _get_logger

logger = _get_logger()


class PostgresConnection:
    def __init__(
        self,
        db_uri: str | os.PathLike | None,
        sslmode: Literal["disable", "require", "verify-ca", "verify-full"] = "disable",
        sslrootcert: str | os.PathLike | None = None,
    ) -> None:
        if isinstance(db_uri, os.PathLike):
            db_uri = str(db_uri)

        if sslmode == "disable":
            self._conn = psycopg2.connect(db_uri)
        else:
            assert (
                sslrootcert
            ), "Parameter: sslrootcert cannot be None when sslmode is not 'disable'"
            self._conn = psycopg2.connect(db_uri, sslmode=sslmode, sslrootcert=sslrootcert)

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

    def create_table(self, table_name: str, schema: dict[str, str], unsafe: bool = False) -> None:
        if self.check_table_exists(table_name) and not unsafe:
            logger.info(f"Table: '{table_name}' already exists, skipping creation")
            return

        column_defs = []

        for name, definition in schema.items():
            column_defs.append(f"{name} {definition}")
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(column_defs)});"

        try:
            self.execute(sql)
            self.commit()
            logger.info(f"Table: '{table_name}' created")
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
        sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders});"
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
        sql = (
            f"SELECT '{table_name}' FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        )
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchone() is not None  # type: ignore
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            raise

    def commit(self) -> None:
        self.connection.commit()

    def close(self, commit=True) -> None:
        if commit:
            self.commit()
        self.connection.close()

    def create_table(self, table_name: str, schema: dict[str, str], unsafe: bool = False) -> None:
        if self.check_table_exists(table_name) and not unsafe:
            logger.info(f"Table: '{table_name}' already exists, skipping creation")
            return

        column_defs = []

        for name, definition in schema.items():
            column_defs.append(f"{name} {definition}")
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(column_defs)});"

        try:
            self.execute(sql)
            self.commit()
            logger.info(f"Table: '{table_name}' created")
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
        placeholders = ("?," * len(columns))[:-1]
        sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders});"
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
            logger.error(sql)
            logger.error(data)
            raise

    def rollback(self) -> None:
        self.connection.rollback()


def db_type(
    sql_type: Literal["sqlite", "postgresql"] = "sqlite",
) -> type[SQLiteConnection] | type[PostgresConnection]:
    match sql_type:
        case "sqlite":
            conn_class = SQLiteConnection
        case "postgresql":
            conn_class = PostgresConnection
        case _:
            raise ValueError(
                f"Unsupported SQL type: {sql_type}. Please use either 'sqlite' or 'postgresql'."
            )
    return conn_class


class LibSQLConnection_Experimental:
    def __init__(
        self, db_name: str, db_url: str | os.PathLike | None = None, auth_token=None
    ) -> None:
        if db_url and auth_token:
            self._conn = libsql.connect(db_name, sync_url=db_url, auth_token=auth_token)
        else:
            self._conn = libsql.connect(db_name)
        self._conn.sync()
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
    def connection(self) -> libsql.Connection:
        return self._conn

    @property
    def cursor(self) -> libsql.Cursor:
        return self._cursor

    def check_table_exists(self, table_name: str) -> bool:
        sql = (
            f"SELECT '{table_name}' FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        )
        try:
            self.cursor.execute(sql)
            return self.cursor.fetchone() is not None  # type: ignore
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            raise

    def commit(self) -> None:
        self.connection.commit()

    def close(self, commit=True) -> None:
        if commit:
            self.commit()
            self.connection.sync()
        # FIXME: connection.close not implemented yet in libsql
        self.connection.close()

    def create_table(self, table_name: str, schema: dict[str, str], unsafe: bool = False) -> None:
        if self.check_table_exists(table_name) and not unsafe:
            logger.info(f"Table: '{table_name}' already exists, skipping creation")
            return

        column_defs = []

        for name, definition in schema.items():
            column_defs.append(f"{name} {definition}")
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(column_defs)});"

        try:
            self.execute(sql)
            self.commit()
            logger.info(f"Table: '{table_name}' created")
        except Exception as e:
            self.rollback()
            logger.error(f"Error creating table: {e}")
            raise

    def execute(self, sql: str, params: dict = {}) -> None:
        self.cursor.execute(sql, params or ())

    def executemany(self, sql: str, vars_list: list[tuple]) -> None:
        # FIXME: cursor.executemany not implemented yet in libsql
        # self.cursor.executemany(sql, vars_list)
        # FIXME: replace this for loop after cursor.executemany is implemented in libsql
        for vals in vars_list:
            self.cursor.execute(sql, vals)

    def fetchall(self) -> list[tuple]:
        return self.cursor.fetchall()

    def insert_dataframe(self, table_name: str, df: pd.DataFrame) -> None:
        columns = df.columns.tolist()
        # Placeholders for all columns, [:-1] remove trailing comma
        placeholders = ("?," * len(columns))[:-1]
        sql = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders});"
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


def db_type(
    sql_type: Literal["sqlite", "postgresql", "libsql"] = "sqlite",
) -> type[SQLiteConnection] | type[PostgresConnection] | type[LibSQLConnection_Experimental]:
    match sql_type:
        case "sqlite":
            conn_class = SQLiteConnection
        case "postgresql":
            conn_class = PostgresConnection
        case "libsql":
            conn_class = LibSQLConnection_Experimental
        case _:
            raise ValueError(
                f"Unsupported SQL type: {sql_type}. Please use either 'sqlite' or 'postgresql'."
            )
    return conn_class
