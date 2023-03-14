"""File containing methods for Postgres."""
import os

import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError


class PostgresConnector:
    """Class for uploading data to Postgres."""

    def __init__(self, schema: str) -> None:
        """Initialize the constructor."""
        # Database parameter
        self.schema = schema

        # Database credentials
        self.host = os.environ['SQL_HOST']
        self.port = os.environ['SQL_PORT']
        self.user = os.environ['SQL_USER']
        self.password = os.environ['SQL_PASS']
        self.database = os.environ['SQL_DB']

        # Engine connection parameters
        self.dialect = 'postgresql'
        self.driver = 'psycopg2'
        self.connection = None
        self.engine = None

    def _connect_to_database(self) -> None:
        """Connect to Postgres server."""
        self.connection = psycopg2.connect(
            database=self.database,
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
        )

        self.engine = create_engine(
            f"{self.dialect}+{self.driver}://"
            f"{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )

    def upload_data(self, dataframe: pd.DataFrame, table_name: str) -> None:
        """Use pandas to_sql method and sqlalchemy engine to send data to postgres."""
        self._connect_to_database()
        dataframe.to_sql(
            name=table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='append',
            index=False,
            chunksize=1000
        )
        self.close_connections()

    def read_sql_query(self, query: str, params: dict) -> pd.DataFrame:
        """Run a query in the database and return its result as a dataframe."""
        self._connect_to_database()
        try:
            dataframe = pd.read_sql_query(
                sql=query,
                con=self.engine,
                params=params
            )
        except ProgrammingError as error:
            # In case of UndefinedTable error, we re-raise it to catch the original error
            raise error.orig

        finally:
            self.close_connections()

        return dataframe

    def execute_statement(self, statement):
        """Execute and commit statement."""
        # Create engine
        self._connect_to_database()

        try:
            # Use engine to connect to database
            with self.engine.connect() as connection:

                # Execute and autocommit
                connection.execute(statement)

        except ProgrammingError as error:
            # In case of InsufficientPrivilege error, we re-raise it to catch the original error
            raise error.orig

        finally:
            # Dispose engine
            self.close_connections()

    def count_rows(self, table_name: str) -> int:
        """Check table existence by counting its row."""
        self._connect_to_database()

        with self.connection.cursor() as cursor:
            statement = sql.SQL("""
                SELECT
                    count(*)
                FROM
                    b3_history.{table_name}
            """).format(
                table_name=sql.Identifier(table_name),
            )
            cursor.execute(statement)  # TODO try-except
            result = cursor.fetchone()

        self.close_connections()

        row_count, = result
        return row_count

    def close_connections(self) -> None:
        """Close all connections."""
        self.engine.dispose()
        # TODO close psycopg2 connection
