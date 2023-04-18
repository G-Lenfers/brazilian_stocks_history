"""File containing methods for Postgres."""
import os

import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError


class PostgresConnector:
    """Class for uploading data to Postgres."""

    def __init__(self, schema: str = "") -> None:
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

    def create_schema_database(self) -> None:
        """Execute schema creation statement."""
        self._connect_to_database()
        self.connection.set_session(autocommit=True)

        with self.connection.cursor() as cursor:
            statement = sql.SQL("""
                CREATE SCHEMA IF NOT EXISTS {schema_name}
            """).format(
                schema_name=sql.Identifier(self.schema)
            )
            cursor.execute(statement)

        self.close_connections()

    def check_table_existence(self, table_name: str) -> dict:
        """Check if given table exists inside schema."""
        self._connect_to_database()

        with self.connection.cursor() as cursor:
            statement = sql.SQL("""
            SELECT EXISTS (
                SELECT * FROM pg_catalog.pg_tables pt
                WHERE pt.schemaname = {schema_name}
                    AND pt.tablename = {table_name}
            );
            """).format(
                schema_name=sql.Literal(self.schema),
                table_name=sql.Literal(table_name)
            )
            # There is no need to put this execution inside try-except block
            # Even without any privilege, one can still safely run this query
            cursor.execute(statement)
            result = cursor.fetchone()

        self.close_connections()

        table_exists, = result
        return {table_name: table_exists}

    def check_materialized_view_existence(self, view_name: str) -> dict:
        """Check if given materialized view exists inside schema."""
        self._connect_to_database()

        with self.connection.cursor() as cursor:
            statement = sql.SQL("""
            SELECT EXISTS (
                SELECT *
                FROM pg_catalog.pg_matviews pm
                WHERE pm.schemaname = {schema_name}
                    AND pm.matviewname = {view_name}
            );
            """).format(
                schema_name=sql.Literal(self.schema),
                view_name=sql.Literal(view_name)
            )
            # There is no need to put this execution inside try-except block
            # Even without any privilege, one can still safely run this query
            cursor.execute(statement)
            result = cursor.fetchone()

        self.close_connections()

        view_exists, = result
        return view_exists

    def close_connections(self) -> None:
        """Close all connections."""
        self.engine.dispose()
        self.connection.close()
