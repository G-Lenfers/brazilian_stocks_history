"""File containing methods for Postgres."""
import os

import pandas as pd
from sqlalchemy import create_engine


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
        self.engine = None

    def _connect_to_database(self) -> None:
        """Connect to Postgres server."""
        self.engine = create_engine(
            f"{self.dialect}+{self.driver}://"
            f"{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )

    def upload_data(self, dataframe: pd.DataFrame, table_name: str) -> None:
        """Use pandas to_sql method and sqlalchemy engine to send data to postgres."""
        print("Uploading data to postgres...", end='')
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
        print('Upload complete!')

    def read_sql_query(self, query) -> pd.DataFrame:
        """Run a query in the database and return its result as a dataframe."""
        self._connect_to_database()
        dataframe = pd.read_sql_query(
            sql=query,
            con=self.engine
        )
        self.close_connections()

        return dataframe

    def close_connections(self) -> None:
        """Close all connections."""
        self.engine.dispose()
