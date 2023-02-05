"""File containing methods for Postgres."""
import os
from typing import TYPE_CHECKING

import sqlalchemy.engine.base
from sqlalchemy import create_engine

if TYPE_CHECKING:
    import pandas as pd


class PostgresConnector:
    """Class for uploading data to Postgres."""

    def __init__(self, schema: str) -> None:
        """Initialize the constructor."""
        # Upload parameter
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
        self.engine = self._connect_to_database()

    def _connect_to_database(self) -> sqlalchemy.engine.base.Engine:
        """Connect to Postgres server."""
        return create_engine(
            f"{self.dialect}+{self.driver}://"
            f"{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )

    def upload_data(self, dataframe: 'pd.DataFrame', table_name: str) -> None:
        """Use pandas to_sql method and sqlalchemy engine to send data to postgres."""
        dataframe.to_sql(
            name=table_name,
            con=self.engine,
            schema=self.schema,
            if_exists='append',
            index=False,
            chunksize=1000
        )

    def close_connections(self) -> None:
        """Close all connections."""
        self.engine.dispose()
