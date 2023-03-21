"""File for extracting data from B3 history files, transforming, and uploading to postgres datalake"""
import pandas as pd
from psycopg2.errors import UndefinedTable

from src.b3_history.modules.extraction_engine import ExtractionEngine
from src.b3_history.modules.transformation_engine import TransformationEngine
from src.shared.databases import PostgresConnector


class MainEngine(ExtractionEngine, TransformationEngine):
    """Main class for reading zipped file, transform the dataframe and upload data to postgres."""

    def __init__(self):
        """Initialize constructor."""
        # Extraction and Transformation engines inheritance
        super().__init__()

        # Postgres class composition
        self._schema = "b3_history"
        self.postgres = PostgresConnector(schema=self.schema)

    @property
    def schema(self) -> str:
        """Access attribute value."""
        return self._schema

    @schema.setter
    def schema(self, schema_name: str) -> None:
        """Define property setter and validate inputted file name."""
        if not isinstance(schema_name, str):
            raise TypeError(f"Invalid type {type(schema_name)} for schema name.")

        # Avoid SQL injection in schema name
        prohibited_characters = ['"', '.', '-', ';']
        if any([
            prohibited_character in schema_name
            for prohibited_character in prohibited_characters
        ]):
            raise ValueError("Prohibited characters found in schema name!")

        self._schema = schema_name
        self.postgres.schema = schema_name

    def run_etl(self) -> None:
        """Run main ETL method."""
        # Extract
        extracted_dataframe = self.read_and_extract_data_from_file()

        # Transform
        transformed_dataframe = self.transform_dataframe(
            dataframe=extracted_dataframe
        )

        # Load
        print("Uploading data to postgres... ", end='')
        self.postgres.upload_data(
            dataframe=transformed_dataframe,
            table_name=self.file_name.split('.')[0].lower()
        )
        self.upload_extraction_progress()
        print('Upload complete!')

    def upload_extraction_progress(self) -> None:
        """Build a dataframe from extraction attribute and upload it to datalake."""
        dataframe = pd.DataFrame([
            {
                "file_name": self.file_name,
                "last_line_read": self.last_line_read
            }
        ])
        self.postgres.upload_data(
            dataframe=dataframe,
            table_name="extraction_progress"
        )

    def create_schema_if_not_exists(self):
        """Execute statement to create schema if it doesn't exist."""
        # TODO, schema name could be class property. But have a look into how to avoid SQL injection in this transaction
        statement = "CREATE SCHEMA IF NOT EXISTS b3_history;"
        self.postgres.execute_statement(statement=statement)

    def create_update_view(self):
        """
        Orchestrate the creation of the view that aggregates all tables uploaded.

        Since there is no user input, we should be safe against SQL injection.
        This view will concatenate every existing table in schema.
        """
        all_years = range(1986, 2023)
        all_tables = [
            "cotahist_a" + str(year)
            for year in all_years
        ]

        # Check which tables exist in datalake
        table_existence_check = []
        for table in all_tables:
            table_existence_check.append(
                self.postgres.check_table_existence(table_name=table)
            )

        existent_tables = [
            table_name
            for table in table_existence_check
            for table_name, exists in table.items()
            if exists
        ]

        # It would be quite weird to arrive here with no table uploaded, but let's check it anyway
        if not existent_tables:
            return

        # build SQL statement by concatenating tables with UNION ALL
        statement_header = f"CREATE MATERIALIZED VIEW IF NOT EXISTS {self.schema}.stocks_history AS\n" \
                           f"SELECT * FROM {self.schema}.{existent_tables.pop(0)}"

        union_statement = f"\nUNION ALL\n SELECT * FROM {self.schema}."
        remaining_statement = [
            union_statement + table
            for table in existent_tables
        ]

        complete_statement = statement_header + ''.join(remaining_statement)

        # execute
        self.postgres.execute_statement(statement=complete_statement)
        print("Created view successfully!")

    def get_last_line_read_from_postgres(self) -> None:
        """Run a query to get file's last line read."""
        query = """
            SELECT *
            FROM b3_history.extraction_progress
            WHERE file_name = %(file_name)s
            ORDER BY last_line_read DESC
            LIMIT 1;
        """
        query_parameter = {'file_name': self.file_name}

        try:
            extraction_progress = self.postgres.read_sql_query(
                query=query,
                params=query_parameter
            )
        except UndefinedTable:
            # project's first run will create this table later on
            self.last_line_read = 0
            return

        if not len(extraction_progress):
            # File will be read for the first time
            self.last_line_read = 0
            return

        self.last_line_read = int(extraction_progress['last_line_read'])
