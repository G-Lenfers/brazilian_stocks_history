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
        self.schema = "b3_history"
        self.postgres = PostgresConnector(schema=self.schema)

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

    def create_update_view(self):
        """"""
        all_tables = [
            'cotahist_a1986', 'cotahist_a1987', 'cotahist_a1988', 'cotahist_a1989', 'cotahist_a1990',
            'cotahist_a1991', 'cotahist_a1992', 'cotahist_a1993', 'cotahist_a1994', 'cotahist_a1995',
            'cotahist_a1996', 'cotahist_a1997', 'cotahist_a1998', 'cotahist_a1999', 'cotahist_a2000',
            'cotahist_a2001', 'cotahist_a2002', 'cotahist_a2003', 'cotahist_a2004', 'cotahist_a2005',
            'cotahist_a2006', 'cotahist_a2007', 'cotahist_a2008', 'cotahist_a2009', 'cotahist_a2010',
            'cotahist_a2011', 'cotahist_a2012', 'cotahist_a2013', 'cotahist_a2014', 'cotahist_a2015',
            'cotahist_a2016', 'cotahist_a2017', 'cotahist_a2018', 'cotahist_a2019', 'cotahist_a2020',
            'cotahist_a2021', 'cotahist_a2022'
        ]
        for table in all_tables:

            # Check its existence in datalake
            # SELECT COUNT(*) FROM {schema}.{table}
            # if count > 0 table is ok
            rows = self.postgres.count_rows(table_name=table)
            print(rows)

            # build query
            # for every table ok, concatenate UNION ALL statement

            pass

        # execute query create or replace view

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
