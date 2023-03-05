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
        super().__init__()
        self.postgres = PostgresConnector(schema="b3_history")

    def run_etl(self) -> None:
        """Run main ETL method."""
        # Get metadata for extraction
        self._get_last_line_read_from_postgres()

        # Check if file has already been read (remember that python considers first line as zero)
        if (self.last_line_read + 1) == self.total_lines:
            self.has_more = False
            return

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

    def _get_last_line_read_from_postgres(self) -> None:
        """Run a query to get file's last line read."""
        query = """SELECT * FROM b3_history.extraction_progress"""

        try:
            extraction_progress = self.postgres.read_sql_query(query=query)
        except UndefinedTable:
            # project's first run will create this table later on
            self.last_line_read = 0
            return

        if not len(extraction_progress):
            # In case of TRUNCATE, the table will be empty
            self.last_line_read = 0
            return

        file_filter = extraction_progress['file_name'] == self.file_name

        self.last_line_read = int(extraction_progress[file_filter]['last_line_read'].max())
