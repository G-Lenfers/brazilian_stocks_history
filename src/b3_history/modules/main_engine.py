"""File for extracting data from B3 history files, transforming, and uploading to postgres datalake"""
from datetime import datetime

import numpy as np
import pandas as pd

from src.b3_history.modules.extraction_engine import ExtractionEngine
from src.shared.databases import PostgresConnector


class MainEngine(ExtractionEngine):
    """Main class for reading zipped file, transform the dataframe and upload data to postgres."""

    def __init__(self):
        """Initialize constructor."""
        super().__init__()
        self.postgres = PostgresConnector(schema="b3_history")

    def run_etl(self) -> None:
        """Run main ETL method."""
        # Get metadata for extraction
        self._get_last_line_read_from_postgres()

        # Extract
        extracted_dataframe = self.read_and_extract_data_from_file()

        # Transform
        transformed_dataframe = self.transform_dataframe(
            dataframe=extracted_dataframe
        )

        # Load
        self.postgres.upload_data(
            dataframe=transformed_dataframe,
            table_name=self.file_name.split('.')[0].lower()
        )
        self.upload_extraction_progress()

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

    def transform_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Apply many dataframe transformations."""
        # Exclude file header
        header_filter = dataframe['data_pregao'] != "COTAHIST"
        dataframe = dataframe[header_filter]

        # Special character treatment
        dataframe.replace(
            to_replace=['\x00^', '\x00\x0f'],
            value='',
            inplace=True
        )  # TODO see why this is not working as expected

        # Remove whitespaces
        dataframe = dataframe.apply(self._remove_whitespaces)

        # The previous removal could generate np.nan values. Removing them
        known_nan_columns = ['prazo_dias_mercado_termo']
        dataframe[known_nan_columns] = dataframe[known_nan_columns].fillna("0")

        # Convert date string to date format
        date_columns = ["data_pregao", "data_vencimento_opcoes"]
        dataframe[date_columns] = dataframe[date_columns].applymap(self._format_dates)

        # Format price values
        price_columns = [
            'preco_abertura_pregao',
            'preco_maximo_pregao',
            'preco_minimo_pregao',
            'preco_medio_pregao',
            'preco_ultimo_negocio',
            'preco_melhor_oferta_compra',
            'preco_melhor_oferta_venda',
            'preco_exercicio_opcoes',
            'preco_exercicio_pontos_opcoes'
        ]
        dataframe[price_columns] = dataframe[price_columns].applymap(self._format_price_values)

        # Format string to integer
        integer_colummns = [
            'prazo_dias_mercado_termo',
            'numero_negocios_efetuados',
            'quantidade_total_titulos_negociados'
        ]
        dataframe[integer_colummns] = dataframe[integer_colummns].applymap(self._format_quantity_values)

        return dataframe

    def _get_last_line_read_from_postgres(self):
        """Run a query to get file's last line read."""
        pass
        # self.postgres = PostgresConnector(schema="b3_history")
        #
        # query = """SELECT * FROM """
        #
        # self.postgres.close_connections()

    @staticmethod
    def _remove_whitespaces(series: pd.Series) -> pd.Series:
        return series.str.rstrip().replace(r'^\s*$', np.nan, regex=True)

    @staticmethod
    def _format_dates(cell: str) -> datetime.date:
        date_format = "%Y%m%d"  # date example: 20230228
        return datetime.strptime(cell, date_format).date()

    @staticmethod
    def _format_price_values(cell: str) -> float:
        return round(int(cell)/100, 2)

    @staticmethod
    def _format_quantity_values(cell: str) -> int:
        return int(cell)

