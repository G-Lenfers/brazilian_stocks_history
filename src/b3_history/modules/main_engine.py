"""File for extracting data from B3 history files, transforming, and uploading to postgres datalake"""
from datetime import datetime
import os
import zipfile
from io import TextIOWrapper

import numpy as np
import pandas as pd

from src.shared.databases import PostgresConnector

ROOT_PATH = os.path.abspath(  # return the absolute path of the following
    os.path.join(  # concatenate the directory of the following
        __file__,  # path of current execution file
        os.pardir,  # parent directory of this file
        os.pardir  # parent directory of modules
    )
)
RESOURCES_PATH = '/resources/'


class B3HistoryExtractorEngine:
    """Main class for reading zipped file, transform the dataframe and upload data to postgres."""

    def __init__(self):
        """Initialize constructor."""
        # File handling properties
        self._file_name = None
        self._file_total_lines = 0

        # Extraction properties
        self.last_line_read = 0
        self._has_more = True
        self.columns_separator = {
            'tipo_de_registro': slice(0, 2),
            'data_pregao': slice(2, 10),
            'codigo_bdi': slice(10, 12),
            'codigo_negociaco_papel': slice(12, 24),
            'tipo_de_mercado': slice(24, 27),
            'nome_resumido': slice(27, 39),
            'especificacao_papel': slice(39, 49),
            'prazo_dias_mercado_termo': slice(49, 52),
            'moeda_referencia': slice(52, 56),
            'preco_abertura_pregao': slice(56, 69),
            'preco_maximo_pregao': slice(69, 82),
            'preco_minimo_pregao': slice(82, 95),
            'preco_medio_pregao': slice(95, 108),
            'preco_ultimo_negocio': slice(108, 121),
            'preco_melhor_oferta_compra': slice(121, 134),
            'preco_melhor_oferta_venda': slice(134, 147),
            'numero_negocios_efetuados': slice(147, 152),
            'quantidade_total_titulos_negociados': slice(152, 170),
            'volume_total_titulos_negociados': slice(170, 188),
            'preco_exercicio_opcoes': slice(188, 201),
            'indicador_correcao_precos': slice(201, 202),
            'data_vencimento_opcoes': slice(202, 210),
            'fator_cotacao_papel': slice(210, 217),
            'preco_exercicio_pontos_opcoes': slice(217, 230),
            'codigo_papel_isin': slice(230, 242),
            'numero_distribuicao_papel': slice(242, 245)
        }

        # Upload engine
        self.postgres = PostgresConnector(schema="b3_history")  # TODO create engine before upload and dispose it rigth after

    @property
    def file_name(self):
        """Access attribute value."""
        return self._file_name

    @file_name.setter
    def file_name(self, new_file_name):
        """Define property setter and validate inputted file name."""
        if isinstance(new_file_name, str) and ".zip" in new_file_name:
            self._file_name = new_file_name
        else:
            raise TypeError("Invalid file_name. Please, check your event list")

    @property
    def total_lines(self):
        """Access attribute value."""
        return self._file_total_lines

    @total_lines.setter
    def total_lines(self, value):
        """Define property setter and validate input."""
        if isinstance(value, int) and value > 0:  # TODO errors first
            self._file_total_lines = value
        else:
            raise ValueError("Total number of lines should be an integer and must not be negative.")

    @property
    def has_more(self):
        """Access attribute value."""
        return self._has_more

    @has_more.setter
    def has_more(self, value):
        """Define property setter and validate input."""
        if not isinstance(value, bool):
            raise TypeError("Property has_more should be of type boolean.")

        self.has_more = value

    def set_last_iteration(self, value: int) -> None:
        self.last_iteration = value

    def run_etl(self) -> None:
        """Run main ETL method."""
        self._get_last_iteration_from_postgres()
        while self.has_more:

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

        self.postgres.close_connections()

    def read_and_extract_data_from_file(self) -> pd.DataFrame:
        """Unzip, read, and store data into pandas dataframe."""
        with self._open_zipped_file() as file:

            dataframe = pd.DataFrame()

            for i, text_line in enumerate(file):

                if i < self.last_line_read:
                    continue

                separated_columns = self.separate_columns(text_line=text_line)

                dataframe = pd.concat(
                    [
                        dataframe,
                        pd.DataFrame(
                            separated_columns,
                            index=[0]
                        )
                    ],
                    ignore_index=True
                )

                if i != 0 and i % 100 == 0:  # TODO remember to revert this parameter to default
                    print(f"i: {i}. Finished reading 10000 lines.")
                    self.set_last_iteration(value=i+1)
                    self.set_has_more(value=True)
                    return dataframe

            print(f"Reached the end of file {self.file_name}.")
            self.set_has_more(value=False)
            return dataframe

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

    def get_file_total_lines(self):
        """Quickly read file and get its total number of lines."""
        # Open compressed file
        with self._open_zipped_file() as file:

            # Iterate over lines
            for i, _ in enumerate(file):
                pass

        # Save into class property
        print(f"Raw total lines: {i}")
        return i + 1  # Enumerate starts at zero

    def _get_last_iteration_from_postgres(self):
        # TODO upload_health_check
        pass

    def _open_zipped_file(self):
        """Open zipped file and read it in non-binary mode."""
        zipped_file = ROOT_PATH + RESOURCES_PATH + self.file_name
        with zipfile.ZipFile(zipped_file, 'r') as my_zip:
            return TextIOWrapper(
                my_zip.open(
                    my_zip.namelist()[0]
                )
            )

    def separate_columns(self, text_line) -> dict:
        """Slice text data and separate content appropriately."""
        return {
            'tipo_de_registro': text_line[self.columns_separator['tipo_de_registro']],
            'data_pregao': text_line[self.columns_separator['data_pregao']],
            'codigo_bdi': text_line[self.columns_separator['codigo_bdi']],
            'codigo_negociaco_papel': text_line[self.columns_separator['codigo_negociaco_papel']],
            'tipo_de_mercado': text_line[self.columns_separator['tipo_de_mercado']],
            'nome_resumido': text_line[self.columns_separator['nome_resumido']],
            'especificacao_papel': text_line[self.columns_separator['especificacao_papel']],
            'prazo_dias_mercado_termo': text_line[self.columns_separator['prazo_dias_mercado_termo']],
            'moeda_referencia': text_line[self.columns_separator['moeda_referencia']],
            'preco_abertura_pregao': text_line[self.columns_separator['preco_abertura_pregao']],
            'preco_maximo_pregao': text_line[self.columns_separator['preco_maximo_pregao']],
            'preco_minimo_pregao': text_line[self.columns_separator['preco_minimo_pregao']],
            'preco_medio_pregao': text_line[self.columns_separator['preco_medio_pregao']],
            'preco_ultimo_negocio': text_line[self.columns_separator['preco_ultimo_negocio']],
            'preco_melhor_oferta_compra': text_line[self.columns_separator['preco_melhor_oferta_compra']],
            'preco_melhor_oferta_venda': text_line[self.columns_separator['preco_melhor_oferta_venda']],
            'numero_negocios_efetuados': text_line[self.columns_separator['numero_negocios_efetuados']],
            'quantidade_total_titulos_negociados': text_line[
                self.columns_separator['quantidade_total_titulos_negociados']
            ],
            'preco_exercicio_opcoes': text_line[self.columns_separator['preco_exercicio_opcoes']],
            'indicador_correcao_precos': text_line[self.columns_separator['indicador_correcao_precos']],
            'data_vencimento_opcoes': text_line[self.columns_separator['data_vencimento_opcoes']],
            'fator_cotacao_papel': text_line[self.columns_separator['fator_cotacao_papel']],
            'preco_exercicio_pontos_opcoes': text_line[self.columns_separator['preco_exercicio_pontos_opcoes']],
            'codigo_papel_isin': text_line[self.columns_separator['codigo_papel_isin']],
            'numero_distribuicao_papel': text_line[self.columns_separator['numero_distribuicao_papel']]
        }

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

