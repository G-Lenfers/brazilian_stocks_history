import os
import zipfile
from io import TextIOWrapper

import pandas as pd

ROOT_PATH = os.path.abspath(  # return the absolute path of the following
    os.path.join(  # concatenate the directory of the following
        __file__,  # path of current execution file
        os.pardir,  # parent directory of this file
        os.pardir  # parent directory of modules
    )
)
RESOURCES_PATH = '/resources/'


class ExtractionEngine:
    """"""

    def __init__(self):
        """Initialize the constructor."""
        # File handling properties
        self._file_name = None
        self._file_total_lines = 0

        # Extraction properties
        self._batch_size = 1000
        self._has_more = True
        self._last_line_read = 0  # First line to read will have i=0
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

    @property
    def file_name(self) -> str:
        """Access attribute value."""
        return self._file_name

    @file_name.setter
    def file_name(self, new_file_name: str) -> None:
        """Define property setter and validate inputted file name."""
        if not isinstance(new_file_name, str):
            raise TypeError("Invalid file_name. Please, check your event list")

        if ".zip" not in new_file_name:
            raise ValueError(f"Expected extension .zip, got {new_file_name[-4:]} instead.")

        self._file_name = new_file_name

    @property
    def total_lines(self) -> int:
        """Access attribute value."""
        return self._file_total_lines

    @total_lines.setter
    def total_lines(self, value: int) -> None:
        """Define property setter and validate input."""
        if not isinstance(value, int):
            raise TypeError("Property file_total_lines should be of type integer, "
                            f"got {type(value)} instead.")
        if value < 0:
            raise ValueError("Total number of lines must not be negative.")

        self._file_total_lines = value

    @property
    def batch_size(self) -> int:
        """Access attribute value."""
        return self.batch_size

    @batch_size.setter
    def batch_size(self, value: int) -> None:
        """Define property setter and validate inputted file name."""
        if not isinstance(value, int):
            try:
                value = int(value)

            except ValueError:
                print('Invalid input of batch size. Please check your event.\n'
                      'Using default value of 1000...')
                self._batch_size = 1000
                return

            except TypeError:
                print('Invalid input of batch size. Please check your event.\n'
                      'Using default value of 1000...')
                self._batch_size = 1000
                return

        if value < 0:
            print('Warning! Batch size parameter must not be negative.\n'
                  'Using default value of 1000...')
            self._batch_size = 1000
            return

        self._batch_size = value

    @property
    def has_more(self) -> bool:
        """Access attribute value."""
        return self._has_more

    @has_more.setter
    def has_more(self, value: bool) -> None:
        """Define property setter and validate input."""
        if not isinstance(value, bool):
            raise TypeError("Property has_more should be of type boolean.")

        self._has_more = value

    @property
    def last_line_read(self) -> int:
        """Access attribute value."""
        return self._last_line_read

    @last_line_read.setter
    def last_line_read(self, value: int) -> None:
        """Define property setter and validate input."""
        if not isinstance(value, int):
            raise TypeError("Property last_line_read should be of type integer.")

        if value < 0:
            raise ValueError("Property last_line_read should not be negative.")

        self._last_line_read = value

    def get_file_total_lines(self):
        """Quickly read file and get its total number of lines."""
        # Open compressed file
        with self._open_zipped_file(file_name=self.file_name) as file:

            # Iterate over lines
            for i, _ in enumerate(file):
                pass

        # Save into class property
        print(f"File {self.file_name} total lines: {i}")
        return i + 1  # Enumerate starts at zero

    def read_and_extract_data_from_file(self) -> pd.DataFrame:
        """
        Unzip, read, and store data into pandas dataframe.

        Note: File's first line will not be read! (it will hit the continue statement)
        This way, we can state firmly state that last_line_read parameter must not be negative.
        The first line would have been thrown away inside transformation engine anyway.
        """
        with self._open_zipped_file(file_name=self.file_name) as file:

            dataframe = pd.DataFrame()

            print('Reading file... ', end='')
            for line_row, line_text in enumerate(file):

                # Avoid re-reading lines that had already been read
                if line_row <= self.last_line_read:
                    continue

                # Split line's content
                split_content = self._separate_columns(text=line_text)

                # Concatenate this loop's content with the previous ones
                dataframe = pd.concat(
                    [
                        dataframe,
                        pd.DataFrame(
                            split_content,
                            index=[0]
                        )
                    ],
                    ignore_index=True
                )

                # Batch completion verification
                if line_row != 0 and line_row % self.batch_size == 0:
                    print(f"Batch {line_row} completed!")
                    self.last_line_read = line_row
                    self.has_more = True
                    return dataframe

            print(f"Reached the end of file {self.file_name}.")
            self.last_line_read = line_row
            self.has_more = False
            return dataframe

    def _separate_columns(self, text) -> dict:
        """Slice text data and split its content appropriately."""
        return {
            'tipo_de_registro': text[self.columns_separator['tipo_de_registro']],
            'data_pregao': text[self.columns_separator['data_pregao']],
            'codigo_bdi': text[self.columns_separator['codigo_bdi']],
            'codigo_negociaco_papel': text[self.columns_separator['codigo_negociaco_papel']],
            'tipo_de_mercado': text[self.columns_separator['tipo_de_mercado']],
            'nome_resumido': text[self.columns_separator['nome_resumido']],
            'especificacao_papel': text[self.columns_separator['especificacao_papel']],
            'prazo_dias_mercado_termo': text[self.columns_separator['prazo_dias_mercado_termo']],
            'moeda_referencia': text[self.columns_separator['moeda_referencia']],
            'preco_abertura_pregao': text[self.columns_separator['preco_abertura_pregao']],
            'preco_maximo_pregao': text[self.columns_separator['preco_maximo_pregao']],
            'preco_minimo_pregao': text[self.columns_separator['preco_minimo_pregao']],
            'preco_medio_pregao': text[self.columns_separator['preco_medio_pregao']],
            'preco_ultimo_negocio': text[self.columns_separator['preco_ultimo_negocio']],
            'preco_melhor_oferta_compra': text[self.columns_separator['preco_melhor_oferta_compra']],
            'preco_melhor_oferta_venda': text[self.columns_separator['preco_melhor_oferta_venda']],
            'numero_negocios_efetuados': text[self.columns_separator['numero_negocios_efetuados']],
            'quantidade_total_titulos_negociados': text[
                self.columns_separator['quantidade_total_titulos_negociados']
            ],
            'preco_exercicio_opcoes': text[self.columns_separator['preco_exercicio_opcoes']],
            'indicador_correcao_precos': text[self.columns_separator['indicador_correcao_precos']],
            'data_vencimento_opcoes': text[self.columns_separator['data_vencimento_opcoes']],
            'fator_cotacao_papel': text[self.columns_separator['fator_cotacao_papel']],
            'preco_exercicio_pontos_opcoes': text[self.columns_separator['preco_exercicio_pontos_opcoes']],
            'codigo_papel_isin': text[self.columns_separator['codigo_papel_isin']],
            'numero_distribuicao_papel': text[self.columns_separator['numero_distribuicao_papel']]
        }

    @staticmethod
    def _open_zipped_file(file_name: str):
        """Open zipped file and read it in non-binary mode."""
        zipped_file = ROOT_PATH + RESOURCES_PATH + file_name
        with zipfile.ZipFile(zipped_file, 'r') as my_zip:
            return TextIOWrapper(
                my_zip.open(
                    my_zip.namelist()[0]
                )
            )
