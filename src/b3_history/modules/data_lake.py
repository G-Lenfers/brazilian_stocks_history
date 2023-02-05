"""File for extracting data from B3 history files, transforming, and uploading to postgres datalake"""
import os
import zipfile
from io import TextIOWrapper

import pandas as pd

from src.shared.databases import PostgresConnector

ROOT_PATH = os.path.abspath(  # return the absolute path of the following
    os.path.join(  # concatenate the directory of the following
        __file__,  # path of current execution file
        os.pardir  # parent directory string
    )
)
RESOURCES_PATH = '/resources/'


class B3HistoryExtractorEngine:
    """Main class for reading zipped file, transform the dataframe and upload data to postgres."""

    def __init__(self, file_name):
        self.file_name = file_name
        self.slice_collection = {
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
        self.has_more = False
        self.last_iteration = 0
        self.postgres = PostgresConnector(schema="b3_history")

    def set_has_more(self, value: bool) -> None:
        self.has_more = value

    def set_last_iteration(self, value: int) -> None:
        self.last_iteration = value

    def read_and_extract_data_from_file(self) -> pd.DataFrame:
        """Unzip, read, and store data into pandas dataframe."""
        with self._open_zipped_file() as file:

            dataframe = pd.DataFrame()

            for i, text_line in enumerate(file):

                if i < self.last_iteration:
                    continue

                sliced_text_line = self._slice_text_data(text_line=text_line)

                dataframe = pd.concat(
                    [
                        dataframe,
                        pd.DataFrame(
                            sliced_text_line,
                            index=[0]
                        )
                    ],
                    ignore_index=True
                )

                if i != 0 and i % 1000 == 0:  # TODO remember to revert this parameter to default
                    print("Dataframe cap of 10000 lines reached! Returning...")
                    self.set_last_iteration(value=i)
                    self.set_has_more(value=True)
                    return dataframe

            print(f"Reached the end of file {self.file_name}.")
            # TODO Should I set last iteration to zero?
            self.set_has_more(value=False)
            return dataframe

    def transform_dataframe(self):
        pass

    def upload_data(self):  # rather than defining here, just call from postgres
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

    def _slice_text_data(self, text_line):
        return {
            'tipo_de_registro': text_line[self.slice_collection['tipo_de_registro']],
            'data_pregao': text_line[self.slice_collection['data_pregao']],
            'codigo_bdi': text_line[self.slice_collection['codigo_bdi']],
            'codigo_negociaco_papel': text_line[self.slice_collection['codigo_negociaco_papel']],
            'tipo_de_mercado': text_line[self.slice_collection['tipo_de_mercado']],
            'nome_resumido': text_line[self.slice_collection['nome_resumido']],
            'especificacao_papel': text_line[self.slice_collection['especificacao_papel']],
            'prazo_dias_mercado_termo': text_line[self.slice_collection['prazo_dias_mercado_termo']],
            'moeda_referencia': text_line[self.slice_collection['moeda_referencia']],
            'preco_abertura_pregao': text_line[self.slice_collection['preco_abertura_pregao']],
            'preco_maximo_pregao': text_line[self.slice_collection['preco_maximo_pregao']],
            'preco_minimo_pregao': text_line[self.slice_collection['preco_minimo_pregao']],
            'preco_medio_pregao': text_line[self.slice_collection['preco_medio_pregao']],
            'preco_ultimo_negocio': text_line[self.slice_collection['preco_ultimo_negocio']],
            'preco_melhor_oferta_compra': text_line[self.slice_collection['preco_melhor_oferta_compra']],
            'preco_melhor_oferta_venda': text_line[self.slice_collection['preco_melhor_oferta_venda']],
            'numero_negocios_efetuados': text_line[self.slice_collection['numero_negocios_efetuados']],
            'quantidade_total_titulos_negociados': text_line[
                self.slice_collection['quantidade_total_titulos_negociados']
            ],
            'preco_exercicio_opcoes': text_line[self.slice_collection['preco_exercicio_opcoes']],
            'indicador_correcao_precos': text_line[self.slice_collection['indicador_correcao_precos']],
            'data_vencimento_opcoes': text_line[self.slice_collection['data_vencimento_opcoes']],
            'fator_cotacao_papel': text_line[self.slice_collection['fator_cotacao_papel']],
            'preco_exercicio_pontos_opcoes': text_line[self.slice_collection['preco_exercicio_pontos_opcoes']],
            'codigo_papel_isin': text_line[self.slice_collection['codigo_papel_isin']],
            'numero_distribuicao_papel': text_line[self.slice_collection['numero_distribuicao_papel']]
        }
