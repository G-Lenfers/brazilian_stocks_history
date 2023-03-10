"""File containing the class that applies many transformations."""
from datetime import datetime

import numpy as np
import pandas as pd


class TransformationEngine:
    """Class for cleaning, formatting and converting dataframe's data."""

    def transform_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Apply many dataframe transformations."""
        # Exclude file header and trailer
        header_trailer_filter = dataframe['data_pregao'] != "COTAHIST"
        dataframe = dataframe[header_trailer_filter]

        # Special character treatment
        special_character_columns = 'prazo_dias_mercado_termo'
        special_character_filter = dataframe[special_character_columns].str.contains(
            "\x00|\x01|\x0f|\x03|\x07|\x02|\t\""  # characters that were found during extraction
        )
        if special_character_filter.sum():
            dataframe.loc[special_character_filter, special_character_columns] = np.nan

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
        integer_columns = [
            'prazo_dias_mercado_termo',
            'numero_negocios_efetuados',
            'quantidade_total_titulos_negociados'
        ]
        dataframe[integer_columns] = dataframe[integer_columns].applymap(self._format_quantity_values)

        return dataframe

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
