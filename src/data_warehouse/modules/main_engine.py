"""Main engine for extracting stocks data from Data Lake and uploading to Data Warehouse."""
import pandas as pd

from src.shared.loading_engine import PostgresConnector


class DataWarehouseMainEngine:
    """Main class for extraction ticket price data from Data Lake, and uploading it to Data Warehouse."""

    def __init__(self):
        """Initialize constructor."""
        # Initially declare loading engine's schema as empty, will be updating as we go through
        self.postgres = PostgresConnector()

        # Schema default names
        self._data_warehouse_schema = "data_warehouse"  # can be overwritten with event parameter
        self._datalake_schema = "b3_history"  # can be overwritten with event parameter

    @property
    def data_warehouse_schema(self):
        """Access attribute value."""
        return self._data_warehouse_schema

    @data_warehouse_schema.setter
    def data_warehouse_schema(self, schema_name: str) -> None:
        """Define property setter and validate inputted schema name."""
        self._validate_schema_name(schema_name=schema_name)
        self._data_warehouse_schema = schema_name
        self.postgres.schema = schema_name

    @property
    def data_lake_schema(self):
        """Access attribute value."""
        return self._datalake_schema

    @data_lake_schema.setter
    def data_lake_schema(self, schema_name: str) -> None:
        """Define property setter and validate inputted schema name."""
        self._validate_schema_name(schema_name=schema_name)
        self._datalake_schema = schema_name

    @staticmethod
    def _validate_schema_name(schema_name: str) -> None:
        """Validate schema name in order to avoid SQL injection."""
        if not isinstance(schema_name, str):
            raise TypeError(f"Invalid type {type(schema_name)} for schema name.")

        # Avoid SQL injection in schema name
        prohibited_characters = ["'", '"', '.', '-', ';']
        if any([
            prohibited_character in schema_name
            for prohibited_character in prohibited_characters
        ]):
            raise ValueError("Prohibited characters found in schema name!")

    def extract_data_lake(self, stock: dict) -> pd.DataFrame:
        """Get ticket data from Data Lake."""
        data_lake_extraction_query = f"""
            SELECT
                sh.data_pregao,
                sh.codigo_negociaco_papel,
                sh.nome_resumido,
                sh.moeda_referencia,
                sh.preco_abertura_pregao,
                sh.preco_ultimo_negocio,
                sh.preco_maximo_pregao,
                sh.preco_minimo_pregao
            FROM {self.data_lake_schema}.stocks_history sh 
            WHERE sh.tipo_de_mercado = '010'
        """

        if not stock.get('ticket_name'):
            print('Main ticket name is mandatory. Skipping...')
            return pd.DataFrame()

        if stock.get('optional_old_ticket_name'):
            query_conditional = """
                AND (
                    sh.codigo_negociaco_papel = %(ticket_name)s
                    OR sh.codigo_negociaco_papel = %(old_ticket_name)s
                )
            """
            query_parameters = {
                "ticket_name": stock.get('ticket_name'),
                "old_ticket_name": stock.get('optional_old_ticket_name')
            }
        else:
            query_conditional = """
                AND sh.codigo_negociaco_papel = %(ticket_name)s
            """
            query_parameters = {
                "ticket_name": stock.get('ticket_name'),
            }

        extracted_ticket_data = self.postgres.read_sql_query(
            query=data_lake_extraction_query+query_conditional,
            params=query_parameters
        )
        return extracted_ticket_data

    @staticmethod
    def transform_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
        """Order dataframe values by date."""
        return dataframe.sort_values(
            by=['data_pregao'],
            ascending=True,
            ignore_index=True
        )
