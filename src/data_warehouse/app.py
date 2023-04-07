"""Filter specific ticket data from datalake and upload it to data warehouse."""
import pandas as pd

from src.shared.loading_engine import PostgresConnector


def lambda_function(event: list) -> None:
    """Orchestrate accordingly."""
    # Basic postgres setup
    postgres = PostgresConnector(schema="data_warehouse")
    postgres.create_schema_database()  # must have 'create' privilege

    # Extract
    # TODO Check view existence
    # TODO schema name as parameter from event
    data_lake_extraction_query = """
        SELECT
            sh.data_pregao,
            sh.codigo_negociaco_papel,
            sh.nome_resumido,
            sh.moeda_referencia,
            sh.preco_abertura_pregao,
            sh.preco_ultimo_negocio,
            sh.preco_maximo_pregao,
            sh.preco_minimo_pregao
        FROM b3_history.stocks_history sh 
        WHERE sh.tipo_de_mercado = '010'
        -- ORDER BY data_pregao ASC
    """  # TODO order by in pandas, not in query
    for stock in event:

        if not stock.get('ticket_name'):
            print('Main ticket name is mandatory. Skipping...')
            continue

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
        extracted_ticket_data = postgres.read_sql_query(
            query=data_lake_extraction_query+query_conditional,
            params=query_parameters
        )



if __name__ == "__main__":
    event = [
        {
            "ticket_name": "VALE3",
            "optional_old_ticket_name": "VAL 3"
        },
        {
            "ticket_name": "PETR3",
            "optional_old_ticket_name": "PET 3"
        }
    ]
    lambda_function(event=event)
