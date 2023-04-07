"""Filter specific ticket data from datalake and upload it to data warehouse."""
from src.data_warehouse.modules.main_engine import DataWarehouseMainEngine


def lambda_function(event: dict) -> None:
    """Orchestrate accordingly."""
    # Instance main engine
    engine = DataWarehouseMainEngine()

    # Data Lake schema verification
    if event.get('datalake_schema'):
        engine.data_lake_schema = event['datalake_schema']
    else:
        engine.data_lake_schema = "b3_history"

    view_exists = engine.postgres.check_materialized_view_existence(view_name="stocks_history")
    if not view_exists:
        print("This app is supposed to run only after the creation of data lake materialized view.")
        return

    # Data Warehouse schema setup
    if event.get('data_warehouse_schema'):
        engine.data_warehouse_schema = event['data_warehouse_schema']
    else:
        engine.data_warehouse_schema = "data_warehouse"
    engine.postgres.create_schema_database()  # must have 'create' privilege

    for stock in event.get('stocks'):

        # Extract
        extracted_ticket_data = engine.extract_data_lake(stock=stock)

        if len(extracted_ticket_data) == 0:
            continue

        # Transform
        ticket_data = engine.transform_dataframe(dataframe=extracted_ticket_data)

        # Load
        print("Uploading to Data Warehouse... ", end="")
        engine.postgres.upload_data(dataframe=ticket_data, table_name=stock.get('ticket_name'))
        print("Upload complete!")


if __name__ == "__main__":
    event = {
        "data_warehouse_schema": "dw",  # there is a default value if this one is not provided
        "datalake_schema": "b3_history",
        "stocks": [
            {
                "ticket_name": "VALE3",
                "optional_old_ticket_name": "VAL 3"
            },
            {
                "ticket_name": "PETR3",
                "optional_old_ticket_name": "PET 3"
            }
        ]
    }
    lambda_function(event=event)
