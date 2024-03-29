"""File for getting brazilian stocks through yfinance package and uploading to postgresql."""
import pandas as pd
import yfinance as yf

from src.shared.loading_engine import PostgresConnector


def lambda_handler(event: any) -> None:
    """Handle the event and call the appropriate methods."""
    # Instance postgres connection and create schema
    postgres = PostgresConnector(schema='yahoo_finance')
    postgres.create_schema_database()

    # Cycle through received stocks
    stocks = event.get("stocks")
    for stock in stocks:

        # Extract
        extracted_history = _download_ticker_information(stock=stock)

        # Transform
        history_dataframe = _transform_dataframe(dataframe=extracted_history)

        # Load
        _load_data_into_postgres(postgres=postgres, dataframe=history_dataframe, stock=stock)

    postgres.close_connections()


def _download_ticker_information(stock: str) -> pd.DataFrame:
    """Get ticker information from yahoo API."""
    return yf.download(tickers=stock, period='max', ignore_tz=True)


def _transform_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Reorder, rename, and round columns."""
    # Date index to column
    dataframe.reset_index(inplace=True)

    # Select specific columns and order them
    selected_columns = ['Date', 'Open', 'Close', 'High', 'Low', 'Volume']
    dataframe = dataframe[selected_columns]

    # Rename columns (postgres treat uppercase as a special character)
    dataframe = dataframe.rename(str.lower, axis='columns')

    # Round values
    columns_to_round = ['open', 'close', 'high', 'low']
    dataframe[columns_to_round] = dataframe[columns_to_round].round(4)

    # Convert pd.Timestamp to datetime.date
    dataframe['date'] = dataframe['date'].apply(lambda x: x.date())

    return dataframe


def _load_data_into_postgres(postgres: PostgresConnector, dataframe: pd.DataFrame, stock: str) -> None:
    """Load dataframe to postgresql."""
    postgres.upload_data(
        dataframe=dataframe,
        table_name=_format_ticker_name(stock=stock)
    )


def _format_ticker_name(stock: str) -> str:
    """Lowercase the ticker name, and replace its dot to underline."""
    return stock.lower().replace('.', '_')


if __name__ == "__main__":
    event = {
        "stocks": [
            "PETR3.SA",
            "VALE3.SA"
        ]
    }
    lambda_handler(event=event)
