"""File for getting brazilian stocks through yfinance package and uploading to postgresql."""
import os

import yfinance as yf
from sqlalchemy import create_engine


def lambda_handler(event: any) -> None:
    """Handle the event and call the appropriate methods."""
    stocks = event.get("stocks")
    for stock in stocks:

        # Extract
        extracted_history = _download_ticker_information(stock=stock)

        # Transform
        history_dataframe = _transform_dataframe(dataframe=extracted_history)

        # Load
        _load_data_into_postgres(dataframe=history_dataframe, stock=stock)


def _download_ticker_information(stock):
    """Get ticker information from yahoo API."""
    return yf.download(tickers=stock, period='max', ignore_tz=True)


def _transform_dataframe(dataframe):
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

    return dataframe


def _load_data_into_postgres(dataframe, stock):
    """Load dataframe to postgresql."""
    postgres_engine = _connect_to_database()
    dataframe.to_sql(
        name=stock.lower().replace('.', '_'),
        con=postgres_engine,
        schema='yahoo_finance',
        if_exists='append',
        index=False,
        chunksize=1000
    )


def _connect_to_database():
    """Connect to Postgres server."""
    # Engine connection parameters
    dialect = 'postgresql'
    driver = 'psycopg2'

    # Database credentials
    host = os.environ['SQL_HOST']
    port = os.environ['SQL_PORT']
    user = os.environ['SQL_USER']
    password = os.environ['SQL_PASS']
    database = os.environ['SQL_DB']

    engine = create_engine(f"{dialect}+{driver}://{user}:{password}@{host}:{port}/{database}")

    return engine


if __name__ == "__main__":
    event = {
        "stocks": [
            "PETR4.SA",
            "VALE3.SA"
        ]
    }
    lambda_handler(event=event)
