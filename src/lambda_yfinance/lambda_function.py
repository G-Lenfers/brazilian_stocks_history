"""File for getting brazilian stocks through yfinance package."""
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine


def lambda_handler(event: any) -> None:
    """Main function."""
    stocks = event.get("stocks")
    for stock in stocks:

        # Extract
        extracted_history = _download_ticker_information(stock=stock)

        # Transform
        history_dataframe = _transform_dataframe(dataframe=extracted_history)


def _download_ticker_information(stock):
    """Get ticker information from yahoo API."""
    return yf.download(tickers=stock, period='max', ignore_tz=True)


def _transform_dataframe(dataframe):
    """Insert docstring here."""
    # Date index to column
    dataframe.reset_index(inplace=True)

    # Order columns
    selected_columns = ['Date', 'Open', 'Close', 'High', 'Low', 'Volume']
    dataframe = dataframe[selected_columns]

    # Rename columns (postgres treat uppercase as a special character)
    dataframe.rename(str.lower, axis='columns', inplace=True)

    return dataframe

if __name__ == "__main__":
    event = {
        "stocks": [
            "PETR4.SA",
            "VALE3.SA"
        ]
    }
    lambda_handler(event=event)
