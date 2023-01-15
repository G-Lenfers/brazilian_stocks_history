"""File for getting brazilian stocks through yfinance package."""
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine


def lambda_handler(event: any) -> None:
    """Main function."""
    selected_columns = ['Date', 'Open', 'Close', 'High', 'Low', 'Volume']

    stocks = event.get("stocks")
    for stock in stocks:

        # Extract
        extracted_history = _download_ticker_information(stock=stock)
        history_dataframe.reset_index(inplace=True)
        history_dataframe = history_dataframe[selected_columns]


def _download_ticker_information(stock):
    """Get ticker information from yahoo API."""
    return yf.download(tickers=stock, period='max', ignore_tz=True)


if __name__ == "__main__":
    event = {
        "stocks": [
            "PETR4.SA",
            "VALE3.SA"
        ]
    }
    lambda_handler(event=event)
