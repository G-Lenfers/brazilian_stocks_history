"""File for getting brazilian stocks through yfinance package."""
import yfinance as yf


def lambda_handler(event: any) -> None:
    """Main function."""
    stocks = event.get("stocks")

    for stock in stocks:

        ticker = yf.Ticker(stock)
        # The oldest value is the first day of 2000
        print(ticker.history(period="max"))


if __name__ == "__main__":
    event = {
        "stocks": [
            "PETR4.SA",
            "VALE3.SA"
        ]
    }
    lambda_handler(event=event)
