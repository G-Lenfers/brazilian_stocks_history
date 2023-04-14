"""
File for plotting Data Warehouse results and comparing its stock prices with yahoo_finance.

Although this could be done with any stock, this file will handle only
stocks referring to Petrobas. It is essential that data_warehouse app
and yahoo_finance app have already been run entirely before the execution of this one.
"""
from datetime import date

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from src.shared.loading_engine import PostgresConnector

# TODO label size
# TODO filtered plot, with all 4 variables
# TODO plot comparison

# # y_query = "SELECT * FROM yahoo_finance.petr4_sa"
# # y_dataframe = postgres.read_sql_query(query=y_query, params={})


def lambda_handler():
    """Orchestrate figures generation."""
    # Get Data Warehouse data
    postgres = PostgresConnector()
    dw_query = "SELECT * FROM data_warehouse.petr3"
    dw_dataframe = postgres.read_sql_query(query=dw_query, params={})

    build_figure_dw_all_dates(dataframe=dw_dataframe)
    build_figure_filtered_dates(dataframe=dw_dataframe)


def build_figure_dw_all_dates(dataframe) -> None:
    """Plot all dates within data warehouse results."""
    x = dataframe['data_pregao']
    y = dataframe['preco_abertura_pregao']

    fig, ax = plt.subplots(1, 1, figsize=(6, 7))
    ax.plot(x, y, linewidth=2.0, label="Preço abertura pregão")

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    for label in ax.get_xticklabels(which='major'):
        label.set(rotation=30)

    plt.grid()
    plt.legend()
    plt.xlabel("Data do pregão")
    plt.ylabel("Preço do ativo (CR\$, CZ\$, NCZ\$ ou R\$)")
    plt.subplots_adjust(left=0.15, bottom=0.15)
    plt.show()
    # plt.savefig('figures/dw_all_dates.pdf')


def build_figure_filtered_dates(dataframe) -> None:
    """"""
    date_filter = dataframe['data_pregao'] >= date(1994, 1, 1)
    pass


if __name__ == "__main__":
    lambda_handler()
