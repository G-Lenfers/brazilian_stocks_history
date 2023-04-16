"""
File for plotting Data Warehouse results and comparing its stock prices with yahoo_finance.

Although this could be done with any stock, this file will handle only
stocks referring to Petrobas. It is essential that data_warehouse app
and yahoo_finance app have already been run entirely before the execution of this one.
"""
from datetime import date
import os

import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from src.shared.loading_engine import PostgresConnector


ROOT_PATH = os.path.abspath(os.path.join(__file__, os.pardir))


def lambda_handler():
    """Orchestrate figures generation."""
    # Get Data Warehouse data
    postgres = PostgresConnector()
    dw_query = "SELECT * FROM data_warehouse.petr3"
    dw_dataframe = postgres.read_sql_query(query=dw_query, params={})

    # Get Yahoo Finance data from postgres
    yahoo_query = "SELECT * FROM yahoo_finance.petr3_sa"
    yahoo_dataframe = postgres.read_sql_query(query=yahoo_query, params={})

    build_figure_dw_all_dates(dataframe=dw_dataframe)
    build_figure_filtered_dates(dataframe=dw_dataframe)
    plot_results_comparison(dw_dataframe=dw_dataframe, yahoo_dataframe=yahoo_dataframe)


def build_figure_dw_all_dates(dataframe) -> None:
    """Plot all dates within data warehouse results."""
    x = dataframe['data_pregao']
    y = dataframe['preco_abertura_pregao']

    fig, ax = plt.subplots(1, 1, figsize=(7, 5.5))
    ax.plot(x, y, linewidth=2.0, label="Preço abertura pregão")

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    for label in ax.get_xticklabels(which='major'):
        label.set(rotation=30)

    plt.grid()
    plt.legend()
    plt.xlabel("Data do pregão")
    plt.ylabel("Preço do ativo (CR\$, CZ\$, NCZ\$ ou R\$)")
    plt.subplots_adjust(left=0.15, bottom=0.15)
    plt.tight_layout()
    # plt.show()
    plt.savefig(ROOT_PATH + '/figures/dw_all_dates.pdf')


def build_figure_filtered_dates(dataframe) -> None:
    """Plot results after July the first, 1994."""
    date_filter = dataframe['data_pregao'] >= date(1994, 7, 1)
    x = dataframe.loc[date_filter, 'data_pregao']
    y1 = dataframe.loc[date_filter, 'preco_abertura_pregao']
    y2 = dataframe.loc[date_filter, 'preco_ultimo_negocio']
    y3 = dataframe.loc[date_filter, 'preco_maximo_pregao']
    y4 = dataframe.loc[date_filter, 'preco_minimo_pregao']

    fig, ax = plt.subplots(1, 1, figsize=(7, 5.5))
    ax.plot(x, y1, linewidth=2.0, label="Preço abertura pregão")
    ax.plot(x, y2, linewidth=2.0, label="Preço encerramento pregão")
    ax.plot(x, y3, linewidth=2.0, label="Preço máximo pregão")
    ax.plot(x, y4, linewidth=2.0, label="Preço mínimo pregão")

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    for label in ax.get_xticklabels(which='major'):
        label.set(rotation=30)

    plt.grid()
    plt.legend()
    plt.xlabel("Data do pregão")
    plt.ylabel("Preço do ativo (R\$)")
    plt.subplots_adjust(left=0.15, bottom=0.15)
    plt.tight_layout()
    # plt.show()
    plt.savefig(ROOT_PATH + '/figures/filtered_dates.pdf')


def plot_results_comparison(dw_dataframe, yahoo_dataframe):
    """Compare opening prices of PETR3 between data provided by B3 and fetched from Yahoo Finance."""
    # Yahoo brings values only after 2000-01-01
    date_filter = dw_dataframe['data_pregao'] >= date(2000, 1, 1)

    # Data Warehouse results
    x1 = dw_dataframe.loc[date_filter, 'data_pregao']
    y1 = dw_dataframe.loc[date_filter, 'preco_abertura_pregao']

    # Yahoo Finance data
    x2 = yahoo_dataframe['date']
    y2 = yahoo_dataframe['open']

    fig, ax = plt.subplots(1, 1, figsize=(7, 5.5))
    ax.plot(x1, y1, linewidth=2.0, label="Preço abertura B3")
    ax.plot(x2, y2, linewidth=2.0, label="Preço abertura Yahoo Finance")

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b-%Y'))
    for label in ax.get_xticklabels(which='major'):
        label.set(rotation=30)

    plt.grid()
    plt.legend()
    plt.xlabel("Data do pregão")
    plt.ylabel("Preço do ativo (R\$)")
    plt.subplots_adjust(left=0.15, bottom=0.15)
    plt.tight_layout()
    # plt.show()
    plt.savefig(ROOT_PATH + '/figures/comparison.pdf')


if __name__ == "__main__":
    lambda_handler()
