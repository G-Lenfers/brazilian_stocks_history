# Brazilian historical stock market database

This project was created as a Final Paper of MBA in Data Science and Analytics at University of São Paulo (USP). It builds a database containing brasilian market historical stock prices.

## Pre-requisites

To run this project's applications, one must have the following environments configured:
- Python 3.10;
- Installed the packages listed in requirements.txt. One can achieve this by running the following command at terminal's root directory: `pip install -r requirements.txt`;
- PostgreSQL 15.1-1;
- A database user with CREATE permissions. It's credentials must be put into environment variables that match the following keys:
  - SQL_HOST
  - SQL_PORT
  - SQL_USER
  - SQL_PASS
  - SQL_DB
