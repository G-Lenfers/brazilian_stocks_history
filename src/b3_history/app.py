"""File for orchestrating the ETL process of B3 stock history."""
from psycopg2.errors import InsufficientPrivilege

from src.b3_history.modules.main_engine import MainEngine


def lambda_handler(event: any) -> None:
    """Orchestrate the workflow."""
    # Instance main engine
    engine = MainEngine()

    # Schema setup
    try:
        engine.create_schema_if_not_exists()

    except InsufficientPrivilege:
        # TODO even if transaction fails, it could continue on if schema already exists
        print("Insufficient privileges to execute create schema statement.\n"
              "Please, configure a database user with CREATE permission.\n"
              "Unable to continue works, stopping...")
        return

    # Set properties according to received event
    if event.get('batch_size'):
        engine.batch_size = event['batch_size']

    if event.get('schema'):
        engine.schema = event['schema']

    # Loop through list of files
    for file in event.get('files_to_run'):

        # Set properties accordingly
        engine.file_name = file
        engine.total_lines = engine.get_file_total_lines()

        # After completion of a certain file, the next iteration should reset has_more parameter
        engine.has_more = True

        # Loop through batches of file's lines
        while engine.has_more:

            # Get metadata for extraction
            engine.get_last_line_read_from_postgres()

            # Check if file has already been read (remember that python considers first line as zero)
            if (engine.last_line_read + 1) == engine.total_lines:
                engine.has_more = False
                continue

            # Execute extract, transform, and load processes
            engine.run_etl()

    engine.create_update_view()
    print("All done!")


if __name__ == "__main__":
    event = {
        'batch_size': 5000,
        'files_to_run': [
            "COTAHIST_A1986.zip",
            "COTAHIST_A1987.zip",
            "COTAHIST_A1988.zip",
            "COTAHIST_A1989.zip",
            "COTAHIST_A1990.zip",
            "COTAHIST_A1991.zip",
            "COTAHIST_A1992.zip",
            "COTAHIST_A1993.zip",
            "COTAHIST_A1994.zip",
            "COTAHIST_A1995.zip",
            "COTAHIST_A1996.zip",
            "COTAHIST_A1997.zip",
            "COTAHIST_A1998.zip",
            "COTAHIST_A1999.zip",
            "COTAHIST_A2000.zip",
            "COTAHIST_A2001.zip",
            "COTAHIST_A2002.zip",
            "COTAHIST_A2003.zip",
            "COTAHIST_A2004.zip",
            "COTAHIST_A2005.zip",
            "COTAHIST_A2006.zip",
            "COTAHIST_A2007.zip",
            "COTAHIST_A2008.zip",
            "COTAHIST_A2009.zip",
            "COTAHIST_A2010.zip",
            "COTAHIST_A2011.zip",
            "COTAHIST_A2012.zip",
            "COTAHIST_A2013.zip",
            "COTAHIST_A2014.zip",
            "COTAHIST_A2015.zip",
            "COTAHIST_A2016.zip",
            "COTAHIST_A2017.zip",
            "COTAHIST_A2018.zip",
            "COTAHIST_A2019.zip",
            "COTAHIST_A2020.zip",
            "COTAHIST_A2021.zip",
            "COTAHIST_A2022.zip"
        ]
    }
    lambda_handler(event=event)
