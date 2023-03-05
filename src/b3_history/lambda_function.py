"""File for orchestrating the ETL process of B3 stock history."""
from src.b3_history.modules.main_engine import MainEngine


def lambda_handler(event: any) -> None:
    """Orchestrate the workflow."""
    # Instance main engine
    engine = MainEngine()

    # Loop through list of files
    for file in event.get('files_to_run'):

        # Set properties accordingly
        engine.file_name = file
        engine.total_lines = engine.get_file_total_lines()

        # After completion of a certain file, the next iteration should reset has_more parameter
        engine.has_more = True

        # Loop through lines of file
        while engine.has_more:

            # Get metadata for extraction
            engine.get_last_line_read_from_postgres()

            # Check if file has already been read (remember that python considers first line as zero)
            if (engine.last_line_read + 1) == engine.total_lines:
                engine.has_more = False
                continue

            # Execute extract, transform, and load processes
            engine.run_etl()

    pass


if __name__ == "__main__":
    event = {
        "files_to_run": [
            "COTAHIST_A1986.zip",
            "COTAHIST_A1987.zip"
        ]
    }
    lambda_handler(event=event)
