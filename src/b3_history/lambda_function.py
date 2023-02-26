"""File for orchestrating the extraction, transform and upload data from B3 to postgres data lake or data warehouse."""
from src.b3_history.modules.data_lake import B3HistoryExtractorEngine


def lambda_handler(event: any) -> None:
    """Orchestrate the workflow."""
    # Instance main engine
    engine = B3HistoryExtractorEngine()

    # Loop through list of files
    for file in event.get('files_to_run'):

        # Set properties accordingly
        engine.file_name = file
        engine.total_lines = engine.get_file_total_lines()

        print('end property')

        # Execute extract, transform, and load processes
        engine.run_etl()

    pass


if __name__ == "__main__":
    event = {
        "files_to_run": [
            "COTAHIST_A2006.zip"
        ]
    }
    lambda_handler(event=event)
