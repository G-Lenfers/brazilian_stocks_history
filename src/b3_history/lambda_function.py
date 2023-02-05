"""File for orchestrating the extraction, transform and upload data from B3 to postgres data lake or data warehouse."""
from src.b3_history.modules.data_lake import B3HistoryExtractorEngine


def lambda_handler(event: any):
    for file in event.get('files_to_run'):
        engine = B3HistoryExtractorEngine(file_name=file)
    pass


if __name__ == "__main__":
    event = {
        "files_to_run": [
            "COTAHIST_A1986.zip"
        ]
    }
    lambda_handler(event=event)
