"""Filter specific ticket data and upload it to data warehouse."""


def lambda_function(event: list) -> None:
    """Orchestrate accordingly."""


if __name__ == "__main__":
    event = [
        {
            "ticket_name": "VALE3",
            "optional_old_ticket_name": "VAL 3"
        },
        {
            "ticket_name": "PETR3",
            "optional_old_ticket_name": "PET 3"
        }
    ]
    lambda_function(event=event)
