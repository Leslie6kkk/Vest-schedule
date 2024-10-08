import argparse
from datetime import datetime
from decimal import Decimal
from typing import Dict

from process_data import process_csv


def parse_args() -> argparse.Namespace:
    """
    Parses command-line arguments for the Vesting Schedule CLI.

    Args:
      1. 'file' (str): Path to a CSV file containing vesting events.
      2. 'target_date' (str): A target date in 'YYYY-MM-DD' format, which is converted to a datetime object.
      3. 'precision' (int, optional): The decimal precision for share amounts. Defaults to 0 if not provided.

    Returns:
        argparse.Namespace: A namespace object containing parsed argument values.

    Raises:
        ValueError: If 'target_date' is not provided in the correct 'YYYY-MM-DD' format.
    """

    parser = argparse.ArgumentParser(description="Vesting Schedule CLI")
    parser.add_argument('file', help='CSV file with vesting events')
    parser.add_argument('target_date', type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help='Target date (YYYY-MM-DD)')
    parser.add_argument('precision', nargs='?', default=0, type=int, help='Decimal precision for share amounts')
    return parser.parse_args()


def display_results(events: Dict[tuple[str, str, str], Decimal], precision: int) -> None:
    """
    This functions displays the calculated vesting schedule to stdout with given format.

    Args:
      1. 'events': A dictionary containing data for each output line. (emp_id, emp_name, award_id): count of shares
      2. 'precision': Required precision to be used when printing to stdout.

    Returns:
        None. Print result to stdout.
    """
    for key in sorted(events.keys()):
        emp_id, emp_name, award_id = key
        vested_shares = f"{events[key]:.{precision}f}"
        print(f'{emp_id},{emp_name},{award_id},{vested_shares}')

def main():
    args = parse_args()
    events = process_csv(args.file, args.target_date, args.precision)
    display_results(events, args.precision)

if __name__ == '__main__':
    main()
