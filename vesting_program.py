import argparse
from datetime import datetime
from decimal import Decimal
from typing import Dict
from process_data import process_csv

# Parse arguments from input
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Vesting Schedule CLI")
    parser.add_argument('file', help='CSV file with vesting events')
    parser.add_argument('target_date', type=lambda d: datetime.strptime(d, '%Y-%m-%d'), help='Target date (YYYY-MM-DD)')
    parser.add_argument('precision', nargs='?', default=0, type=int, help='Decimal precision for share amounts')
    return parser.parse_args()

# Display calculated vest schedule
def display_results(events: Dict[tuple[str, str, str], Decimal], precision: int) -> None:
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
