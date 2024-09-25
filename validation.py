import os
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Dict, Union


FIELD_NAMES = ['event_type', 'emp_id', 'emp_name', 'award_id', 'date', 'quantity']

# Validate each row from csv file and return parsed object(on valid rows) or warning message(on invalid rows)
def validate_row(row: Dict[str, str], index: int, precision: int) -> Union[Dict[str, object], str]:
    # Ensure each field has non-empty values
    for key in FIELD_NAMES:
        if row[key] is None or row[key].strip() == '':
            return f"Row index {index}: Missing field in {row.values()}: "

    # Validate event type
    if row['event_type'] not in ('VEST', 'CANCEL'):
        return f"Row index {index}: Invalid event type '{row['event_type']}' in {row.values()}"

    # Validate and parse date
    try:
        event_date = datetime.strptime(row['date'], '%Y-%m-%d')
    except ValueError:
        return f"Row index {index}: Invalid date format in {row.values()}"

    # Validate and round quantity
    try:
        rounding_format = Decimal(f'1.{"0" * precision}')
        quantity = Decimal(row['quantity']).quantize(rounding_format, rounding=ROUND_DOWN)
        if quantity < 0:
            return f"Row index {index}: Negative quantity in {row.values()}"
    except (ValueError, InvalidOperation):
        return f"Row index {index}: Invalid quantity in {row.values()}"

    return {
        'event_type': row['event_type'],
        'emp_id': row['emp_id'],
        'emp_name': row['emp_name'],
        'award_id': row['award_id'],
        'event_date': event_date,
        'quantity': quantity
    }

# Validate the file exists and in csv format
def validate_csv_file(file: str) -> None:
    if not file.endswith('.csv'):
        raise ValueError(f"File '{file}' is not a valid CSV file.")

    if not os.path.isfile(file):
        raise FileNotFoundError(f"File '{file}' not found.")


