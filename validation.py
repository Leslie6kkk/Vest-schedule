import os
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from typing import Dict, Union


FIELD_NAMES = ['event_type', 'emp_id', 'emp_name', 'award_id', 'date', 'quantity']

def validate_row(row: Dict[str, str], index: int, precision: int) -> Union[Dict[str, object], str]:
    """
    Validates a row from the CSV file and returns a parsed dictionary if valid or an error message if invalid.

    The function checks for the presence of required fields, validates the event type, date format, and
    ensures that the quantity is a valid, non-negative decimal. It rounds the quantity to the specified precision.
    If any validation fails, it returns an appropriate error message.

    Args:
        row (Dict[str, str]): A dictionary representing a row from the CSV file, with keys corresponding to field names.
        index (int): The index of the row being validated (used for error reporting).
        precision (int): The decimal precision for rounding share quantities.

    Returns:
        Union[Dict[str, object], str]:
            - A dictionary of parsed and validated row data if the row is valid.
            - An error message (str) describing the validation issue if the row is invalid.

    Raises:
        None: Errors are returned as strings for invalid rows.
    """
    for key in FIELD_NAMES:
        if row[key] is None or row[key].strip() == '':
            return f"Row index {index}: Missing field in {row.values()}: "

    if row['event_type'] not in ('VEST', 'CANCEL'):
        return f"Row index {index}: Invalid event type '{row['event_type']}' in {row.values()}"

    try:
        event_date = datetime.strptime(row['date'], '%Y-%m-%d')
    except ValueError:
        return f"Row index {index}: Invalid date format in {row.values()}"

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


def validate_csv_file(file: str) -> None:
    """
    Validates the existence and format of the CSV file.

    This function checks if the given file exists on the filesystem and ensures it has a '.csv' extension.
    If the file does not exist or is not a CSV file, appropriate exceptions are raised.

    Args:
        file (str): The path to the file being validated.

    Returns:
        None: Raises exceptions on validation failure.

    Raises:
        ValueError: If the file does not have a '.csv' extension.
        FileNotFoundError: If the file does not exist.
    """
    if not file.endswith('.csv'):
        raise ValueError(f"File '{file}' is not a valid CSV file.")

    if not os.path.isfile(file):
        raise FileNotFoundError(f"File '{file}' not found.")


