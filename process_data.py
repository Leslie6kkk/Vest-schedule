import csv
from datetime import datetime
import logging
from decimal import Decimal
from typing import Dict, Tuple, List
from collections import defaultdict

from validation import FIELD_NAMES, validate_row, validate_csv_file

BATCH_SIZE = 100

def process_batch(batch: List[Tuple[int, Dict[str, str]]], target_date: datetime, precision: int, batch_index: int) -> Tuple[Dict[Tuple[str, str, str], Decimal], int, List[str]]:
    """
    Processes a batch of vesting events from a CSV file.

    This function validates and processes each row in the given batch. For valid rows, it aggregates
    the vesting or cancellation events based on the target date and updates the event quantities.
    For invalid rows, it logs error messages for later review.

    Args:
        batch (List[Tuple[int, Dict[str, str]]]): A batch of CSV rows, where each row is represented by 
            a tuple containing the row index and a dictionary of row data.
        target_date (datetime): The cutoff date; only events that occur on or before this date are considered.
        precision (int): The decimal precision to apply to share amounts.
        batch_index (int): The index of the current batch being processed.

    Returns:
        Tuple[Dict[Tuple[str, str, str], Decimal], int, List[str]]:
            - A dictionary of events where the key is a tuple (emp_id, emp_name, award_id) and the value is the aggregated quantity (Decimal).
            - The count of errors encountered in this batch (int).
            - A list of error messages encountered during processing (List[str]).

    Raises:
        ValueError: If row validation fails, specific error messages will be returned in error messages.
    """
    events = defaultdict(Decimal)
    error_count = 0
    error_messages = []
    for index, row in batch:

        validated_row = validate_row(row, batch_index * BATCH_SIZE + index, precision)

        if isinstance(validated_row,str):
            error_count += 1
            error_messages.append(validated_row)
            continue

        event_type, emp_id, emp_name, award_id, event_date, quantity = validated_row.values()

        key = (emp_id, emp_name, award_id)
        if event_date <= target_date:
            if event_type == 'VEST':
                events[key] += quantity
            elif event_type == 'CANCEL':
                events[key] -= quantity
        else:
            if key not in events:
                events[key] = Decimal(0)

    return dict(events), error_count, error_messages


def process_csv(file: str, target_date: datetime, precision: int) -> Dict[Tuple[str, str, str], Decimal]:
    """
    Processes a CSV file and aggregates vesting events based on the target date.

    This function reads a CSV file containing vesting event data, processes it in batches, and aggregates
    the vesting and cancellation quantities by employee and award. The aggregation considers only events
    on or before the target date. Invalid rows are logged, and errors are counted and reported.

    Args:
        file (str): Path to the CSV file to be processed.
        target_date (datetime): The cutoff date; only events on or before this date are considered.
        precision (int): The decimal precision to apply to share amounts.

    Returns:
        Dict[Tuple[str, str, str], Decimal]: A dictionary where the key is a tuple (emp_id, emp_name, award_id)
            and the value is the aggregated quantity (Decimal).

    Logs:
        Warnings are logged for any invalid rows, including error messages and line numbers.
    """
    events = defaultdict(Decimal)
    error_count = 0
    error_messages = []

    validate_csv_file(file)

    with open(file, 'r') as csvfile:
        reader = csv.DictReader(csvfile, fieldnames=FIELD_NAMES)
        batch = []
        batch_index = 0
        for index, row in enumerate(reader):
            batch.append((index, row))

            if len(batch) == BATCH_SIZE:
                batch_events, error_count_batch, error_messages_batch = process_batch(batch, target_date, precision, batch_index)

                for key, quantity in batch_events.items():
                    events[key] += quantity
                error_count += error_count_batch
                error_messages += error_messages_batch
                batch_index += 1
                batch = []

        if batch:
            batch_events, error_count_batch, error_messages_batch = process_batch(batch, target_date, precision, batch_index)

            for key, quantity in batch_events.items():
                events[key] += quantity
            error_count += error_count_batch
            error_messages += error_messages_batch

    if error_count > 0:
        logging.warning(f"Total Lines Skipped: {error_count}. Please check the following messages.")
        for message in error_messages:
            logging.warning(message)

    return dict(events)
