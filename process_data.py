import csv
from datetime import datetime
import logging
from decimal import Decimal
from typing import Dict, Tuple, List
from collections import defaultdict
from validation import FIELD_NAMES, validate_row, validate_csv_file

BATCH_SIZE = 100

# Process data for each batch
def process_batch(batch: List[Tuple[int, Dict[str, str]]], target_date: datetime, precision: int, batch_index: int) -> Tuple[Dict[Tuple[str, str, str], Decimal], int, List[str]]:
    events = defaultdict(Decimal)
    error_count = 0
    error_messages = []
    for index, row in batch:
        # validate and parse row data
        validated_row = validate_row(row, batch_index * BATCH_SIZE + index, precision)

        # row data is not valid: add warning messages to error_messsages for later log
        if isinstance(validated_row,str):
            error_count += 1
            error_messages.append(validated_row)
            continue

        # row data is valid for processing
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

# Process CSV file into aggregated vesting events
def process_csv(file: str, target_date: datetime, precision: int) -> Dict[Tuple[str, str, str], Decimal]:
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

        # Process any remaining rows in the last batch
        if batch:
            batch_events, error_count_batch, error_messages_batch = process_batch(batch, target_date, precision, batch_index)

            for key, quantity in batch_events.items():
                events[key] += quantity
            error_count += error_count_batch
            error_messages += error_messages_batch

    # log warning about all invalid rows with total count, reason and index
    if error_count > 0:
        logging.warning(f"Total Lines Skipped: {error_count}. Please check the following messages.")
        for message in error_messages:
            logging.warning(message)

    return dict(events)
