# Vesting CLI Application

## Description

This is a simple command-line application for processing vesting events from a CSV file and calculating a cumulative vesting schedule.
The program receives two required argument `filename` and `target date`, and one optional argument `precision`.
The output vesting schedule would be printed to stdout.

## Features

- **Input Validation**: Checks for valid CSV format and row data integrity.
- **Error Handling**: Skips invalid rows while logging detailed information for debugging.
- **Batch Processing**: Processes rows in batches of 100 for improved performance.

## Design Decisions

1. **Abnormal Case Handling**:
   - If the provided file argument is not valid, the program will terminate with an error message.
   - Invalid rows are skipped, and the program continues. The total number of skipped rows and the reason for skipping each row are logged for later debugging.

2. **Batch Processing**:
   - Rows in the CSV file are processed in batches of 100 to maximize performance on large datasets.
   - The structure is designed for future enhancements, such as potential parallel processing of different batches.

## Future Improvements

1. **Complete Test Cases**:
   - Implement unit tests for core functions like `process_csv`, `validate_row` with various inputs(including abnormal ones)
   - Create e2e testing with Pytest to ensure the whole program works as expected.
2. **Implement Parallel Processing**:
    Parallel processing would be beneficial to handle large datasets with significantly shorter processing time.

## Requirements
- Python 3.x
- No external libraries are required at this time.

## Usage

To run the application, use the following command:

```bash
python vesting_program.py <file> <target_date> [precision]
