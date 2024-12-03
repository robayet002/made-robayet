#!/bin/bash

# Define the path to the Python script
PYTHON_SCRIPT_PATH="path_to_your_python_script.py"

# Execute the data pipeline script
echo "Running the data pipeline script..."
python $PYTHON_SCRIPT_PATH

# Check if the SQLite database file exists
DB_FILE="merged_dataset.db"
if [ -f "$DB_FILE" ]; then
    echo "$DB_FILE exists."
else
    echo "Error: $DB_FILE does not exist."
    exit 1
fi

# Check if the CSV file exists
CSV_FILE="merged_dataset.csv"
if [ -f "$CSV_FILE" ]; then
    echo "$CSV_FILE exists."
else
    echo "Error: $CSV_FILE does not exist."
    exit 1
fi

echo "All tests passed successfully!"
