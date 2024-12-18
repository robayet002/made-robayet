import unittest
import pandas as pd
import sqlite3
import os
from pipeline import load_datasets, preprocess_crime, preprocess_arrest, merge_dataframes, categorize_crimes, main

class TestDataProcessing(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Ensure a clean start by removing any existing data files
        if os.path.exists('total_dataset.db'):
            os.remove('total_dataset.db')
        if os.path.exists('merged_dataset.csv'):
            os.remove('merged_dataset.csv')
        # Prepopulate the database and CSV by running the main pipeline
        main()

    def test_file_creation(self):
        # Verify that both the database and CSV files are created
        self.assertTrue(os.path.exists('total_dataset.db'), "Database file was not created.")
        self.assertTrue(os.path.exists('merged_dataset.csv'), "CSV file was not created.")

    def test_data_loading(self):
        # Validate that data loading from URLs is successful
        crime_data, arrest_data = load_datasets()
        self.assertIsNotNone(crime_data, "Failed to load crime data")
        self.assertIsNotNone(arrest_data, "Failed to load arrest data")

    def test_preprocess_crime(self):
        # Verify that crime data preprocessing correctly filters and processes data
        crime_data, _ = load_datasets()
        if crime_data is not None:
            categorized_crime_data = categorize_crimes(crime_data)
            processed_crime_data = preprocess_crime(categorized_crime_data)
            self.assertIsNotNone(processed_crime_data, "Preprocessing of crime data failed")
            self.assertFalse(processed_crime_data.empty, "Processed crime data is empty")

    def test_preprocess_arrest(self):
        # Check that arrest data preprocessing operates as expected
        _, arrest_data = load_datasets()
        if arrest_data is not None:
            processed_arrest_data = preprocess_arrest(arrest_data)
            self.assertIsNotNone(processed_arrest_data, "Preprocessing of arrest data failed")
            self.assertFalse(processed_arrest_data.empty, "Processed arrest data is empty")

    def test_data_merging(self):
        # Ensure the merging process of crime and arrest data frames is functioning
        crime_data, arrest_data = load_datasets()
        categorized_crime_data = categorize_crimes(crime_data)
        processed_crime_data = preprocess_crime(categorized_crime_data)
        processed_arrest_data = preprocess_arrest(arrest_data)
        merged_df = merge_dataframes(processed_crime_data, processed_arrest_data)
        self.assertIsNotNone(merged_df, "Merging data frames failed")
        self.assertFalse(merged_df.empty, "Merged data frame is empty")

    def test_database_contents(self):
        # Validate that data is correctly stored in the database
        conn = sqlite3.connect('total_dataset.db')
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM crime_data")
        crime_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM arrest_data")
        arrest_count = cursor.fetchone()[0]
        conn.close()
        self.assertGreater(crime_count, 0, "Crime data table in database is empty")
        self.assertGreater(arrest_count, 0, "Arrest data table in database is empty")

    def test_csv_contents(self):
        # Confirm that the merged CSV file contains data
        df = pd.read_csv('merged_dataset.csv')
        self.assertFalse(df.empty, "CSV file is empty")
        self.assertGreater(len(df), 0, "CSV file contains no data")

if __name__ == '__main__':
    unittest.main()
