import unittest
import pandas as pd
import sqlite3
import os
from pipeline import load_datasets, preprocess_crime, preprocess_arrest, merge_dataframes, main

class TestDataProcessing(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Before running any tests, clean up previous test data to avoid conflicts
        if os.path.exists('merged_dataset.db'):
            os.remove('merged_dataset.db')
        if os.path.exists('merged_dataset.csv'):
            os.remove('merged_dataset.csv')
        # Run the main function to execute the data pipeline
        main()

    def test_file_creation(self):
        # Check that the database and CSV files are created
        self.assertTrue(os.path.exists('merged_dataset.db'), "Database file was not created.")
        self.assertTrue(os.path.exists('merged_dataset.csv'), "CSV file was not created.")

    def test_data_loading(self):
        # Test the initial data loading from external sources
        crime_data, arrest_data = load_datasets()
        self.assertIsNotNone(crime_data, "Failed to load crime data")
        self.assertIsNotNone(arrest_data, "Failed to load arrest data")

    def test_preprocess_crime(self):
        # Test the crime data preprocessing
        crime_data, _ = load_datasets()
        if crime_data is not None:
            result = preprocess_crime(crime_data)
            self.assertIsNotNone(result, "Preprocessing crime data failed")
            self.assertFalse(result.empty, "Processed crime data is empty")

    def test_preprocess_arrest(self):
        # Test the arrest data preprocessing
        _, arrest_data = load_datasets()
        if arrest_data is not None:
            result = preprocess_arrest(arrest_data)
            self.assertIsNotNone(result, "Preprocessing arrest data failed")
            self.assertFalse(result.empty, "Processed arrest data is empty")

    def test_data_merging(self):
        # Test merging of crime and arrest data
        crime_data, arrest_data = load_datasets()
        if crime_data is not None and arrest_data is not None:
            processed_crime_data = preprocess_crime(crime_data)
            processed_arrest_data = preprocess_arrest(arrest_data)
            merged_df = merge_dataframes(processed_crime_data, processed_arrest_data)
            self.assertIsNotNone(merged_df, "Merging dataframes failed")
            self.assertFalse(merged_df.empty, "Merged dataframe is empty")

    def test_database_contents(self):
        # Verify that data has been inserted into the database
        conn = sqlite3.connect('merged_dataset.db')
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM merged_data")
        count = cursor.fetchone()[0]
        conn.close()
        self.assertGreater(count, 0, "No data inserted into the database")

    def test_csv_contents(self):
        # Verify that the CSV file contains data
        df = pd.read_csv('merged_dataset.csv')
        self.assertFalse(df.empty, "CSV file is empty")
        self.assertGreater(len(df), 0, "CSV file contains no data")


if __name__ == '__main__':
    unittest.main()
