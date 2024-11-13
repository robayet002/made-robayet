import pandas as pd
import numpy as np
import os
import sqlite3
import ssl
from urllib.request import urlretrieve

def load_datasets():
    """
    Load datasets from given URLs and return as DataFrames.
    """
    url1 = "https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD"
    url2 = "https://data.lacity.org/api/views/amvf-fr72/rows.csv?accessType=DOWNLOAD"
    
    crime_data = pd.read_csv(url1, delimiter=';')
    arrest_data = pd.read_csv(url2, delimiter=';')
    
    return crime, arrest

def preprocess_crime(crime_data):
    """
    Preprocess the gs DataFrame: Rename columns, group by year and select first entry for each year.
    """
    crime_data.dropna(subset=['DATE OCC', 'Crm Cd Desc'], inplace=True)  # Drop rows where critical information is missing
    crime_data['DATE OCC'] = pd.to_datetime(crime_data['DATE OCC']) # Convert date fields to datetime objects and time fields to appropriate format
    crime_data['Year'] = crime_data['DATE OCC'].dt.year # extract year, month, day for easier analysis
    crime_cols = ['DATE OCC', 'AREA NAME', 'Crm Cd Desc', 'Vict Age', 'Vict Sex', 'Vict Descent'] # Select only the necessary columns for analysis
    filtered_crime_data = crime_data[crime_cols]
    return filtered_crime_data