import pandas as pd
import sqlite3
import os

def load_datasets():
    url1 = "https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD"
    url2 = "https://data.lacity.org/api/views/amvf-fr72/rows.csv?accessType=DOWNLOAD"
    crime_data = pd.read_csv(url1)
    arrest_data = pd.read_csv(url2)
    return crime_data, arrest_data

def preprocess_crime(crime_data):
    crime_data.dropna(subset=['DATE OCC', 'Crm Cd Desc'], inplace=True)
    crime_data['DATE OCC'] = pd.to_datetime(crime_data['DATE OCC'])
    crime_data['Year'] = crime_data['DATE OCC'].dt.year
    crime_cols = ['DATE OCC', 'AREA NAME', 'Crm Cd Desc', 'Vict Age', 'Vict Sex', 'Vict Descent']
    filtered_crime_data = crime_data[crime_cols]
    return filtered_crime_data

def preprocess_arrest(arrest_data):
    arrest_data['Arrest Date'] = pd.to_datetime(arrest_data['Arrest Date'])
    arrest_data['Year'] = arrest_data['Arrest Date'].dt.year
    arrest_data.rename(columns={'Area Name': 'Arrest Area Name'}, inplace=True)
    arrest_cols = ['Arrest Area Name', 'Arrest Date', 'Charge Group Description', 'Age', 'Sex Code', 'Descent Code']
    filtered_arrest_data = arrest_data[arrest_cols]
    return filtered_arrest_data

def merge_dataframes(filtered_crime_data, filtered_arrest_data):
    merged_df = pd.merge(filtered_crime_data, filtered_arrest_data, left_on=['DATE OCC', 'AREA NAME'], right_on=['Arrest Date', 'Arrest Area Name'], how='inner')
    return merged_df

def main():
    # Load datasets
    crime_data, arrest_data = load_datasets()
    
    # Preprocess the datasets
    filtered_crime_data = preprocess_crime(crime_data)
    filtered_arrest_data = preprocess_arrest(arrest_data)
    
    # Merge the datasets
    merged_df = merge_dataframes(filtered_crime_data, filtered_arrest_data)
    
    # Display the merged DataFrame
    print(merged_df)

    # Create an SQLite database connection
    conn = sqlite3.connect('merged_dataset.db')

    # Store the merged DataFrame in the SQLite database
    merged_df.to_sql('merged_data', conn, if_exists='replace', index=False)

    # Commit and close the connection
    conn.commit()
    
    # Query all rows from the merged data table
    query = "SELECT * FROM merged_data LIMIT 5"
    queried_df = pd.read_sql_query(query, conn)
    
    # Display the first few rows of the queried DataFrame to confirm
    print("Queried DataFrame from SQLite:")
    print(queried_df)
    
    # Close the connection
    conn.close()
    
    # Save the merged DataFrame to a CSV file
    output_csv = os.path.join(os.getcwd(), 'merged_dataset.csv')
    merged_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"Merged dataset saved as {output_csv}")

if __name__ == "__main__":
    main()
