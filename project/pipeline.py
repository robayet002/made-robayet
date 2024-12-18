import pandas as pd
import sqlite3
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def load_datasets():
    try:
        crime_data = pd.read_csv("https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD")
        arrest_data = pd.read_csv("https://data.lacity.org/api/views/amvf-fr72/rows.csv?accessType=DOWNLOAD")
    except pd.errors.EmptyDataError:
        print("Error: No data found in the source.")
        return None, None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None, None
    return crime_data, arrest_data


def validate_data(df, required_columns):
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.error(f"Missing columns: {', '.join(missing_columns)}")
        raise ValueError(f"Dataframe is missing columns: {', '.join(missing_columns)}")
    return df


def preprocess_crime(crime_data):
    try:
        crime_data = validate_data(crime_data, ['DATE OCC', 'Crm Cd Desc'])
        crime_data.dropna(subset=['DATE OCC', 'Crm Cd Desc'], inplace=True)
        crime_data['DATE OCC'] = pd.to_datetime(crime_data['DATE OCC'])
        crime_data['Date of Crime'] = crime_data['DATE OCC'].dt.date
        include_categories = ['Vehicle Theft', 'Larceny', 'Burglary', 'Prostitution/Allied', 'Aggravated Assault',
                              'Robbery', 'Weapon (carry/poss)',
                              'Sex (except rape/prst)', 'Against Family/Child', 'Rape', 'Disturbing the Peace',
                              'Fraud/Embezzlement', 'Forgery/Counterfeit', 'Moving Traffic Violations',
                              'Federal Offenses', 'Homicide', 'Drunkeness']
        crime_data = crime_data[crime_data['Crm Cd Desc'].isin(include_categories)]
        crime_cols = ['Date of Crime', 'AREA NAME', 'Crm Cd Desc', 'Vict Age', 'Vict Sex', 'Vict Descent']
        filtered_crime_data = crime_data[crime_cols]
        logging.info("Crime data preprocessing completed.")
    except Exception as e:
        logging.error(f"Error during crime data preprocessing: {e}")
        return None
    return remove_null_rows(filtered_crime_data)


def preprocess_arrest(arrest_data):
    try:
        arrest_data = validate_data(arrest_data, ['Arrest Date', 'Area Name'])
        arrest_data['Arrest Date'] = pd.to_datetime(arrest_data['Arrest Date'])
        arrest_data['Date of Arrest'] = arrest_data['Arrest Date'].dt.date
        arrest_data.rename(columns={'Area Name': 'Arrest Area Name'}, inplace=True)
        arrest_data = arrest_data[arrest_data['Charge Group Description'] != "Miscellaneous Other Violations"]
        arrest_cols = ['Date of Arrest', 'Arrest Area Name', 'Charge Group Description', 'Age', 'Sex Code',
                       'Descent Code']
        filtered_arrest_data = arrest_data[arrest_cols]
        logging.info("Arrest data preprocessing completed.")
    except Exception as e:
        logging.error(f"Error during arrest data preprocessing: {e}")
        return None
    return remove_null_rows(filtered_arrest_data)


def merge_dataframes(filtered_crime_data, filtered_arrest_data):
    try:
        merged_df = pd.merge(filtered_crime_data, filtered_arrest_data,
                             left_on=['Date of Crime', 'AREA NAME', 'Crm Cd Desc'],
                             right_on=['Date of Arrest', 'Arrest Area Name', 'Charge Group Description'], how='inner')
        logging.info("Dataframes successfully merged.")
    except Exception as e:
        logging.error(f"Error during data merging: {e}")
        return None
    return merged_df


def remove_null_rows(df):
    initial_count = len(df)
    df_cleaned = df.dropna()
    logging.info(f"Removed {initial_count - len(df_cleaned)} rows due to null values.")
    return df_cleaned


def categorize_crimes(df):
    # Updated Crime mapping dictionary
    crime_mapping = {
        **{crime: "Larceny" for crime in [
            "THEFT-GRAND ($950.01 & OVER)EXCPT,GUNS,FOWL,LIVESTK,PROD",
            "THEFT PLAIN - PETTY ($950 & UNDER)", "THEFT FROM MOTOR VEHICLE - PETTY ($950 & UNDER)",
            "SHOPLIFTING - PETTY THEFT ($950 & UNDER)", "THEFT FROM MOTOR VEHICLE - GRAND ($950.01 AND OVER)",
            "SHOPLIFTING-GRAND THEFT ($950.01 & OVER)", "THEFT PLAIN - ATTEMPT",
            "PURSE SNATCHING", "PURSE SNATCHING - ATTEMPT", "PICKPOCKET", "PICKPOCKET, ATTEMPT",
            "THEFT, PERSON", "THEFT FROM PERSON - ATTEMPT", "CREDIT CARDS, FRAUD USE ($950.01 & OVER)",
            "CREDIT CARDS, FRAUD USE ($950 & UNDER)", "DEFRAUDING INNKEEPER/THEFT OF SERVICES, $950 & UNDER",
            "DEFRAUDING INNKEEPER/THEFT OF SERVICES, OVER $950.01", "THEFT, COIN MACHINE - PETTY ($950 & UNDER)",
            "THEFT, COIN MACHINE - GRAND ($950.01 & OVER)", "THEFT, COIN MACHINE - ATTEMPT",
            "PETTY THEFT - AUTO REPAIR", "GRAND THEFT / AUTO REPAIR"
        ]},
        **{crime: "Vehicle Theft" for crime in [
            "VEHICLE - STOLEN", "VEHICLE, STOLEN - OTHER (MOTORIZED SCOOTERS, BIKES, ETC)",
            "VEHICLE - ATTEMPT STOLEN", "BIKE - STOLEN", "BIKE - ATTEMPTED STOLEN", "BOAT - STOLEN",
            "THEFT FROM MOTOR VEHICLE - ATTEMPT"
        ]},
        **{crime: "Burglary" for crime in [
            "BURGLARY FROM VEHICLE", "BURGLARY FROM VEHICLE, ATTEMPTED", "BURGLARY",
            "BURGLARY, ATTEMPTED"
        ]},
        **{crime: "Robbery" for crime in [
            "ATTEMPTED ROBBERY", "ROBBERY"
        ]},
        **{crime: "Aggravated Assault" for crime in [
            "ASSAULT WITH DEADLY WEAPON, AGGRAVATED ASSAULT", "BATTERY - SIMPLE ASSAULT",
            "INTIMATE PARTNER - SIMPLE ASSAULT", "INTIMATE PARTNER - AGGRAVATED ASSAULT",
            "BATTERY WITH SEXUAL CONTACT", "ASSAULT WITH DEADLY WEAPON ON POLICE OFFICER",
            "BATTERY POLICE (SIMPLE)", "BATTERY ON A FIREFIGHTER", "CHILD ABUSE (PHYSICAL) - SIMPLE ASSAULT",
            "CHILD ABUSE (PHYSICAL) - AGGRAVATED ASSAULT", "OTHER ASSAULT"
        ]},
        **{crime: "Rape" for crime in [
            "RAPE, FORCIBLE", "RAPE, ATTEMPTED"
        ]},
        **{crime: "Fraud/Embezzlement" for crime in [
            "COUNTERFEIT", "FALSE POLICE REPORT", "EMBEZZLEMENT, GRAND THEFT ($950.01 & OVER)",
            "EMBEZZLEMENT, PETTY THEFT ($950 & UNDER)", "DOCUMENT WORTHLESS ($200 & UNDER)",
            "DOCUMENT WORTHLESS ($200.01 & OVER)"
        ]},
        **{crime: "Weapon (carry/poss)" for crime in [
            "DISCHARGE FIREARMS/SHOTS FIRED", "SHOTS FIRED AT INHABITED DWELLING",
            "SHOTS FIRED AT MOVING VEHICLE, TRAIN OR AIRCRAFT", "BRANDISH WEAPON",
            "ASSAULT WITH DEADLY WEAPON ON POLICE OFFICER", "WEAPONS POSSESSION/BOMBING"
        ]},
        **{crime: "Disturbing the Peace" for crime in [
            "DISTURBING THE PEACE", "THROWING OBJECT AT MOVING VEHICLE",
            "INCITING A RIOT", "FAILURE TO DISPERSE", "VIOLATION OF RESTRAINING ORDER",
            "VIOLATION OF COURT ORDER"
        ]},
        **{crime: "Forgery/Counterfeit" for crime in [
            "FORGERY", "COUNTERFEIT", "DOCUMENT FORGERY / STOLEN FELONY"
        ]},
        **{crime: "Against Family/Child" for crime in [
            "CHILD NEGLECT (SEE 300 W.I.C.)", "CHILD ANNOYING (17YRS & UNDER)",
            "CHILD PORNOGRAPHY", "CHILD ABUSE (PHYSICAL) - SIMPLE ASSAULT",
            "CHILD ABUSE (PHYSICAL) - AGGRAVATED ASSAULT", "CHILD STEALING"
        ]},

        **{crime: "Drunkeness" for crime in [
            "DRUGS,TO A MINOR", "DRUNK ROLL", "DRUNK ROLL - ATTEMPT"
        ]},

        **{crime: "Miscellaneous Other Violations" for crime in
           ["OTHER MISCELLANEOUS CRIME", "BUNCO, ATTEMPT", "OBSTRUCTION OF JUSTICE", "CONSPIRACY", "FAILURE TO YIELD",
            "RESISTING ARREST", "BLOCKING DOOR INDUCTION CENTER", "BOMB SCARE", "PROWLER", "DISRUPT SCHOOL",
            "DISTURBANCE"]},

        **{crime: "Moving Traffic Violations" for crime in
           ["RECKLESS DRIVING", "DRIVING WITHOUT OWNER CONSENT (DWOC)", "TRAFFIC OFFENSE"]},

        **{crime: "Environmental Crime" for crime in
           ["ILLEGAL DUMPING", "ENVIRONMENTAL CRIME"]},

        **{crime: "Animal Cruelty" for crime in
           ["ANIMAL CRUELTY", "CRUELTY TO ANIMALS"]},

        **{crime: "Federal Offenses" for crime in
           ["SEX OFFENDER REGISTRANT OUT OF COMPLIANCE", "FIREARMS EMERGENCY PROTECTIVE ORDER (FIREARMS EPO)",
            "FIREARMS RESTRAINING ORDER (FIREARMS RO)", "REPLICA FIREARMS(SALE, DISPLAY, MANUFACTURE OR DISTRIBUTE)"]},

        **{crime: "Homicide" for crime in
           ["CRIMINAL HOMICIDE"]},

        **{crime: "Sex (except rape/prst)" for crime in
           ["INDECENT EXPOSURE", "LEWD CONDUCT", "SEXUAL PENETRATION W/FOREIGN OBJECT",
            "LEWD/LASCIVIOUS ACTS WITH CHILD", "SODOMY/SEXUAL CONTACT B/W PENIS OF ONE PERS TO ANUS OTH",
            "ORAL COPULATION", "SEX,UNLAWFUL(INC MUTUAL CONSENT, PENETRATION W/ FRGN OBJ"]},

        **{crime: "Prostitution/Allied" for crime in ["PIMPING", "PROSTITUTION"]}}

    # Apply the crime mapping
    df['Crm Cd Desc'] = df['Crm Cd Desc'].map(crime_mapping).fillna(df['Crm Cd Desc'])
    return df


def main():
    # Load datasets
    crime_data, arrest_data = load_datasets()
    if crime_data is None or arrest_data is None:
        return

    # Apply categorization to the crime data
    categorized_crime_data = categorize_crimes(crime_data)
    if categorized_crime_data is None:
        logging.error("Failed to categorize crime data.")
        return

    # Preprocess the datasets
    filtered_crime_data = preprocess_crime(categorized_crime_data)
    if filtered_crime_data is None:
        return

    filtered_arrest_data = preprocess_arrest(arrest_data)
    if filtered_arrest_data is None:
        return

    # Merge the categorized datasets
    merged_df = merge_dataframes(filtered_crime_data, filtered_arrest_data)
    if merged_df is None:
        return

    # Create an SQLite database connection
    conn = sqlite3.connect('total_dataset.db')
    try:
        # Store the individual categorized DataFrames in the SQLite database
        filtered_crime_data.to_sql('crime_data', conn, if_exists='replace', index=False)
        filtered_arrest_data.to_sql('arrest_data', conn, if_exists='replace', index=False)
        logging.info("Individual categorized datasets successfully saved to SQLite database.")

        # Query all rows from the merged data table to confirm
        query1 = "SELECT * FROM crime_data LIMIT 5"
        query2 = "SELECT * FROM arrest_data LIMIT 5"
        queried_df1 = pd.read_sql_query(query1, conn)
        queried_df2 = pd.read_sql_query(query2, conn)

        # Display the first few rows of the queried DataFrame to confirm
        print("Queried DataFrame from SQLite:")
        print(queried_df1)
        print(queried_df2)
    except Exception as e:
        logging.error(f"Database operation failed: {e}")
    finally:
        conn.close()

     # Save the merged DataFrame to a CSV file
    output_csv = os.path.join(os.getcwd(), 'merged_dataset.csv')
    merged_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    logging.info(f"Merged dataset saved as {output_csv}")


if __name__ == "__main__":
    main()
