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
        crime_data['Year'] = crime_data['DATE OCC'].dt.year
        crime_cols = ['DATE OCC', 'AREA NAME', 'Crm Cd Desc', 'Vict Age', 'Vict Sex', 'Vict Descent']
        filtered_crime_data = crime_data[crime_cols]
        logging.info("Crime data preprocessing completed.")
    except Exception as e:
        logging.error(f"Error during crime data preprocessing: {e}")
        return None
    return filtered_crime_data


def preprocess_arrest(arrest_data):
    try:
        arrest_data = validate_data(arrest_data, ['Arrest Date', 'Area Name'])
        arrest_data['Arrest Date'] = pd.to_datetime(arrest_data['Arrest Date'])
        arrest_data['Year'] = arrest_data['Arrest Date'].dt.year
        arrest_data.rename(columns={'Area Name': 'Arrest Area Name'}, inplace=True)
        arrest_cols = ['Arrest Area Name', 'Arrest Date', 'Charge Group Description', 'Age', 'Sex Code', 'Descent Code']
        filtered_arrest_data = arrest_data[arrest_cols]
        logging.info("Arrest data preprocessing completed.")
    except Exception as e:
        logging.error(f"Error during arrest data preprocessing: {e}")
        return None
    return filtered_arrest_data

def merge_dataframes(filtered_crime_data, filtered_arrest_data):
    try:
        merged_df = pd.merge(filtered_crime_data, filtered_arrest_data, left_on=['DATE OCC', 'AREA NAME'], right_on=['Arrest Date', 'Arrest Area Name'], how='inner')
        logging.info("Dataframes successfully merged.")
    except Exception as e:
        logging.error(f"Error during data merging: {e}")
        return None
    return merged_df


def categorize_crimes(df):
   
    # Crime mapping dictionary
    crime_mapping = {
        # Theft related crimes
    **{crime: "Theft" for crime in [
        "THEFT-GRAND ($950.01 & OVER)EXCPT,GUNS,FOWL,LIVESTK,PROD",
        "THEFT PLAIN - PETTY ($950 & UNDER)", "THEFT FROM MOTOR VEHICLE - PETTY ($950 & UNDER)",
        "SHOPLIFTING - PETTY THEFT ($950 & UNDER)", "THEFT FROM MOTOR VEHICLE - GRAND ($950.01 AND OVER)",
        "SHOPLIFTING-GRAND THEFT ($950.01 & OVER)", "THEFT PLAIN - ATTEMPT",
        "PURSE SNATCHING", "PURSE SNATCHING - ATTEMPT", "PICKPOCKET", "PICKPOCKET, ATTEMPT",'DISHONEST EMPLOYEE - GRAND THEFT',
        "THEFT, PERSON", "THEFT FROM PERSON - ATTEMPT", "CREDIT CARDS, FRAUD USE ($950.01 & OVER)",'BUNCO, PETTY THEFT',
        "CREDIT CARDS, FRAUD USE ($950 & UNDER)", "DEFRAUDING INNKEEPER/THEFT OF SERVICES, $950 & UNDER",'TILL TAP - PETTY ($950 & UNDER)',
        "DEFRAUDING INNKEEPER/THEFT OF SERVICES, OVER $950.01", "THEFT, COIN MACHINE - PETTY ($950 & UNDER)",'GRAND THEFT / INSURANCE FRAUD',
        "THEFT, COIN MACHINE - GRAND ($950.01 & OVER)", "THEFT, COIN MACHINE - ATTEMPT",'BUNCO, GRAND THEFT','DISHONEST EMPLOYEE - PETTY THEFT',
        "PETTY THEFT - AUTO REPAIR", "GRAND THEFT / AUTO REPAIR", 'TILL TAP - GRAND THEFT ($950.01 & OVER)', 'DISHONEST EMPLOYEE ATTEMPTED THEFT',
        "COUNTERFEIT", "FALSE POLICE REPORT", "SHOPLIFTING - ATTEMPT", "DOCUMENT WORTHLESS ($200 & UNDER)", 'TILL TAP - GRAND THEFT ($950.01 & OVER)',
        "DOCUMENT WORTHLESS ($200.01 & OVER)","EMBEZZLEMENT, GRAND THEFT ($950.01 & OVER)", "EMBEZZLEMENT, PETTY THEFT ($950 & UNDER)"
    ]},

    **{crime: "Vehicle Theft" for crime in [
        "VEHICLE - STOLEN", "VEHICLE, STOLEN - OTHER (MOTORIZED SCOOTERS, BIKES, ETC)",
        "VEHICLE - ATTEMPT STOLEN", "BIKE - STOLEN", "BIKE - ATTEMPTED STOLEN", "BOAT - STOLEN",'THEFT FROM MOTOR VEHICLE - ATTEMPT'
    ]},

    **{crime: "Burglary" for crime in [
        "BURGLARY FROM VEHICLE", "BURGLARY FROM VEHICLE, ATTEMPTED", "BURGLARY",
        "BURGLARY, ATTEMPTED", 'ATTEMPTED ROBBERY','ROBBERY'
    ]},

    **{crime: "Assault" for crime in [
        "ASSAULT WITH DEADLY WEAPON, AGGRAVATED ASSAULT", "BATTERY - SIMPLE ASSAULT",
        "INTIMATE PARTNER - SIMPLE ASSAULT", "INTIMATE PARTNER - AGGRAVATED ASSAULT",
        "BATTERY WITH SEXUAL CONTACT", "ASSAULT WITH DEADLY WEAPON ON POLICE OFFICER",
        "BATTERY POLICE (SIMPLE)", "BATTERY ON A FIREFIGHTER", "CHILD ABUSE (PHYSICAL) - SIMPLE ASSAULT",
        "CHILD ABUSE (PHYSICAL) - AGGRAVATED ASSAULT", 'OTHER ASSAULT'
    ]},

    **{crime: "Sexual Offense" for crime in [
        "RAPE, FORCIBLE", "RAPE, ATTEMPTED", "SEXUAL PENETRATION W/FOREIGN OBJECT",
        "LEWD/LASCIVIOUS ACTS WITH CHILD", "SODOMY/SEXUAL CONTACT B/W PENIS OF ONE PERS TO ANUS OTH",
        "ORAL COPULATION", "INDECENT EXPOSURE", "PEEPING TOM", "LEWD CONDUCT",
        "SEX,UNLAWFUL(INC MUTUAL CONSENT, PENETRATION W/ FRGN OBJ", "BIGAMY", 'Sexual Exploitation',
        "INCEST (SEXUAL ACTS BETWEEN BLOOD RELATIVES)", "BEASTIALITY, CRIME AGAINST NATURE SEXUAL ASSLT WITH ANIM", "PIMPING"
    ]},

    **{crime: "Human Trafficking" for crime in [
        "KIDNAPPING", "KIDNAPPING - GRAND ATTEMPT", "CHILD STEALING",
        "HUMAN TRAFFICKING - INVOLUNTARY SERVITUDE", "HUMAN TRAFFICKING - COMMERCIAL SEX ACTS", "FALSE IMPRISONMENT",'Human Trafficking'
    ]},

    **{crime: "Firearm Offense" for crime in [
        "DISCHARGE FIREARMS/SHOTS FIRED", "SHOTS FIRED AT INHABITED DWELLING",
        "SHOTS FIRED AT MOVING VEHICLE, TRAIN OR AIRCRAFT", "BRANDISH WEAPON",
        "ASSAULT WITH DEADLY WEAPON ON POLICE OFFICER", "WEAPONS POSSESSION/BOMBING","FIREARMS EMERGENCY PROTECTIVE ORDER (FIREARMS EPO)",
        "FIREARMS EMERGENCY PROTECTIVE ORDER (FIREARMS EPO)","FIREARMS RESTRAINING ORDER (FIREARMS RO)",'REPLICA FIREARMS(SALE,DISPLAY,MANUFACTURE OR DISTRIBUTE)'
    ]},

    **{crime: "Vandalism" for crime in [
        "VANDALISM - FELONY ($400 & OVER, ALL CHURCH VANDALISMS)", "VANDALISM - MISDEAMEANOR ($399 OR UNDER)",'DISRUPT SCHOOL',"TRAIN WRECKING",'PANDERING',
        "THROWING OBJECT AT MOVING VEHICLE", 'TELEPHONE PROPERTY - DAMAGE','DISTURBING THE PEACE','Disturbance','INCITING A RIOT','LYNCHING - ATTEMPTED','LYNCHING'
    ]},

    **{crime: "Coercive Offense" for crime in [
        "EXTORTION", "CRIMINAL THREATS - NO WEAPON DISPLAYED", "ARSON",
        "VIOLATION OF RESTRAINING ORDER", "VIOLATION OF COURT ORDER",'Obstruction', 'CONTRIBUTING','Sabotage',
         "THREATENING PHONE CALLS/LETTERS", "BOMB SCARE","BLOCKING DOOR INDUCTION CENTER"
    ]},

    **{crime: "Cyber-crime" for crime in [
        "UNAUTHORIZED COMPUTER ACCESS", "THEFT OF IDENTITY",'CREDIT CARDS, FRAUD USE ($950 & UNDER'
    ]},
    
    **{crime: "Drug and drunk offense" for crime in [
        "DRUGS, TO A MINOR","DRUNK ROLL", "DRUNK ROLL - ATTEMPT"
    ]},
    
    **{crime: "Child neglencey and offense" for crime in [
        "CHILD NEGLECT (SEE 300 W.I.C.)", 'CHILD ANNOYING (17YRS & UNDER)','Child Neglect', 'CHILD PORNOGRAPHY','CRM AGNST CHLD (13 OR UNDER) (14-15 & SUSP 10 YRS OLDER)',
        'Child neglect and offense','CHILD ABANDONMENT'
    ]},
    
    **{crime: "Legal offense" for crime in [
        "CONSPIRACY","OBSTRUCTING JUSTICE",'SEX OFFENDER REGISTRANT OUT OF COMPLIANCE','FAILURE TO YIELD',"RESISTING ARREST", 'Legal Order',
        'Obstructing Justice','CONTEMPT OF COURT',"VIOLATION OF TEMPORARY RESTRAINING ORDER"
    ]},

    # categorized miscellaneous crimes
   
        "OTHER MISCELLANEOUS CRIME": "Miscellaneous Crime",
        "DRIVING WITHOUT OWNER CONSENT (DWOC)": "Traffic Offense",
        "Traffic Offense": "Traffic Offense",
        "Trespass": "Trespass",
        "Harassment": "Harassment",
        "Forgery": "Forgery",
        "Environmental Crime": "Environmental Crime",
        "DOCUMENT FORGERY / STOLEN FELONY": "Forgery",
        "Animal Cruelty": "Animal Cruelty",
        "BUNCO, ATTEMPT": "Miscellaneous Crime",
        "BRIBERY": "Serious Offenses",
        "MANSLAUGHTER, NEGLIGENT": "Serious Offenses",
        "LETTERS, LEWD  -  TELEPHONE CALLS, LEWD": "Harassment",
        "STALKING": "Harassment",
        "PROWLER": "Trespass",
        "TRESPASSING": "Trespass",
        "RECKLESS DRIVING": "Traffic Offense",
        "ILLEGAL DUMPING": "Environmental Crime",
        "CRUELTY TO ANIMALS": "Animal Cruelty",
        "FAILURE TO DISPERSE": "Miscellaneous Crime"
    }

    # Apply the crime mapping
    df['Crm Cd Desc'] = df['Crm Cd Desc'].map(crime_mapping).fillna(df['Crm Cd Desc'])
    return df

def main():
    # Load datasets
    crime_data, arrest_data = load_datasets()
    if crime_data is None or arrest_data is None:
        return

    # Preprocess the datasets
    filtered_crime_data = preprocess_crime(crime_data)
    if filtered_crime_data is None:
        return

    filtered_arrest_data = preprocess_arrest(arrest_data)
    if filtered_arrest_data is None:
        return

    # Merge the datasets
    merged_df = merge_dataframes(filtered_crime_data, filtered_arrest_data)
    if merged_df is None:
        return

    # Create an SQLite database connection
    conn = sqlite3.connect('merged_dataset.db')
    try:
        # Store the merged DataFrame in the SQLite database
        merged_df.to_sql('merged_data', conn, if_exists='replace', index=False)
        conn.commit()
        logging.info("Data successfully saved to SQLite database.")

        # Query all rows from the merged data table
        query = "SELECT * FROM merged_data LIMIT 5"
        queried_df = pd.read_sql_query(query, conn)
        
        # Display the first few rows of the queried DataFrame to confirm
        print("Queried DataFrame from SQLite:")
        print(queried_df)
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
