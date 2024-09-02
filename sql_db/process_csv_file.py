from sqlalchemy.orm import Session
from . import models, crud
import pandas as pd
import os
from flask import current_app
import logging
import chardet


def add_gender_csv_file_to_db(db: Session, file, uc_campus_id, year):
    # Detect file encoding
    with open(file, 'rb') as raw_file:
        result = chardet.detect(raw_file.read(10000))
    
    # Read the CSV file with detected encoding
    df = pd.read_csv(file, encoding=result['encoding'])
    
    for _, row in df.iterrows():
        # Create or update high school
        high_school = crud.get_high_school_by_name_and_city(db, row['School'], row['City'])


def add_ethnicity_csv_file_to_db(db: Session, file, uc_campus_name, year, high_school_type):
    logging.getLogger('werkzeug').info(f"In add_ethnicity_csv_file_to_db file: {file}, {uc_campus_name}, {year}, {high_school_type}")

    # Detect file encoding
    with open(file, 'rb') as raw_file:
        result = chardet.detect(raw_file.read(10000))
    
    # Read the tab-delimited CSV file with detected encoding
    df = pd.read_csv(file, encoding=result['encoding'], sep='\t')
    
    # Log the columns and number of rows in the DataFrame
    logging.getLogger('werkzeug').info(f"Columns in the DataFrame: {df.columns.tolist()}")
    logging.getLogger('werkzeug').info(f"Number of rows in the DataFrame: {len(df)}")
    # logging.getLogger('werkzeug').info("First 10 rows of the DataFrame:")
    # for index, row in df.head(10).iterrows():
    #     logging.getLogger('werkzeug').info(f"Row {index}: {row.to_dict()}")

    # Check if required columns are present
    required_columns = ['Calculation1', 'School', 'City', 'County/State/ Territory', 'Count']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        error_msg = f"Missing required columns: {', '.join(missing_columns)}"
        logging.getLogger('werkzeug').error(error_msg)
        raise ValueError(error_msg)

    # Get UC campus ID
    uc_campus = crud.get_uc_campus_by_name(db, uc_campus_name)
    if not uc_campus:
        error_msg = f"Invalid UC campus name: {uc_campus_name}"
        logging.getLogger('werkzeug').error(error_msg)
        raise ValueError(error_msg)
    
    uc_campus_id = uc_campus.id

    # Collect all ethnicity records for batch insertion
    ethnicity_records = []

    for _, row in df.iterrows():
        try:
            # Create or update high school
            uc_school_name = row['Calculation1']
            high_school = crud.get_high_school_by_uc_school_name(db, uc_school_name)
            if not high_school:
                if high_school_type.upper() in ['CA_PUBLIC', 'CA_PRIVATE']:
                    county = row['County/State/ Territory']
                    state = 'CA'
                    country = 'United States'
                elif high_school_type.upper() == 'NON_CA':
                    county = None
                    state = row['County/State/ Territory']
                    country = 'United States'
                elif high_school_type.upper() == 'FOREIGN':
                    county = None
                    state = None
                    country = row['County/State/ Territory']
                else:
                    raise ValueError(f"Invalid high school type: {high_school_type}")

                high_school = crud.create_high_school(db, {
                    'uc_school_name': uc_school_name,
                    'school_name': row['School'],
                    'city': row['City'],
                    'county': county,
                    'state': state,
                    'country': country,
                    'is_public': high_school_type.upper() == 'CA_PUBLIC'
                })
            # Get admission type from the 'Count' column
            admission_type = row['Count']
            
            # Create ethnicity records
            ethnicities = ['All','African American', 'American Indian', 'Hispanic/ Latinx', 'Pacific Islander', 'Asian', 'White', 'Domestic Unknown', "Int'l"]
            for ethnicity in ethnicities:
                count = row.get(ethnicity, 0)  # Use 0 if the column doesn't exist
                if pd.notna(count):  # Check if the value is not NaN
                    ethnicity_records.append({
                        'high_school_id': high_school.id,
                        'uc_campus_id': uc_campus_id,
                        'admission_type': admission_type,
                        'academic_year': year,
                        'ethnicity': ethnicity,
                        'count': int(count)
                    })
        except Exception as e:
            logging.getLogger('werkzeug').error(f"Error processing row: {row}")
            logging.getLogger('werkzeug').error(f"Error details: {str(e)}")
            # Rollback the transaction for this row
            db.rollback()
            # Continue processing other rows
            continue

    # Perform batch insertion of ethnicity records
    if ethnicity_records:
        crud.bulk_create_uc_admission_ethnicity(db, ethnicity_records)

    # Commit the transaction after processing all rows
    db.commit()

    logging.getLogger('werkzeug').info("All rows processed successfully")
    return True

    
