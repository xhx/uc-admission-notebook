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
    required_columns = ['School', 'City', 'County/State/ Territory', 'Count']
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

    for _, row in df.iterrows():
        try:
            # Create or update high school
            high_school = crud.get_high_school_by_name_and_city(db, row['School'], row['City'])
            if not high_school:
                high_school = crud.create_high_school(db, {
                    'school_name': row['School'],
                    'city': row['City'],
                    'county': row['County/State/ Territory'],
                    'state': 'CA',  # Assuming all schools are in California
                    'country': 'United States',
                    'is_public': high_school_type.upper() == 'CA_PUBLIC'
                })
            
            # Get admission type from the 'Count' column
            admission_type = row['Count']
            
            # Create ethnicity records
            ethnicities = ['African American', 'American Indian', 'Hispanic/ Latinx', 'Pacific Islander', 'Asian', 'White', 'Domestic Unknown', "Int'l"]
            for ethnicity in ethnicities:
                count = row.get(ethnicity, 0)  # Use 0 if the column doesn't exist
                if pd.notna(count):  # Check if the value is not NaN
                    crud.create_uc_admission_ethnicity(db, {
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

    # Commit the transaction after processing all rows
    db.commit()

    logging.getLogger('werkzeug').info("All rows processed successfully")
    return True

    
