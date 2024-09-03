from sqlalchemy.orm import Session
from . import models, crud
import pandas as pd
import os
from flask import current_app
import logging
import chardet
import re



def add_gpa_csv_file_to_db(db: Session, file, uc_campus_name, year):
    logging.getLogger('werkzeug').info(f"In add_gpa_csv_file_to_db file: {file}, {uc_campus_name}, {year}")

    # Detect file encoding
    with open(file, 'rb') as raw_file:
        result = chardet.detect(raw_file.read(10000))
    
    # Read the CSV file with detected encoding
    df = pd.read_csv(file, encoding=result['encoding'], sep='\t')
    
    # Log the columns and number of rows in the DataFrame
    logging.getLogger('werkzeug').info(f"Columns in the DataFrame: {df.columns.tolist()}")
    logging.getLogger('werkzeug').info(f"Number of rows in the DataFrame: {len(df)}")

    # Check if required columns are present
    required_columns = ['Calculation1', 'School', 'City', 'County/State/Country', 'App GPA', 'Adm GPA', 'Enrl GPA']
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

    # Collect all GPA records for batch insertion
    gpa_records = []

    # i = 0   
    for _, row in df.iterrows():
        # # # testing purpose
        # i += 1
        # if i > 100:
        #     break

        try:
            # Create or update high school
            uc_school_name = row['Calculation1']
            # Remove leading zeros from the numerical part of the school name
            if uc_school_name and any(char.isdigit() for char in uc_school_name):
                match = re.search(r'(\d+)$', uc_school_name)
                if match:
                    digits = match.group(1)
                    stripped_digits = digits.lstrip('0')
                    uc_school_name = re.sub(r'\d+$', stripped_digits, uc_school_name)


            
            high_school = crud.get_high_school_by_uc_school_name(db, uc_school_name)
            if not high_school:
                if pd.isna(row['County/State/Country']) or row['County/State/Country'].upper() == 'N/A':
                    county = None
                    state = None
                    country = 'Unknown'
                elif row['County/State/Country'] in ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']:
                    county = None
                    state = row['County/State/Country']
                    country = 'United States'
                else:
                    county = row['County/State/Country']
                    state = 'CA'
                    country = 'United States'

                high_school = crud.create_high_school(db, {
                    'uc_school_name': uc_school_name,
                    'school_name': row['School'],
                    'city': row['City'],
                    'county': county,
                    'state': state,
                    'country': country,
                    'is_public': True  # Assuming all schools are public, adjust if needed
                })

            # Create GPA record
            gpa_types = ['App GPA', 'Adm GPA', 'Enrl GPA']
            gpa_type_mapping = {
                'App GPA': 'App',
                'Adm GPA': 'Adm',
                'Enrl GPA': 'Enr'
            }
            for gpa_type in gpa_types:
                gpa_value = row.get(gpa_type)
                if pd.notna(gpa_value):  # Check if the value is not NaN
                    gpa_records.append({
                        'high_school_id': high_school.id,
                        'uc_campus_id': uc_campus_id,
                        'academic_year': year,
                        'admission_type': gpa_type_mapping[gpa_type],
                        'mean_gpa': float(gpa_value),
                    })
        except Exception as e:
            logging.getLogger('werkzeug').error(f"Error processing row: {row}")
            logging.getLogger('werkzeug').error(f"Error details: {str(e)}")
            continue

    # Perform batch insertion of GPA records
    if gpa_records:
        try:
            crud.bulk_create_uc_admission_gpa(db, gpa_records)            
            logging.getLogger('werkzeug').info("All rows processed successfully")
        except Exception as e:
            logging.getLogger('werkzeug').error(f"Error bulk creating uc admission gpa: {e}")
            db.rollback()

    return True


def add_gender_csv_file_to_db(db: Session, file, uc_campus_name, year):
    logging.getLogger('werkzeug').info(f"In add_gender_csv_file_to_db file: {file}, {uc_campus_name}, {year}")

    # Detect file encoding
    with open(file, 'rb') as raw_file:
        result = chardet.detect(raw_file.read(10000))
    
    # Read the CSV file with detected encoding
    df = pd.read_csv(file, encoding=result['encoding'], sep='\t')
    
    # Log the columns and number of rows in the DataFrame
    logging.getLogger('werkzeug').info(f"Columns in the DataFrame: {df.columns.tolist()}")
    logging.getLogger('werkzeug').info(f"Number of rows in the DataFrame: {len(df)}")

    # Check if required columns are present
    required_columns = ['Calculation1', 'School', 'City', 'County/State/ Territory', 'Count', 'All', 'Female', 'Male']
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

    # Collect all gender records for batch insertion
    gender_records = []

    for _, row in df.iterrows():
        try:
            # Create or update high school
            uc_school_name = row['Calculation1']
            high_school = crud.get_high_school_by_uc_school_name(db, uc_school_name)
            if not high_school:
                if pd.isna(row['County/State/ Territory']) or row['County/State/ Territory'] == 'N/A':
                    county = None
                    state = None
                    country = 'Unknown'
                elif row['County/State/ Territory'] in ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']:
                    county = None
                    state = row['County/State/ Territory']
                    country = 'United States'
                else:
                    county = row['County/State/ Territory']
                    state = 'CA'
                    country = 'United States'

                high_school = crud.create_high_school(db, {
                    'uc_school_name': uc_school_name,
                    'school_name': row['School'],
                    'city': row['City'],
                    'county': county,
                    'state': state,
                    'country': country,
                    'is_public': True  # Assuming all schools are public, adjust if needed
                })

            # Get admission type from the 'Count' column
            admission_type = row['Count']
            
            # Map CSV columns to database columns
            total_applicants = int(row['All']) if pd.notna(row['All']) else 0
            male_applicants = int(row['Male']) if pd.notna(row['Male']) else 0
            female_applicants = int(row['Female']) if pd.notna(row['Female']) else 0
            other_applicants = int(row['Other']) if 'Other' in row and pd.notna(row['Other']) else 0
            unknown_gender = int(row['Unknown']) if 'Unknown' in row and pd.notna(row['Unknown']) else 0

            # If 'Unknown' is not in the CSV, calculate it
            if 'Unknown' not in row:
                unknown_gender = total_applicants - male_applicants - female_applicants - other_applicants

            gender_records.append({
                'high_school_id': high_school.id,
                'uc_campus_id': uc_campus_id,
                'admission_type': admission_type,
                'academic_year': year,
                'total_applicants': total_applicants,
                'male_applicants': male_applicants,
                'female_applicants': female_applicants,
                'other_applicants': other_applicants,
                'unknown_gender': unknown_gender
            })
        except Exception as e:
            logging.getLogger('werkzeug').error(f"Error processing row: {row}")
            logging.getLogger('werkzeug').error(f"Error details: {str(e)}")
            continue

    # Perform batch insertion of gender records
    if gender_records:
        try:
            crud.bulk_create_uc_admission_gender(db, gender_records)            
            logging.getLogger('werkzeug').info("All rows processed successfully")
        except Exception as e:
            logging.getLogger('werkzeug').error(f"Error bulk creating uc admission gender: {e}")
            db.rollback()

    return True


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

    # i = 0
    for _, row in df.iterrows():
        # # testing purpose
        # i += 1
        # if i > 100:
        #     break


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
            continue

    # Perform batch insertion of ethnicity records
    if ethnicity_records:
        try:
            crud.bulk_create_uc_admission_ethnicity(db, ethnicity_records)            
            logging.getLogger('werkzeug').info("All rows processed successfully")
        except Exception as e:
            logging.getLogger('werkzeug').error(f"Error bulk creating uc admission ethnicity: {e}")
            db.rollback()

    return True

    

def clean_high_school_table(db: Session):
    logging.getLogger('werkzeug').info("Starting high school table cleanup")

    # Get all high schools
    high_schools = db.query(models.HighSchool).all()

    deleted_schools = []

    for high_school in high_schools:
        uc_school_name = high_school.uc_school_name

        # Clean the uc_school_name
        if uc_school_name and any(char.isdigit() for char in uc_school_name):
            match = re.search(r'(\d+)$', uc_school_name)
            if match:
                digits = match.group(1)
                stripped_digits = digits.lstrip('0')
                uc_school_name2 = re.sub(r'\d+$', stripped_digits, uc_school_name)

                # If the cleaned name is different, delete the row
                if uc_school_name2 != uc_school_name:
                    logging.getLogger('werkzeug').info(f"Deleting high school: {uc_school_name}")
                    db.delete(high_school)
                    deleted_schools.append(uc_school_name)

    # Commit the changes
    try:
        db.commit()
        logging.getLogger('werkzeug').info(f"High school table cleanup completed. Deleted {len(deleted_schools)} rows.")
    except Exception as e:
        db.rollback()
        logging.getLogger('werkzeug').error(f"Error during high school table cleanup: {str(e)}")

    return deleted_schools


