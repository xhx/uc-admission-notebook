from sqlalchemy.orm import Session
from . import models, crud
import pandas as pd
import os
from flask import current_app
import logging


def add_gender_csv_file_to_db(db: Session, file, uc_campus_id, year):
    df = pd.read_csv(file)
    
    for _, row in df.iterrows():
        # Create or update high school
        high_school = crud.get_high_school_by_name_and_city(db, row['School'], row['City'])


def add_ethnicity_csv_file_to_db(db: Session, file, uc_campus_id, year, high_school_type):
    df = pd.read_csv(file)
    
    for _, row in df.iterrows():
        # Create or update high school
        high_school = crud.get_high_school_by_name_and_city(db, row['School'], row['City'])
        if not high_school:
            high_school = crud.create_high_school(db, {
                'school_name': row['School'],
                'city': row['City'],
                'county': row['County/State/ Territory'],
                'state': 'CA',  # Assuming all schools are in California
                'country': 'United States',
                'is_public': high_school_type == 'CA_PUBLIC'
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
                    'year': year,
                    'ethnicity': ethnicity,
                    'count': int(count)
                })

    db.commit()


