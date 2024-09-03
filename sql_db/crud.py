from sqlalchemy.orm import Session
from . import models
import pandas as pd
import os
from flask import current_app
import logging
from sqlalchemy import select
from sqlalchemy.exc import PendingRollbackError
from sqlalchemy import and_
import re


def create_or_update_high_school(db: Session, high_school_data):
    high_school = db.query(models.HighSchool).filter(models.HighSchool.school_name == high_school_data['school_name']).first()
    if high_school:
        for key, value in high_school_data.items():
            setattr(high_school, key, value)
    else:
        high_school = models.HighSchool(**high_school_data)
        db.add(high_school)
    db.commit()
    db.refresh(high_school)
    return high_school

def get_uc_campus_by_name(db: Session, campus_name: str):
    try:
        return db.query(models.UCCampus).filter(models.UCCampus.campus_name == campus_name).first()
    except PendingRollbackError:
        db.rollback()
        return db.query(models.UCCampus).filter(models.UCCampus.campus_name == campus_name).first()

def create_uc_admission_gender(db: Session, admission_data):
    admission = models.UCAdmissionGender(**admission_data)
    db.add(admission)
    db.commit()
    db.refresh(admission)
    return admission

def create_uc_admission_ethnicity(db: Session, ethnicity_data):
    ethnicity = models.UCAdmissionEthnicity(**ethnicity_data)
    db.add(ethnicity)
    db.commit()
    db.refresh(ethnicity)
    return ethnicity

def create_uc_admission_gpa(db: Session, gpa_data):
    gpa = models.UCAdmissionGPA(**gpa_data)
    db.add(gpa)
    db.commit()
    db.refresh(gpa)
    return gpa

def query_database(db: Session, query_type, parameters):
    if query_type == 'admissions':
        return db.query(models.UCAdmissionGender).all()
    elif query_type == 'ethnicity':
        return db.query(models.UCAdmissionEthnicity).all()
    elif query_type == 'gpa':
        return db.query(models.UCAdmissionGPA).all()
    else:
        return "Invalid query type"

def seed_uc_campuses(db: Session):
    campuses = [
        {"campus_name": "Los Angeles", "location": "Los Angeles, CA"},
        {"campus_name": "Berkeley", "location": "Berkeley, CA"},
        {"campus_name": "Davis", "location": "Davis, CA"},
        {"campus_name": "San Diego", "location": "La Jolla, CA"},
        {"campus_name": "Irvine", "location": "Irvine, CA"},
        {"campus_name": "Santa Barbara", "location": "Santa Barbara, CA"},
        {"campus_name": "Riverside", "location": "Riverside, CA"},
        {"campus_name": "Merced", "location": "Merced, CA"},
        {"campus_name": "Santa Cruz", "location": "Santa Cruz, CA"},
        {"campus_name": "San Francisco", "location": "San Francisco, CA"}
    ]

    for campus_data in campuses:
        existing_campus = db.query(models.UCCampus).filter_by(campus_name=campus_data["campus_name"]).first()
        if not existing_campus:
            new_campus = models.UCCampus(**campus_data)
            db.add(new_campus)
    
    db.commit()

def add_files_to_db(db: Session, files_directory: str):
    logging.getLogger('werkzeug').info("Adding files to database")  # This will be visible in Docker logs
    logging.getLogger('werkzeug').info(f"Files directory: {files_directory}")  # This will be visible in Docker logs

    for root, _, files in os.walk(files_directory):
        for file in files:
            file_path = os.path.join(root, file)
            location = os.path.abspath(file_path)
            
            # Extract information from filename
            category = None
            if 'Eth' in file:
                category = models.Category.ETHNICITY
            elif 'GPA' in file:
                category = models.Category.GPA
            elif 'Gdr' in file:
                category = models.Category.GENDER

            # Extract the last 4 digits from the filename
            year = re.search(r'\d{4}(?=\.csv$)', file)
            if year:
                year = int(year.group())
                print(f"Extracted year: {year}")
            else:
                print("Year not found in the filename")



            high_school_type = None
            if 'CA Public' in file:
                high_school_type = models.HighSchoolType.CA_PUBLIC
            elif 'CA Private' in file:
                high_school_type = models.HighSchoolType.CA_PRIVATE
            elif 'Foreign' in file:
                high_school_type = models.HighSchoolType.FOREIGN
            elif 'non-CA' in file:
                high_school_type = models.HighSchoolType.NON_CA
            else:
                high_school_type = models.HighSchoolType.ALL

            uc_campus_name = None
            if 'Berkeley' in file:
                uc_campus_name = 'Berkeley'
            elif 'LA' in file:
                uc_campus_name = 'Los Angeles'
            elif 'Davis' in file:
                uc_campus_name = 'Davis'
            elif 'Irvine' in file:
                uc_campus_name = 'Irvine'
            elif 'UCSB' in file:
                uc_campus_name = 'Santa Barbara'
            elif 'UCSC' in file:
                uc_campus_name = 'Santa Cruz'
            elif 'Riverside' in file:
                uc_campus_name = 'Riverside'
            elif 'Merced' in file:
                uc_campus_name = 'Merced'
            elif 'UCSD' in file:
                uc_campus_name = 'San Diego'
            elif 'San Francisco' in file:
                uc_campus_name = 'San Francisco'

            uc_campus = get_uc_campus_by_name(db, uc_campus_name) if uc_campus_name else None
            
            # Print extracted information
            logging.getLogger('werkzeug').info(f"File: {file}")
            logging.getLogger('werkzeug').info(f"  Location: {location}")
            logging.getLogger('werkzeug').info(f"  Category: {category}")
            logging.getLogger('werkzeug').info(f"  Year: {year}")
            logging.getLogger('werkzeug').info(f"  High School Type: {high_school_type}")
            logging.getLogger('werkzeug').info(f"  UC Campus: {uc_campus_name}")
            logging.getLogger('werkzeug').info("---")

            # Check if file already exists in the database
            existing_file = db.query(models.File).filter_by(location=location).first()
            if not existing_file:
                new_file = models.File(
                    location=location,
                    high_school_type=high_school_type,
                    uc_campus_id=uc_campus.id if uc_campus else None,
                    category=category,
                    year=year
                )
                db.add(new_file)
    
    db.commit()

def get_high_school_by_name_and_city(db: Session, school_name: str, city: str):
    # First, try to find schools by name (case insensitive)
    schools = db.query(models.HighSchool).filter(
        models.HighSchool.school_name.ilike(f"%{school_name}%")
    ).all()
    
    # If city is empty or None, return all matching schools
    if not city:
        return schools
    
    # If only one school is found, return it
    if len(schools) == 1:
        return schools[0]
    
    # If multiple schools are found, use city to find a match (case insensitive)
    elif len(schools) > 1:
        return next((school for school in schools if school.city.lower() == city.lower()), None)
    
    # If no schools are found, return None
    return None

def create_high_school(db: Session, school_data: dict):
    db_school = models.HighSchool(**school_data)
    db.add(db_school)
    db.flush()  # This assigns an id to db_school without committing the transaction
    return db_school

def create_uc_admission_ethnicity(db: Session, ethnicity_data: dict):
    db_ethnicity = models.UCAdmissionEthnicity(**ethnicity_data)
    db.add(db_ethnicity)
    db.flush()
    return db_ethnicity

def get_high_school_by_uc_school_name(db: Session, uc_school_name: str):
    return db.query(models.HighSchool).filter(models.HighSchool.uc_school_name == uc_school_name).first()

def bulk_create_uc_admission_ethnicity(db: Session, ethnicity_data_list: list):
    db.bulk_insert_mappings(models.UCAdmissionEthnicity, ethnicity_data_list)
    db.commit()

def bulk_create_uc_admission_gender(db: Session, gender_data_list: list):
    db.bulk_insert_mappings(models.UCAdmissionGender, gender_data_list)
    db.commit()

def bulk_create_uc_admission_gpa(db: Session, gpa_data_list: list):
    db.bulk_insert_mappings(models.UCAdmissionGPA, gpa_data_list)
    db.commit()


def get_files_as_dataframe(db: Session):
    return pd.read_sql(select(models.File), db.bind)

def get_high_school_data(db: Session, school_name: str, city: str):
    # First, get the high school
    high_school = get_high_school_by_name_and_city(db, school_name, city)
    
    if not high_school:
        return None  # High school not found
    
    # Query data from all three tables
    ethnicity_data = db.query(models.UCAdmissionEthnicity).filter(
        models.UCAdmissionEthnicity.high_school_id == high_school.id
    ).all()
    
    gender_data = db.query(models.UCAdmissionGender).filter(
        models.UCAdmissionGender.high_school_id == high_school.id
    ).all()
    
    gpa_data = db.query(models.UCAdmissionGPA).filter(
        models.UCAdmissionGPA.high_school_id == high_school.id
    ).all()
    


    # Prepare the result dictionary
    result = {
        "high_school": {
            "id": high_school.id,
            "uc_school_name": high_school.uc_school_name,
            "school_name": high_school.school_name,
            "city": high_school.city,
            "county": high_school.county,
            "state": high_school.state,
            "country": high_school.country,
            "is_public": high_school.is_public
        },
        "ethnicity_data": [],
        "gender_data": [],
        "gpa_data": []
    }
    
    # Add ethnicity data
    for entry in ethnicity_data:
        result["ethnicity_data"].append({
            "uc_campus_id": entry.uc_campus_id,
            "academic_year": entry.academic_year,
            "admission_type": entry.admission_type,
            "ethnicity": entry.ethnicity,
            "count": entry.count
        })
    
    # Add gender data
    for entry in gender_data:
        result["gender_data"].append({
            "uc_campus_id": entry.uc_campus_id,
            "academic_year": entry.academic_year,
            "admission_type": entry.admission_type,
            "total_applicants": entry.total_applicants,
            "female_applicants": entry.female_applicants,
            "male_applicants": entry.male_applicants,
            "other_applicants": entry.other_applicants,
            "unknown_gender": entry.unknown_gender
        })
    
    # Add GPA data
    for entry in gpa_data:
        result["gpa_data"].append({
            "uc_campus_id": entry.uc_campus_id,
            "academic_year": entry.academic_year,
            "admission_type": entry.admission_type,
            "mean_gpa": entry.mean_gpa
        })




    # Generate nested dictionary
    nested_result = {
        "high_school": result["high_school"]
    }

    # Get unique UC campus IDs
    uc_campus_ids = set([entry["uc_campus_id"] for entry in result["ethnicity_data"] + result["gender_data"] + result["gpa_data"]])

    # Create nested structure for each UC campus
    for campus_id in uc_campus_ids:
        campus_name = db.query(models.UCCampus).filter_by(id=campus_id).first().campus_name
        nested_result[campus_name] = {
            "App": {},
            "Adm": {},
            "Enr": {}
        }

        # Add ethnicity data
        for entry in result["ethnicity_data"]:
            if entry["uc_campus_id"] == campus_id:
                nested_result[campus_name][entry["admission_type"]][entry["ethnicity"]] = entry["count"]

        # Add gender data
        for entry in result["gender_data"]:
            if entry["uc_campus_id"] == campus_id:
                nested_result[campus_name][entry["admission_type"]].update({
                    "total_applicants": entry["total_applicants"] if entry["total_applicants"] is not None else None,
                    "male_applicants": entry["male_applicants"] if entry["male_applicants"] is not None else None,
                    "female_applicants": entry["female_applicants"] if entry["female_applicants"] is not None else None,
                    "other_applicants": entry["other_applicants"] if entry["other_applicants"] is not None else None,
                    "unknown_gender": entry["unknown_gender"] if entry["unknown_gender"] is not None else None
                })

        # Add GPA data
        for entry in result["gpa_data"]:
            if entry["uc_campus_id"] == campus_id:
                nested_result[campus_name][entry["admission_type"]]["Mean GPA"] = entry["mean_gpa"]

    return nested_result




