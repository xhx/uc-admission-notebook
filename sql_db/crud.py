from sqlalchemy.orm import Session
from . import models
import pandas as pd

def process_excel_file(db: Session, file):
    df = pd.read_excel(file)
    
    for _, row in df.iterrows():
        # Create or update high school
        high_school = create_or_update_high_school(db, {
            'school_name': row['School Name'],
            'city': row['City'],
            'county': row['County'],
            'state': row['State'],
            'country': row.get('Country', 'United States'),  # New field with default value
            'zip_code': row['Zip Code'],
            'is_public': row['Is Public']
        })
        
        # Process data for each UC campus
        for campus in ['Los Angeles', 'Berkeley', 'Davis', 'San Diego', 'Irvine', 'Santa Barbara', 'Riverside', 'Merced', 'Santa Cruz']:
            uc_campus = get_uc_campus_by_name(db, campus)
            
            for admission_type in ['App', 'Adm', 'Enr']:
                # Create admission record
                create_admission(db, {
                    'high_school_id': high_school.id,
                    'uc_campus_id': uc_campus.id,
                    'admission_type': admission_type,
                    'year': row['Year'],  # New field
                    'total_applicants': row[f'{campus} {admission_type}'],
                    'female_applicants': row[f'{campus} {admission_type} Female'],
                    'male_applicants': row[f'{campus} {admission_type} Male'],
                    'other_applicants': row[f'{campus} {admission_type} Other'],
                    'unknown_gender': row[f'{campus} {admission_type} Unknown']
                })
                
                # Create ethnicity records
                for ethnicity in ['African American', 'Asian', 'Hispanic', 'White', 'Other']:
                    create_ethnicity(db, {
                        'high_school_id': high_school.id,
                        'uc_campus_id': uc_campus.id,
                        'admission_type': admission_type,
                        'ethnicity': ethnicity,
                        'count': row[f'{campus} {admission_type} {ethnicity}']
                    })
                
                # Create GPA record
                create_gpa(db, {
                    'high_school_id': high_school.id,
                    'uc_campus_id': uc_campus.id,
                    'admission_type': admission_type,
                    'mean_gpa': row[f'{campus} {admission_type} GPA']
                })

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
    return db.query(models.UCCampus).filter(models.UCCampus.campus_name == campus_name).first()

def create_admission(db: Session, admission_data):
    admission = models.Admission(**admission_data)
    db.add(admission)
    db.commit()
    db.refresh(admission)
    return admission

def create_ethnicity(db: Session, ethnicity_data):
    ethnicity = models.Ethnicity(**ethnicity_data)
    db.add(ethnicity)
    db.commit()
    db.refresh(ethnicity)
    return ethnicity

def create_gpa(db: Session, gpa_data):
    gpa = models.GPA(**gpa_data)
    db.add(gpa)
    db.commit()
    db.refresh(gpa)
    return gpa

def query_database(db: Session, query_type, parameters):
    if query_type == 'admissions':
        return db.query(models.Admission).all()
    elif query_type == 'ethnicity':
        return db.query(models.Ethnicity).all()
    elif query_type == 'gpa':
        return db.query(models.GPA).all()
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
        {"campus_name": "Santa Cruz", "location": "Santa Cruz, CA"}
    ]

    for campus_data in campuses:
        existing_campus = db.query(models.UCCampus).filter_by(campus_name=campus_data["campus_name"]).first()
        if not existing_campus:
            new_campus = models.UCCampus(**campus_data)
            db.add(new_campus)
    
    db.commit()