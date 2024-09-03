from sql_db.database import SessionLocal
from sql_db import crud, models
import os
import sys
from sql_db.database import SessionLocal
import logging
from sql_db.process_csv_file import add_ethnicity_csv_file_to_db, add_gender_csv_file_to_db
from sql_db.crud import get_files_as_dataframe

def handle_files():
    welcome_message = "Welcome! We are now processing files to add them to the database."
    
    # Use print for immediate console output
    print(welcome_message)
    
    # Use only logging
    logging.getLogger('werkzeug').info(welcome_message)

    try:
        db = SessionLocal()
        full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'handle-files', 'files')
        crud.add_files_to_db(db, full_path)
        
        success_message = "Files processed successfully."
        print(success_message)
        logging.info(success_message)
        
        return {'status': 'success', 'message': success_message, 'welcome_message': welcome_message}
    except Exception as e:
        error_message = f'Error processing files: {str(e)}'
        print(error_message)
        logging.error(error_message)
        return {'status': 'error', 'message': error_message}

def process_files():
    try:
        db = SessionLocal()
        files = db.query(models.File).all()
        
        file_data = []
        for file in files:
            file_info = {
                'id': file.id,
                'location': file.location,
                'high_school_type': file.high_school_type.value if file.high_school_type else None,
                'uc_campus': file.uc_campus.campus_name if file.uc_campus else None,
                'category': file.category.value if file.category else None,
                'year': file.year,
                'is_added_to_db': file.is_added_to_db
            }
            file_data.append(file_info)
        
        return {'files': file_data}
    except Exception as e:
        error_message = f'Error retrieving files: {str(e)}'
        logging.error(error_message)
        return {'error': error_message}
    finally:
        db.close()

def process_single_file(file_location, uc_campus, category, year, high_school_type):
    try:
        db = SessionLocal()
        logging.info(f"Processing file: {file_location}, {uc_campus}, {category}, {year}, {high_school_type}")

        # Process the single file
        if category.upper() == 'ETHNICITY':
            success = add_ethnicity_csv_file_to_db(db, file_location, uc_campus, year, high_school_type)
        elif category.upper() == 'GENDER':
            success = add_gender_csv_file_to_db(db, file_location, uc_campus, year)
        else:   
            # Handle other categories if needed
            success = False
            logging.error(f"Unsupported category: {category}")

        if success:
            # Update the file's is_added_to_db status
            file = db.query(models.File).filter_by(location=file_location).first()
            if file:
                file.is_added_to_db = True
                db.commit()

        response = {
            'input_parameters': {
                'file_location': file_location,
                'uc_campus': uc_campus,
                'category': category,
                'year': year,
                'high_school_type': high_school_type
            },
            'success': success
        }

        return response

    except Exception as e:
        error_message = f'Error processing file: {str(e)}'
        logging.error(error_message)
        return {'error': error_message}
    finally:
        db.close()

def get_files_dataframe():
    try:
        db = SessionLocal()
        df = get_files_as_dataframe(db)
        return df.to_dict(orient='records')  # Convert DataFrame to list of dictionaries for JSON serialization
    except Exception as e:
        error_message = f'Error retrieving files as DataFrame: {str(e)}'
        logging.error(error_message)
        return {'error': error_message}
    finally:
        db.close()


