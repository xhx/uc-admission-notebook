from flask import Blueprint, render_template, request, redirect, url_for, jsonify, current_app
from sql_db.database import get_db
from sql_db import crud, models
import os
import sys
from sql_db.database import SessionLocal
import logging
from sql_db.process_csv_file import add_ethnicity_csv_file_to_db, add_gender_csv_file_to_db


# # Add the parent directory to sys.path to allow importing from sibling directories
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# # Import the function from add-file-to-db.py
# from app.handle_files.add_file_to_db import process_files

main = Blueprint('main', __name__)

@main.route('/', methods=['GET'])
def index():
    welcome_message = "Welcome to the High School Data Analysis Tool!"
    instructions = [
        "Use the 'Process Files' link to view all files in the database.",
        "Use the 'Handle Files' link to add new files to the database.",
        "After processing, you can analyze the uploaded data."
    ]
    return render_template('index.html', welcome_message=welcome_message, instructions=instructions)

@main.route('/query', methods=['GET', 'POST'])
def query():
    result = None
    if request.method == 'POST':
        query_type = request.form.get('query_type')
        parameters = request.form.to_dict()
        db = next(get_db())
        result = crud.query_database(db, query_type, parameters)
    return render_template('query.html', result=result)

@main.route('/handle-files', methods=['GET'])
def handle_files():
    welcome_message = "Welcome! We are now processing files to add them to the database."
    
    # Use print for immediate console output
    print(welcome_message)
    
    # Use both app.logger and current_app.logger
    current_app.logger.info(welcome_message)
    logging.getLogger('werkzeug').info(welcome_message)

    try:
        db = SessionLocal()
        full_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'handle-files', 'files')
        crud.add_files_to_db(db, full_path)
        
        success_message = "Files processed successfully."
        print(success_message)
        current_app.logger.info(success_message)
        logging.getLogger('werkzeug').info(success_message)
        
        return render_template('handle_files.html', status='success', message=success_message, welcome_message=welcome_message)
    except Exception as e:
        error_message = f'Error processing files: {str(e)}'
        print(error_message)
        current_app.logger.error(error_message)
        logging.getLogger('werkzeug').error(error_message)
        
        return render_template('handle_files.html', status='error', message=error_message, welcome_message=welcome_message)

@main.route('/process-files', methods=['GET', 'POST'])
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
        
        return render_template('process_files.html', files=file_data)
    except Exception as e:
        error_message = f'Error retrieving files: {str(e)}'
        current_app.logger.error(error_message)
        return render_template('process_files.html', error=error_message)
    finally:
        db.close()

@main.route('/process-single-file', methods=['POST'])
def process_single_file():
    try:
        db = SessionLocal()
        data = request.json
        file_location = data.get('file_location')
        uc_campus = data.get('uc_campus')
        category = data.get('category')
        year = data.get('year')
        high_school_type = data.get('high_school_type')

        logging.getLogger('werkzeug').info(f"Processing file: {file_location}, {uc_campus}, {category}, {year}, {high_school_type}")

        # Process the single file
        if category.upper() == 'ETHNICITY':
            success = add_ethnicity_csv_file_to_db(db, file_location, uc_campus, year, high_school_type)
        elif category.upper() == 'GENDER':
            success = add_gender_csv_file_to_db(db, file_location, uc_campus, year)
        else:   
            # Handle other categories if needed
            success = False
            current_app.logger.error(f"Unsupported category: {category}")

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

        return jsonify(response)

    except Exception as e:
        error_message = f'Error processing file: {str(e)}'
        current_app.logger.error(error_message)
        return jsonify({'error': error_message}), 400
    finally:
        db.close()


