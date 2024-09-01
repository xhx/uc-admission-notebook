from flask import Blueprint, render_template, request, redirect, url_for, jsonify, current_app
from sql_db.database import get_db
from sql_db import crud
import os
import sys
from sql_db.database import SessionLocal
import logging



# # Add the parent directory to sys.path to allow importing from sibling directories
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# # Import the function from add-file-to-db.py
# from app.handle_files.add_file_to_db import process_files

main = Blueprint('main', __name__)

@main.route('/', methods=['GET', 'POST'])
def index():
    welcome_message = "Welcome to the High School Data Analysis Tool!"
    instructions = [
        "Upload an Excel file (.xlsx) containing high school data.",
        "After uploading, you'll be redirected to the query page.",
        "On the query page, you can analyze the uploaded data."
    ]
    if request.method == 'POST':
        if 'file' not in request.files:
            return redirect(request.url)
        file = request.files['file']
        if file.filename == '':
            return redirect(request.url)
        if file and file.filename.endswith('.xlsx'):
            db = next(get_db())
            crud.process_excel_file(db, file)
            return redirect(url_for('main.query'))
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