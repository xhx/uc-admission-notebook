from flask import Blueprint, render_template, request, redirect, url_for
from sql_db.database import get_db
from sql_db import crud

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