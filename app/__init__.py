from flask import Flask
from dotenv import load_dotenv
import os
from sql_db.database import engine, SessionLocal
from sql_db import models
from sql_db.crud import seed_uc_campuses
from sqlalchemy import inspect

load_dotenv()

def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'default_secret_key')
    
    # Check if tables exist and create them if they don't
    inspector = inspect(engine)
    if not inspector.has_table("high_schools"):
        models.Base.metadata.create_all(bind=engine)
        
        # Seed UC campuses after creating tables
        with SessionLocal() as db:
            seed_uc_campuses(db)
    
    
    from .routes import main
    app.register_blueprint(main)

    return app