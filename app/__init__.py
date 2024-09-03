from dotenv import load_dotenv
import os
from sql_db.database import engine, SessionLocal
from sql_db import models
from sql_db.crud import seed_uc_campuses
from sqlalchemy import inspect, select
from sqlalchemy.orm import Session

load_dotenv()

def create_app():
    # Check if all tables exist and create them if they don't
    inspector = inspect(engine)
    tables = models.Base.metadata.tables.keys()
    
    tables_to_create = [table for table in tables if not inspector.has_table(table)]
    
    if tables_to_create:
        models.Base.metadata.create_all(bind=engine)
    
    # Check if uc_campuses table is empty and seed if necessary
    with SessionLocal() as db:
        uc_campuses_count = db.scalar(select(models.UCCampus).limit(1))
        if uc_campuses_count is None:
            seed_uc_campuses(db)

