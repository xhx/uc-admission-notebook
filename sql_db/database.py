import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

database_url = os.getenv('DATABASE_URL')

print("Environment variables:")
for key, value in os.environ.items():
    print(f"{key}: {value}")

if database_url is None:
    raise ValueError("DATABASE_URL environment variable is not set")

print(f"Database URL: {database_url}")  # For debugging

try:
    engine = create_engine(database_url, pool_pre_ping=True)
    print("Engine created successfully")
except Exception as e:
    print(f"Error creating engine: {str(e)}")
    raise

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()