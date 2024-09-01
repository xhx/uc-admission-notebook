from sql_db.database import SessionLocal
from sql_db import crud

db = SessionLocal()
crud.add_files_to_db(db, "/path/to/files/directory")
db.close()

