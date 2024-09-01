from sqlalchemy import Column, Integer, String, Float, ForeignKey, Boolean, Enum
from sqlalchemy.orm import relationship
from .database import Base
import enum

class HighSchoolType(enum.Enum):
    CA_PUBLIC = "CA public"
    CA_PRIVATE = "CA private"
    NON_CA = "non-CA"
    FOREIGN = "Foreign"
    ALL = "All"

class Category(enum.Enum):
    GENDER = "Gender"
    ETHNICITY = "Ethnicity"
    GPA = "GPA"

class HighSchool(Base):
    __tablename__ = "high_schools"

    id = Column(Integer, primary_key=True, index=True)
    school_name = Column(String, index=True)
    city = Column(String)
    county = Column(String)
    state = Column(String)
    country = Column(String, default="United States")  # New column
    zip_code = Column(String)
    is_public = Column(Boolean, default=True)  # New column

    admissions = relationship("Admission", back_populates="high_school")
    ethnicities = relationship("Ethnicity", back_populates="high_school")
    gpas = relationship("GPA", back_populates="high_school")

class UCCampus(Base):
    __tablename__ = "uc_campuses"

    id = Column(Integer, primary_key=True, index=True)
    campus_name = Column(String, index=True)
    location = Column(String)

    admissions = relationship("Admission", back_populates="uc_campus")
    ethnicities = relationship("Ethnicity", back_populates="uc_campus")
    gpas = relationship("GPA", back_populates="uc_campus")
    files = relationship("File", back_populates="uc_campus")

class Admission(Base):
    __tablename__ = "admissions"

    id = Column(Integer, primary_key=True, index=True)
    high_school_id = Column(Integer, ForeignKey("high_schools.id"))
    uc_campus_id = Column(Integer, ForeignKey("uc_campuses.id"))
    admission_type = Column(String)
    year = Column(Integer)  # New column
    total_applicants = Column(Integer)
    female_applicants = Column(Integer)
    male_applicants = Column(Integer)
    other_applicants = Column(Integer)
    unknown_gender = Column(Integer)

    high_school = relationship("HighSchool", back_populates="admissions")
    uc_campus = relationship("UCCampus", back_populates="admissions")

class Ethnicity(Base):
    __tablename__ = "ethnicities"

    id = Column(Integer, primary_key=True, index=True)
    high_school_id = Column(Integer, ForeignKey("high_schools.id"))
    uc_campus_id = Column(Integer, ForeignKey("uc_campuses.id"))
    admission_type = Column(String)
    ethnicity = Column(String)
    count = Column(Integer)

    high_school = relationship("HighSchool", back_populates="ethnicities")
    uc_campus = relationship("UCCampus", back_populates="ethnicities")

class GPA(Base):
    __tablename__ = "gpas"

    id = Column(Integer, primary_key=True, index=True)
    high_school_id = Column(Integer, ForeignKey("high_schools.id"))
    uc_campus_id = Column(Integer, ForeignKey("uc_campuses.id"))
    admission_type = Column(String)
    mean_gpa = Column(Float)

    high_school = relationship("HighSchool", back_populates="gpas")
    uc_campus = relationship("UCCampus", back_populates="gpas")

class File(Base):
    __tablename__ = "files"

    id = Column(Integer, primary_key=True, index=True)
    location = Column(String, unique=True, index=True)
    high_school_type = Column(Enum(HighSchoolType))
    uc_campus_id = Column(Integer, ForeignKey("uc_campuses.id"))
    category = Column(Enum(Category))
    year = Column(Integer)

    uc_campus = relationship("UCCampus", back_populates="files")