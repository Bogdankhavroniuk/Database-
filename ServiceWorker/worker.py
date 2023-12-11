
from sqlalchemy import create_engine, Column, Integer, Float, String, ForeignKey, MetaData
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import sessionmaker, relationship

import subprocess
import  psycopg2


Base = declarative_base()


class ZnoData(Base):
    __tablename__ = 'zno_data'
    outid = Column(String, primary_key=True)
    birth = Column(String)
    sextypename = Column(String)
    regname = Column(String)
    areaname = Column(String)
    tername = Column(String)
    regtypename = Column(String)
    tertypename = Column(String)
    classprofilename = Column(String)
    classlangname = Column(String)
    eoname = Column(String)
    eotypename = Column(String)
    eoregname = Column(String)
    eoareaname = Column(String)
    eotername = Column(String)
    eoparent = Column(String)
    umltest = Column(String)
    umlteststatus = Column(String)
    umlball100 = Column(String)
    umlball12 = Column(String)
    umlball = Column(String)
    umladaptscale = Column(String)
    umlptname = Column(String)
    umlptregname = Column(String)
    umlptareaname = Column(String)
    umlpttername = Column(String)
    ukrtest = Column(String)
    ukrsubtest = Column(String)
    ukrteststatus = Column(String)
    ukrball100 = Column(String)
    ukrball12 = Column(String)
    ukrball = Column(String)
    ukradaptscale = Column(String)
    ukrptname = Column(String)
    ukrptregname = Column(String)
    ukrptareaname = Column(String)
    ukrpttername = Column(String)
    histtest = Column(String)
    histlang = Column(String)
    histteststatus = Column(String)
    histball100 = Column(String)
    histball12 = Column(String)
    histball = Column(String)
    histptname = Column(String)
    histptregname = Column(String)
    histptareaname = Column(String)
    histpttername = Column(String)
    mathtest = Column(String)
    mathlang = Column(String)
    mathteststatus = Column(String)
    mathball100 = Column(String)
    mathball12 = Column(String)
    mathdpalevel = Column(String)
    mathball = Column(String)
    mathptname = Column(String)
    mathptregname = Column(String)
    mathptareaname = Column(String)
    mathpttername = Column(String)
    physball100 = Column(String)
    chemball100 = Column(String)
    bioball100 = Column(String)
    geoball100 = Column(String)
    fraball100 = Column(String)
    deuball100 = Column(String)
    spaball100 = Column(String)
    engball100 = Column(String)


# Define the Student class
class Student(Base):
    __tablename__ = 'student'
    outid = Column(String, ForeignKey('zno_data.outid'), primary_key=True)
    birth = Column(Integer)
    sextypename = Column(String)
    regtypename = Column(String)
    eotypename = Column(String)

    # Add relationship to Location table

    location = relationship('Location', back_populates='student', uselist=False)

    balls = relationship('StudentBalls', back_populates='student', uselist=False)


# Define the Location class
class Location(Base):
    __tablename__ = 'student_location'

    outid = Column(String, ForeignKey('zno_data.outid'), primary_key=True)
    student_outid = Column(String, ForeignKey('student.outid'), nullable=False)
    areaname = Column(String)
    tertypename = Column(String)
    tername = Column(String)

    # Add foreign key relationship to Student table
    student = relationship('Student', back_populates='location')


# Define the StudentBalls class
class StudentBalls(Base):
    __tablename__ = 'student_balls'
    outid = Column(String, ForeignKey('zno_data.outid'), primary_key=True)
    student_outid = Column(String, ForeignKey('student.outid'), nullable=False)
    ukrball100 = Column(Float)
    histball100 = Column(Float)
    mathball100 = Column(Float)
    physball100 = Column(Float)
    chemball100 = Column(Float)
    bioball100 = Column(Float)
    geoball100 = Column(Float)
    fraball100 = Column(Float)
    deuball100 = Column(Float)
    spaball100 = Column(Float)
    engball100 = Column(Float)

    student = relationship('Student', back_populates='balls')


def migrate_and_populate_data(old_db_info, new_db_info):
    old_engine = create_engine(old_db_info)
    new_engine = create_engine(new_db_info)

    SessionOld = sessionmaker(bind=old_engine)
    session_old = SessionOld()


    conn = psycopg2.connect("dbname=new_lr1_dat user=user1 password=123321 ")
    cur = conn.cursor()


    SessionNew = sessionmaker(bind=new_engine)
    session_new = SessionNew()

    drop_table_query = f"DROP TABLE IF EXISTS student_balls;"
    drop_table_query1 = f"DROP TABLE IF EXISTS student_location;"
    drop_table_query2 = f"DROP TABLE IF EXISTS student;"

    # Create table definitions
    create_table_query = """CREATE TABLE student (
        outid TEXT PRIMARY KEY,
        birth INTEGER,
        sextypename VARCHAR(255),
        regtypename TEXT,
        eotypename TEXT
    );"""

    create_table_query1 = """CREATE TABLE student_location (
        outid TEXT PRIMARY KEY,
        student_outid Text,
        areaname TEXT,
        tertypename VARCHAR(255),
        tername TEXT,
        FOREIGN KEY (student_outid) REFERENCES student(outid)
    );"""

    create_table_query2 = """CREATE TABLE student_balls (
        outid TEXT PRIMARY KEY,
        student_outid Text,
        ukrball100 Text,
        histball100 Text,
        mathball100 Text,
        physball100 Text,
        chemball100 Text,
        bioball100 Text,
        geoball100 Text,
        fraball100 Text,
        deuball100 Text,
        spaball100 Text,
        engball100 Text,
        FOREIGN KEY (student_outid) REFERENCES student(outid)
    );"""
    cur.execute(drop_table_query)
    cur.execute(drop_table_query1)
    cur.execute(drop_table_query2)
    cur.execute(create_table_query)
    cur.execute(create_table_query1)
    cur.execute(create_table_query2)
    conn.commit()
    # Insert data into the new tables

    zno_data_records =  session_old.query(ZnoData).all()


    # Migrate data to the new tables
    for zno_data in zno_data_records:
        # Migrate data to the Student table
        student_data = Student(
            outid=zno_data.outid,
            birth=zno_data.birth ,
            sextypename=zno_data.sextypename,
            regtypename=zno_data.regtypename,
            eotypename=zno_data.eotypename
        )
        session_new.add(student_data)

        # Migrate data to the Location table
        location_data = Location(
            outid=zno_data.outid,
            areaname=zno_data.areaname,
            tertypename=zno_data.tertypename,
            tername=zno_data.tername
        )
        session_new.add(location_data)


        student_balls_data = StudentBalls(

            outid=zno_data.outid,
            ukrball100=zno_data.ukrball100 ,
            histball100=zno_data.histball100,
            mathball100=zno_data.mathball100,
            physball100=zno_data.physball100 ,
            chemball100=zno_data.chemball100,
            bioball100=zno_data.bioball100,
            geoball100=zno_data.geoball100,
            fraball100=zno_data.fraball100,
            deuball100=zno_data.deuball100,
            spaball100=zno_data.spaball100,
            engball100=zno_data.engball100
        )
        session_new.add(student_balls_data)

    # Commit the changes to the database
    session_new.commit()
    session_new.commit()

    # Close sessions
    session_old.close()
    session_new.close()


def execute_command(command):
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"Command executed successfully:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e.stderr}")
