from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class RunLog(Base):
    """
    A table to store a log of program executions.
    """

    __tablename__ = 'runlog'

    id = Column(Integer, primary_key=True)
    debug = Column(Boolean)
    run_once = Column(Boolean)
    time_start = Column(DateTime)
    time_end = Column(DateTime)
    ip_start = Column(String)
    ip_end = Column(String)
    num_results = Column(Integer)
    exit_code = Column(Integer)
    status_message = Column(String)

class Listing(Base):
    """
    A table to store data on craigslist listings.
    """

    __tablename__ = 'listings'

    id = Column(Integer, primary_key=True)
    link = Column(String, unique=True)
    created = Column(DateTime)
    geotag = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    name = Column(String)
    price = Column(Float)
    location = Column(String)
    cl_id = Column(Integer, unique=True)
    area = Column(String)
    bart_stop = Column(String)
    bedrooms = Column(Integer)

engine = create_engine('sqlite:///listings.db', echo=False)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

def query_cl_id(id):
    return session.query(Listing).filter_by(cl_id=id).first()

def query_last_run():
    # Find out how many results were gained last run.
    results = session.query(RunLog).all()
    if results:
        return results[-1].num_results
    else:
        # runlog table is empty (probably your first run).
        return 1

def add(record):
    session.add(record)
    session.commit()
