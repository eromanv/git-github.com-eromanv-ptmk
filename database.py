from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def create_db_engine(username, password, database):
    connection_string = f'postgresql://{username}:{password}@localhost/{database}'
    return create_engine(connection_string, echo=True)

def create_db_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()
