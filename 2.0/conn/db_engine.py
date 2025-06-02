from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config.settings import SQL_DATABASE_URL

engine = create_engine(SQL_DATABASE_URL) #echo=True FOR DEBUG ONLY
Session = sessionmaker(bind=engine)