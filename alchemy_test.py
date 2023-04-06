from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.users import User
import logging

logging.basicConfig(level=logging.INFO)

def create_dburl():
  hostname = "0.0.0.0"
  username = "postgres"
  password = "abc123"
  port = 5432
  database = "twitter"
  return f"postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{database}"

engine = create_engine(create_dburl())
Session = sessionmaker(bind=engine)

session = Session()

allUsersData = session.query(User).all()
logging.info(allUsersData)

session.close()