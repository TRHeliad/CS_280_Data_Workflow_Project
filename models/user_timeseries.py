from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float

Base = declarative_base()

class User_Timeseries(Base):
    __tablename__ = "user_timeseries"
    id = Column(Integer, primary_key=True, nullable=False, unique=True, autoincrement="auto")
    user_id = Column(String, nullable=False)
    followers_count = Column(Integer, nullable=False)
    following_count = Column(Integer, nullable=False)
    tweet_count = Column(Integer, nullable=False)
    listed_count = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)
    
    def __repr__(self) -> str:
        return (
            f"User_Timeseries(id={self.id}, "
            f"user_id={self.user_id}, "
            f"followers_count={self.followers_count}, "
            f"following_count={self.following_count}, "
            f"tweet_count={self.tweet_count}, "
            f"listed_count={self.listed_count}, "
            f"date={self.date})"
        )