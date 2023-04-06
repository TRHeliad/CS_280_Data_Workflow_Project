from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float

Base = declarative_base()

class Tweet_Timeseries(Base):
    __tablename__ = "tweet_timeseries"
    id = Column(Integer, primary_key=True, nullable=False, unique=True, autoincrement="auto")
    tweet_id = Column(String, nullable=False)
    retweet_count = Column(Integer, nullable=False)
    like_count = Column(Integer, nullable=False)
    date = Column(Date, nullable=False)
    
    def __repr__(self) -> str:
        return (
            f"Tweet_Timeseries(id={self.id}, "
            f"tweet_id={self.tweet_id}, "
            f"retweet_count={self.retweet_count}, "
            f"like_count={self.like_count}, "
            f"date={self.date})"
        )