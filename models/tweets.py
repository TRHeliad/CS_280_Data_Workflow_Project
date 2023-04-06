from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float

Base = declarative_base()

class Tweet(Base):
    __tablename__ = "tweets"
    id = Column(Integer, primary_key=True, nullable=False)
    tweet_id = Column(String, nullable=False)
    user_id = Column(String, nullable=False)
    text = Column(String, nullable=False)
    created_at = Column(Date, nullable=False)
    
    def __repr__(self) -> str:
        return (
            f"Tweet(id={self.id}, "
            f"tweet_id={self.tweet_id}, "
            f"user_id={self.user_id}, "
            f"text={self.text}, "
            f"created_at={self.created_at})"
        )