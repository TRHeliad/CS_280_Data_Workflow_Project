from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Float

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, nullable=False)
    user_id = Column(Integer, nullable=False)
    username = Column(String, nullable=False)
    name = Column(String, nullable=False)
    created_at = Column(Date, nullable=False)
    
    def __repr__(self) -> str:
        return (
            f"User(id={self.id}, "
            f"user_id={self.user_id}, "
            f"username={self.username}, "
            f"name={self.name}, "
            f"created_at={self.created_at})"
        )