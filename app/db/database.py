from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,declarative_base

DATABASE_URL = "postgresql://admin:admin123@localhost:5432/jobqueue"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base =declarative_base()


def get_db():
    """
    Creates a database session, yields it for use,
    then closes it automatically when done.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
