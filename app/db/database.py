"""SQLAlchemy engine/session. Connection string comes from the environment so the
same code runs locally and in Docker (DB host = service name 'postgres')."""
import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://admin:admin123@localhost:5432/jobqueue"
)

# pool_pre_ping recycles connections that the DB dropped while idle.
engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def init_db() -> None:
    """Create tables if they don't exist yet."""
    from app.db import models  # noqa: F401  (registers metadata)
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
