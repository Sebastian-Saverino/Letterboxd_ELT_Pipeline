from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core import config

DATABASE_URL = (
    f"postgresql+psycopg2://{config.META_DB_USER}:{config.META_DB_PASSWORD}"
    f"@{config.META_DB_HOST}:{config.META_DB_PORT}/{config.META_DB_NAME}"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
