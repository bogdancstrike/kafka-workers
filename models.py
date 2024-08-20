from sqlalchemy import Column, String, Integer, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class ConsumerConfig(Base):
    __tablename__ = 'consumer_configs'

    id = Column(Integer, primary_key=True)
    consumer_name = Column(String, unique=True, nullable=False)
    topics_input = Column(String, nullable=False)
    topics_output = Column(String, nullable=False)
    metadatas = Column(String)
    kafka_bootstrap_server = Column(String, nullable=False)  # New column


# Database connection setup
def get_engine():
    from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    return create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')


def create_session():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()


# Initialize the database (for creating tables, etc.)
def init_db():
    engine = get_engine()
    Base.metadata.create_all(engine)
