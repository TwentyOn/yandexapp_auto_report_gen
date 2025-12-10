from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from settings import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME


engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
session_maker = sessionmaker(bind=engine)

scheme_name = 'yandexapp_stats'


class Base(DeclarativeBase):
    __table_args__ = {'schema': scheme_name}
