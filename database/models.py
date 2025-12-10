import dotenv
from sqlalchemy import Column, Integer, TEXT, ForeignKey, DATE, CHAR, DATETIME, Boolean
from sqlalchemy.orm import relationship

from .db import Base, scheme_name

class Report(Base):
    __tablename__ = 'report'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer)
    application_id = Column(Integer)
    global_campaign_id = Column(Integer, ForeignKey(f'{scheme_name}.global_campaign.id', ondelete='NO ACTION'))
    start_date = Column(DATE, nullable=False)
    end_date = Column(DATE, nullable=False)
    s3_filepath = Column(CHAR(1000), nullable=True)
    created_at = Column(DATETIME)
    to_delete = Column(Boolean)
    status_id = Column(Integer)
    error_msg = Column(TEXT)

class Application(Base):
    __tablename__ = 'application'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(TEXT)
    yandex_app_id = Column(TEXT)
    yd_login = Column(TEXT)
    created_at = Column(DATETIME)
    updated_at = Column(DATETIME)
    user_id = Column(Integer)



class GlobalCampaign(Base):
    __tablename__ = 'global_campaign'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(TEXT, nullable=False)
    start_date = Column(DATE, nullable=False)
    end_date = Column(DATE, nullable=False)

    groups = relationship('CampaignGroup', backref='global_campaign')


class CampaignGroup(Base):
    __tablename__ = 'campaign_group'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    global_campaign_id = Column(
        Integer, ForeignKey(f'{scheme_name}.global_campaign.id', ondelete='CASCADE'), nullable=False)
    name = Column(TEXT)

    yd_campaigns = relationship('YdCampaign', back_populates='group')


class YdCampaign(Base):
    __tablename__ = 'yd_campaign'

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    name = Column(TEXT)
    yd_campaign_id = Column(TEXT)
    group_id = Column(
        Integer, ForeignKey(f'{scheme_name}.campaign_group.id', ondelete='CASCADE'), nullable=False)

    group = relationship('CampaignGroup', back_populates='yd_campaigns', uselist=False)