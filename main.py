import traceback
from datetime import datetime
import io
import os
import logging
from itertools import chain

import dotenv
import xlsxwriter

from utils.xlsx_formatter import CreateXlsx
from utils.yapp_data_api import YandexAppAPI
from database.models import Report, Application, GlobalCampaign, YdCampaign
from sqlalchemy import select

from database.db import session_maker

dotenv.load_dotenv()

logging.basicConfig(level=logging.INFO, format='[{asctime}] #{levelname:4} {name}:{lineno} - {message}', style='{')
logger = logging.getLogger('main.py')


def get_campaigns_ids(new_report: Report, by_groups):
    with session_maker() as session:
        global_campaign_obj: GlobalCampaign = session.execute(
            select(GlobalCampaign).where(GlobalCampaign.id == new_report.global_campaign_id)).scalar()

        yd_groups = {}
        groups = global_campaign_obj.groups
        for group in groups:
            if group.name not in yd_groups:
                yd_groups[group.name] = []
            for yd_camp in group.yd_campaigns:
                yd_groups[group.name].append(yd_camp.yd_campaign_id)
        # if by_groups:
        #     return yd_groups
        # return list(chain(*yd_groups.values()))

def get_campaigns_ids_by_group():
    with session_maker() as session:
        global_campaign_obj: GlobalCampaign = session.execute(
            select(GlobalCampaign).where(GlobalCampaign.id == new_report.global_campaign_id)).scalar()

        yd_groups = {}
        groups = global_campaign_obj.groups
        for group in groups:
            if group.name not in yd_groups:
                yd_groups[group.name] = []
            for yd_camp in group.yd_campaigns:
                yd_groups[group.name].append(yd_camp.yd_campaign_id)
        # if by_groups:
        #     return yd_groups
        # return list(chain(*yd_groups.values()))

# НЕТ ФИЛЬТРАЦИИ ПО КАМПАНИЯМ ГЛОБАЛЬНОЙ КАМПАНИИ


def create_report(app_id, date1, date2, campaigns, doc_header: str):
    """
    Метод для управления созданием отчёта
    :param app_id:
    :param date1:
    :param date2:
    :param campaigns:
    :param doc_header:
    :return:
    """
    token = os.getenv('yapp_token')
    api_req = YandexAppAPI(token, app_id, date1, date2, campaigns)
    general = api_req.get_all_campaigns()
    general_groups = api_req.get_campaign_groups(general)
    week_distribution = api_req.get_week_distribution()
    retention = api_req.get_retention_by_weeks()
    events = api_req.get_events()
    installs_info = api_req.get_installs_info()

    # создание файла в оперативной памяти
    with io.BytesIO() as file:
        workbook = xlsxwriter.Workbook(file, options={'in_memory': True})
        workbook.filename = 'yapp_report.xlsx'

    # форматирование даты в ру-формат для вставки в заголовок листа
    form_d1 = datetime.strptime(date1, '%Y-%m-%d').date().strftime('%d.%m.%Y')
    form_d2 = datetime.strptime(date2, '%Y-%m-%d').date().strftime('%d.%m.%Y')

    # формирование листов
    xlsx_form = CreateXlsx(workbook, f'{doc_header} {form_d1} - {form_d2}')
    xlsx_form.write_general(general, sheet_name='Все кампании')
    xlsx_form.write_general(general_groups, sheet_name='Группы кампаний')
    xlsx_form.write_week_distribution(week_distribution)
    xlsx_form.write_retention_by_weeks(retention, general)
    xlsx_form.write_events(events)
    xlsx_form.write_installs_by_regions(installs_info)
    xlsx_form.write_installs_by_oc(installs_info)
    xlsx_form.write_installs_by_brand(installs_info)

    # закрытие и сохранение файла
    workbook.close()


CAMPAIGNS = [704011362, 704010325, 704011628, 704011482, 704011760, 704013108, 704010283, 704004623, 704002660,
             704002262, 704002942, 704004722, 704005046, 704001940]


# НЕОБХОДИМО ДОБАВИТЬ ВО ВСЕ ДАТАФРЕЙМЫ ПРОВЕРКУ НА ПОЛУЧАЕМЫЕ ИЗ МЕТРИКИ ДАННЫЕ, ЕСЛИ ИЗ МЕТРИКИ НИЧЕГО НЕ ВЕРНУЛОСЬ В DATAFRAME
# ДОЛЖНЫ БЫТЬ ВСЕ!!! КАМПАНИИ С 0 ПАРАМЕТРАМИ
# ДОБАВИТЬ СКРИПТ СТЁПЫ НА ОПРЕДЕЛЕНИЕ URL-ПАРАМЕТРА с campaign_id
# create_report('2777872', '2025-11-1', '2025-11-30', CAMPAIGNS, 'Отчёт - приложение "Узнай Москву"')
while True:
    with session_maker() as session:
        # statement
        stmt = select(Report).where(Report.status_id == 1).order_by(Report.created_at.asc()).limit(1)
        new_report: Report | None = session.execute(stmt).scalar()
        if new_report:
            try:
                # new_report.status_id = 2
                session.commit()

                app_id = session.execute(
                    select(Application.id).where(Application.id == new_report.application_id)).scalar()
                start_date = new_report.start_date
                end_date = new_report.end_date
                campaigns_pairs = []
                gl_c = session.execute(
                    select(GlobalCampaign).where(GlobalCampaign.id == new_report.global_campaign_id)).scalar()
                print(gl_c.groups)
                import pandas as pd
                data = [(yd_camp.yd_campaign_id, yd_camp.name, yd_camp.group.name) for campaign_group in gl_c.groups for yd_camp in campaign_group.yd_campaigns]
                d = pd.DataFrame(data, columns=['campaign_id', 'campaign_name', 'campaign_group'])
                groups = d.groupby('campaign_group')['campaign_id'].apply(list).to_dict()
                ids = d['campaign_id']
                print(ids)

            except Exception as err:
                new_report.status_id = 4
                new_report.error_msg = traceback.format_exc()
                session.commit()
        break
