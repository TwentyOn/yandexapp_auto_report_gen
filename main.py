import contextlib
import time
import traceback
from datetime import datetime, date
import io
import logging
import xlsxwriter

from utils.xlsx_formatter import CreateXlsx
from utils.yapp_data_api import YandexAppAPI
from utils.s3_storage import storage
from database.models import Report, GlobalCampaign, CampaignGroup
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import selectinload, Session

from database.db import session_maker
from settings import YAPP_TOKEN

logging.basicConfig(level=logging.INFO, format='[{asctime}] #{levelname:4} {name}:{lineno} - {message}', style='{')
logger = logging.getLogger('main.py')

S3_PATH = 'yandexapp_report_generator'


def create_report(app_id, date1, date2, campaigns_data, doc_header: str) -> bytes:
    """
    Метод для управления созданием отчёта
    :param app_id:
    :param date1:
    :param date2:
    :param campaigns:
    :param doc_header:
    :return:
    """
    api_req = YandexAppAPI(YAPP_TOKEN, app_id, date1, date2, campaigns_data)
    general = api_req.get_all_campaigns()
    general_groups = api_req.get_campaign_groups(general)
    week_distribution = api_req.get_week_distribution()
    retention = api_req.get_retention_by_weeks()
    events = api_req.get_events()
    installs_info = api_req.get_installs_info()

    # создание файла в оперативной памяти
    with io.BytesIO() as xlsx_file:
        workbook = xlsxwriter.Workbook(xlsx_file, options={'in_memory': True})
        # раскоментировать, если нужно сохрание в файловой системе
        # workbook.filename = 'yapp_report.xlsx'

        # формирование листов
        xlsx_form = CreateXlsx(workbook, doc_header)
        xlsx_form.write_general(general, sheet_name='Все кампании')
        xlsx_form.write_general(general_groups, sheet_name='Группы кампаний')
        xlsx_form.write_week_distribution(week_distribution)
        xlsx_form.write_retention_by_weeks(retention, general)
        xlsx_form.write_events(events)
        xlsx_form.write_installs_by_regions(installs_info)
        xlsx_form.write_installs_by_oc(installs_info)
        xlsx_form.write_installs_by_brand(installs_info)

        # закрытие и сохранение документа
        workbook.close()

        xlsx_file.seek(0)
        xlsx_file = xlsx_file.getvalue()
    return xlsx_file


# ДОБАВИТЬ СКРИПТ СТЁПЫ НА ОПРЕДЕЛЕНИЕ URL-ПАРАМЕТРА с campaign_id

# бесконечный цикл ожидания нового отчёта
while True:
    try:
        with session_maker() as session:
            # statement
            stmt = (
                select(Report)
                .where(Report.status_id == 1, Report.to_delete == False)
                .order_by(Report.created_at.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
                .options(
                    selectinload(Report.application),
                    selectinload(Report.global_campaign)
                    .selectinload(GlobalCampaign.groups)
                    .selectinload(CampaignGroup.yd_campaigns)
                )
            )
            # объект Report из БД
            new_report_obj: Report | None = session.execute(stmt).scalar()

            if new_report_obj:
                try:
                    logger.info(f'Новый отчёт от {new_report_obj.created_at}')
                    new_report_obj.status_id = 2
                    session.commit()

                    app_id = new_report_obj.application.yandex_app_id
                    app_name: str = new_report_obj.application.name

                    # ru-формат записи даты
                    date_format = '%d/%m/%Y'
                    start_date: date = new_report_obj.start_date
                    end_date: date = new_report_obj.end_date

                    # данные кампаний ЯД для полученой глобальной кампании
                    # список кортежей: (campaign_id, campaign_name, campaign_group), ...
                    campaigns_data = [(yd_camp.yd_campaign_id, yd_camp.name, yd_camp.group.name) for campaign_group in
                                      new_report_obj.global_campaign.groups for yd_camp in campaign_group.yd_campaigns]

                    start_date_ru = start_date.strftime(date_format)
                    end_date_ru = end_date.strftime(date_format)
                    report_name = f'Отчёт по приложению "{app_name}" {start_date_ru} - {end_date_ru}'

                    new_report_file: bytes = create_report(
                        app_id, str(start_date), str(end_date), campaigns_data, report_name)

                    logger.info(f'Завершено формирование отчёта от {new_report_obj.created_at}')

                    logger.info('Загрузка отчёта в S3-хранилище...')
                    suffix = int(datetime.today().timestamp())
                    filename = (f'Отчёт_приложение_{app_name.replace(" ", "_")}_'
                                f'{start_date_ru}_{end_date_ru}_{suffix}.xlsx')
                    filepath = '/'.join((S3_PATH, filename))
                    storage.upload_memory_file(filepath, io.BytesIO(new_report_file), len(new_report_file))

                    new_report_obj.status_id = 3
                    new_report_obj.s3_filepath = filepath
                    if new_report_obj.error_msg:
                        new_report_obj.error_msg = None
                    session.commit()

                    logger.info('Успех.')

                except Exception as err:
                    new_report_obj.status_id = 4
                    new_report_obj.error_msg = traceback.format_exc()
                    session.commit()
                    raise err

            else:
                logger.info('Нет новых запросов на создание отчёта, жду 30 секунд...')
                session.close()
                time.sleep(30)

    except OperationalError as err:
        logger.error('Ошибка БД, переподключение через 10 секунд...')
        session.close()
        time.sleep(10)

    except Exception as err:
        logger.info('Произошла ошибка!')
        time.sleep(30)