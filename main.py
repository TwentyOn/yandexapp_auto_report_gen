import string
import time
import traceback
from datetime import datetime, date
import io
import logging
import xlsxwriter

from utils.xlsx_formatter import CreateXlsx
from integrations.yapp_data_api import YandexAppAPI
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
    Функция для управления созданием отчёта
    :param app_id:
    :param date1:
    :param date2:
    :param campaigns_data:
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


def get_request(session: Session) -> Report | None:
    """
    Функция для поиска нового запроса в БД
    :param session:
    :return:
    """
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

    new_report_obj: Report | None = session.execute(stmt).scalar()

    return new_report_obj


def initial_report_generation(session: Session, request: Report) -> tuple[bytes, str]:
    """
    Функция для сбора, обработки и передачи параметров, необходимых для создания отчёта в функцию создания отчёта
    :param session:
    :param request:
    :return:
    """
    logger.info(f'Начинаю обработку запроса от {request.created_at}...')

    request.status_id = 2
    session.commit()

    # данные приложения Yandex App
    app_id = request.application.yandex_app_id
    app_name: str = request.application.name

    start_date: date = request.start_date
    end_date: date = request.end_date

    # ru-формат записи даты
    date_format = '%d-%m-%Y'
    start_date_ru = start_date.strftime(date_format)
    end_date_ru = end_date.strftime(date_format)

    # данные кампаний ЯД для полученой глобальной кампании
    # список кортежей: (campaign_id, campaign_name, campaign_group), ...
    campaigns_data = [(yd_camp.yd_campaign_id, yd_camp.name, yd_camp.group.name) for campaign_group in
                      request.global_campaign.groups for yd_camp in campaign_group.yd_campaigns]

    # заголовок для листов в отчёте
    header = (f'Отчёт по приложению "{app_name}" {start_date_ru.replace("-", ".")}-'
              f'{end_date_ru.replace("-", ".")}')

    # инициализация формирования отчёта
    new_report_file: bytes = create_report(
        app_id, str(start_date), str(end_date), campaigns_data, header)

    logger.info('Обработка завершена.')

    return new_report_file, header


def upload_report_to_s3(file: bytes, report_name: str) -> str:
    """
    Загрузка файла в хранилище S3
    :param file:
    :param report_name:
    :return:
    """
    logger.info('Загрузка отчёта в S3-хранилище...')

    suffix = str(int(datetime.today().timestamp()))

    filename = report_name.replace(string.punctuation, '')
    filename = filename.replace(' ', '_').replace('.', '-').replace('"', '')
    filename = filename + '_' + suffix + '.xlsx'

    filepath = '/'.join((S3_PATH, filename))
    storage.upload_memory_file(filepath, io.BytesIO(file), len(file))

    logger.info('Успешно.')
    return filepath


# ДОБАВИТЬ СКРИПТ СТЁПЫ НА ОПРЕДЕЛЕНИЕ URL-ПАРАМЕТРА с campaign_id

# бесконечный цикл ожидания нового отчёта
while True:
    to_sleep = True
    try:
        with session_maker() as session:
            new_request = get_request(session)

            if new_request:
                to_sleep = False
                try:
                    # формирование файла
                    new_report_file, report_name = initial_report_generation(session, new_request)

                    # загрузка файла в хранилище
                    path_to_file = upload_report_to_s3(new_report_file, report_name)

                    new_request.status_id = 3
                    new_request.s3_filepath = path_to_file

                    if new_request.error_msg:
                        new_request.error_msg = None

                    session.commit()

                except Exception as err:
                    new_request.status_id = 4
                    new_request.error_msg = traceback.format_exc()
                    session.commit()
                    raise err

        if to_sleep:
            logger.info('Сплю')
            time.sleep(30)

    except OperationalError as err:
        logger.error('Ошибка БД, переподключение через 10 секунд...')
        time.sleep(10)

    except Exception as err:
        logger.info('Произошла ошибка! Повторная попытка через 30 секунд...')
        time.sleep(30)
