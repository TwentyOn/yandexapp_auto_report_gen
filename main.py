from datetime import datetime
import inspect
import io
import json
import os
import logging
from time import perf_counter

import requests
import dotenv
import pandas as pd
import numpy as np
import xlsxwriter

from xlsx_formatter import XlsxForm
from functools import wraps

dotenv.load_dotenv()

logging.basicConfig(level=logging.INFO, format='[{asctime}] #{levelname:4} {name}:{lineno} - {message}', style='{')
logger = logging.getLogger('main.py')


def status_decorator(func):
    """
    Декоратор для подсчёта времени на запрос и информировании об успешности запроса
    :param func:
    :return:
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = perf_counter()
        result = func(*args, **kwargs)
        if result.status_code == 200:
            logger.info(f'Запрос успешен! ({round(perf_counter() - start_time, 3)} cек)')
        else:
            logger.error('Ошибка запроса!')
            print(result.url)
        return result

    return wrapper


def fillna_decorator(func):
    """
    Декоратор для заполнения nan-значений на 0 в результирующих объектах DataFrame
    (Только для функций возвращающих объекты pd.DataFrame)
    :param func:
    :return:
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        result = result.fillna(0)
        return result

    return wrapper


class YandexAppAPI:
    def __init__(self, yapp_token, app_id=None, date1=None, date2=None, campaign_ids: list = None,
                 campaign_groups: list = None):
        self.yapp_token = yapp_token
        self.api_url = 'https://api.appmetrica.yandex.ru/stat/v1/data.csv'
        self.header = {'Authorization': yapp_token}
        self.app_id = app_id

        self.date1_repr = date1
        self.date2_repr = date2
        # перевод строкового значения даты в python-объект date для вычислений
        self.date1 = datetime.strptime(date1, '%Y-%m-%d').date()
        self.date2 = datetime.strptime(date2, '%Y-%m-%d').date()

        self.campaign_ids = campaign_ids
        self.campaign_group_ids = campaign_groups

    @fillna_decorator
    def get_all_campaigns(self) -> pd.DataFrame:
        """
        Данные (сбор и обработка) для листа "Все кампании"
        :return:
        """
        general_metrics = 'ym:ts:userClicks,ym:ts:advInstallDevices,ym:ts:clickToInstallConversion'
        general_dimensions = "ym:ts:urlParameter{'utm_campaign'}"

        session_metrics = 'ym:s:sessions,ym:s:totalSessionDurationPerUser'
        session_dimensions = "ym:s:profileUrlParameter{'utm_campaign'},ym:s:session"

        event_count_metrics = 'ym:ce2:allEvents'
        event_count_dimensions = "ym:ce2:profileUrlParameter{'utm_campaign'},ym:ce2:device,ym:ce2:eventLabel"

        general_labels = ['campaign_id', 'clicks', 'installs', 'conversion_clicks']
        session_labels = ['campaign_id', 'session_id', 'sessions', 'timespent']
        events_count_labels = ['campaign_id', 'device_id', 'event', 'events_count']

        # запрос данных из API AppMetrica
        logger.info('Запрос основных параметров.')
        request_general = self._make_request(general_metrics, general_dimensions, 'ym:ts:urlParameter')

        logger.info('Запрос количества сессий.')
        request_sessions = self._make_request(session_metrics, session_dimensions, 'ym:ts:urlParameter')

        logger.info('Запрос количества событий.')
        request_events_count = self._make_request(event_count_metrics, event_count_dimensions, 'ym:ts:urlParameter')

        # общие показатели кликов, установок, конверсии кликов
        general_df = pd.read_csv(io.StringIO(request_general.text))
        general_df.columns = general_labels
        general_df.conversion_clicks = general_df.conversion_clicks.apply(lambda x: round(x, 2))
        # имена кампаний из БД
        campaigns_name_df = pd.DataFrame(
            {'campaign_id': ['Итого и средние', '704011362', '704010325', '704011628', '704011482', '704011760',
                             '704013108', '704010283', '704004623', '704002660',
                             '704002262', '704002942', '704004722', '704005046', '704001940'],
             'campaign_name': ['Всего', 'Узнай Москву/РСЯ/Экскурсии, гиды/iOS (РФ)',
                               'Узнай Москву/РСЯ/Парки, усадьбы/iOS',
                               'Узнай Москву/РСЯ/Музеи по наименованиям/iOS',
                               'Узнай Москву/РСЯ/Достопримечательности, выставки, музеи/iOS (РФ)',
                               'Узнай Москву/РСЯ/Достопримечательности в AR/iOS',
                               'Узнай Москву/РСЯ/Двойники в AR/iOS',
                               'Узнай Москву/РСЯ/Выходные/iOS,',
                               'Узнай Москву/Поиск/Экспозиции, выставки, музеи/iOS',
                               'Узнай Москву/Поиск/Экскурсии/iOS (РФ)',
                               'Узнай Москву/Поиск/Парки, усадьбы/iOS',
                               'Узнай Москву/Поиск/Достопримечательности/iOS (РФ)',
                               'Узнай Москву/Поиск/Достопримечательности в AR/iOS',
                               'Узнай Москву/Поиск/Двойники в AR/iOS',
                               'Узнай Москву/Поиск/Выходные/IOS'
                               ]})
        # добавление имён кампаний
        general_df = general_df.merge(campaigns_name_df, on='campaign_id', how='left')
        general_labels.insert(1, 'campaign_name')
        general_df = general_df[general_labels]

        # датафрейм с общей информацией о событиях
        events_count_df = pd.read_csv(io.StringIO(request_events_count.text))
        events_count_df.columns = events_count_labels

        # общее кол-во событий
        total_events_df = events_count_df.copy()
        total_events_df = total_events_df.drop(columns=['event', 'device_id'])
        total_events_df = total_events_df.groupby('campaign_id').sum().reset_index()
        total_events_df = total_events_df.sort_values(by='events_count', ascending=False)

        # ТРЕБУЕТСЯ ПРОВЕРКА (нужно ли сравнивать id устройств событий с id устройств установок)
        # количество пользователей, установивших приложение и вошедших в него хотя бы 1 раз
        log_count_df = events_count_df.copy()
        log_count_df = log_count_df[log_count_df['event'] == 'Запуск приложения и отображение экрана заставки.']
        log_count_df = log_count_df.drop(columns=['device_id', 'event'])
        # любое количество входов > 0 считаем как 1 уникальный вход
        log_count_df['active_users'] = np.where(log_count_df['events_count'] > 0, 1, 0)
        log_count_df = log_count_df.drop(columns='events_count')
        log_count_df = log_count_df.groupby('campaign_id').sum().reset_index()
        log_count_df.loc[-1] = ['Итого и средние', log_count_df['active_users'].sum()]

        # количество сессии, время сессий
        sessions_df = pd.read_csv(io.StringIO(request_sessions.text)).fillna(0)
        sessions_df.columns = session_labels

        # Формирование результирующего датафрейма (со всеми параметрами)
        # добавление столбца с количеством новых пользователей
        general_df = general_df.merge(on='campaign_id', how='left', right=log_count_df)

        # добавление столбца с количеством сессий
        # отсутствующие в ответе API кампании заполняем как кампании с 0-м показателем сессий
        check = [str(i) for i in self.campaign_ids if str(i) not in sessions_df.campaign_id.values]
        if check:
            for campaign_id in check:
                sessions_df.loc[len(sessions_df)] = [campaign_id, 0, 0, 0]

        # общие показатели количества сессий
        sessions_count_df = sessions_df.drop(columns=['session_id', 'timespent']).groupby('campaign_id').sum()
        general_df = general_df.merge(on='campaign_id', how='left', right=sessions_count_df)

        # столбец с количеством сессий на 1 установку
        general_df['session_per_install'] = np.where((general_df['sessions'] != 0) & (general_df['installs'] != 0),
                                                     (general_df['sessions'] / general_df['installs']).round(2), 0)

        # добавление столбца с количеством событий
        general_df = general_df.merge(on='campaign_id', how='left', right=total_events_df).fillna(0)

        # столбец с количеством событий на 1 сессию
        general_df['events_per_session'] = np.where(general_df['sessions'] != 0,
                                                    (general_df['events_count'] / general_df['sessions']).round(2), 0)

        # среднее время сессий в секундах
        mean_session_time_df = sessions_df.drop(columns=['session_id', 'sessions']).groupby(
            'campaign_id').mean().round(0).rename(columns={'timespent': 'mean_timespent'}).reset_index()
        summary_mean_time_row = mean_session_time_df['campaign_id'] == 'Итого и средние'
        mean_sessions_time = mean_session_time_df.loc[~summary_mean_time_row, 'mean_timespent'].mean()
        mean_session_time_df.loc[summary_mean_time_row, 'mean_timespent'] = mean_sessions_time
        general_df = general_df.merge(on='campaign_id', how='left', right=mean_session_time_df)

        # медианное время сессии в секундах
        median_session_time_df = sessions_df.drop(columns=['session_id', 'sessions']).groupby(
            'campaign_id').median().round(0).rename(columns={'timespent': 'median_timespent'}).reset_index()
        summary_median_time_row = median_session_time_df['campaign_id'] == 'Итого и средние'
        median_sessions_time = median_session_time_df.loc[~summary_median_time_row, 'median_timespent'].median()
        median_session_time_df.loc[summary_median_time_row, 'median_timespent'] = median_sessions_time
        general_df = general_df.merge(on='campaign_id', how='left', right=median_session_time_df)

        # доля сессий продолжительностью меньше 10 секунд
        sessions_time_less_10_df = sessions_df.drop(columns=['session_id', 'sessions']).groupby('campaign_id').apply(
            lambda x: (x['timespent'] < 10).mean(), include_groups=False).reset_index(name='sessions_lt_10')
        summary_session_time_less_10 = sessions_time_less_10_df['campaign_id'] == 'Итого и средние'
        mean_perc_session_less_10 = sessions_time_less_10_df.loc[~summary_session_time_less_10, 'sessions_lt_10'].mean()
        sessions_time_less_10_df.loc[summary_session_time_less_10, 'sessions_lt_10'] = mean_perc_session_less_10
        general_df = general_df.merge(on='campaign_id', how='left', right=sessions_time_less_10_df)

        # доля сессий продолжительностью больше 10 но меньше 30 секунд
        sessions_time_10_30_df = sessions_df.drop(columns=['session_id', 'sessions']).groupby('campaign_id').apply(
            lambda x: ((x['timespent'] >= 10) & (x['timespent'] <= 30)).mean(), include_groups=False).reset_index(
            name='sessions_10_30')
        summary_session_time_10_30 = sessions_time_10_30_df['campaign_id'] == 'Итого и средние'
        mean_perc_session_10_30 = sessions_time_10_30_df.loc[~summary_session_time_10_30, 'sessions_10_30'].mean()
        sessions_time_10_30_df.loc[summary_session_time_10_30, 'sessions_10_30'] = mean_perc_session_10_30
        general_df = general_df.merge(on='campaign_id', how='left', right=sessions_time_10_30_df)

        # доля сессий продолжительностью больше 30
        sessions_time_gt_30_df = sessions_df.drop(columns=['session_id', 'sessions']).groupby('campaign_id').apply(
            lambda x: (x['timespent'] > 30).mean(), include_groups=False).reset_index(name='sessions_gt_30')
        summary_session_time_gt_30 = sessions_time_gt_30_df['campaign_id'] == 'Итого и средние'
        mean_perc_session_gt_30 = sessions_time_gt_30_df.loc[~summary_session_time_gt_30, 'sessions_gt_30'].mean()
        sessions_time_gt_30_df.loc[summary_session_time_gt_30, 'sessions_gt_30'] = mean_perc_session_gt_30
        general_df = general_df.merge(on='campaign_id', how='left', right=sessions_time_gt_30_df)

        return general_df

    @fillna_decorator
    def get_campaign_groups(self, general_df: pd.DataFrame):
        """
        Данные (обработка) для листа "Группы кампаний"
        :param general_df:
        :return: DataFrame
        """
        # ПОДТЯГИВАТЬ ИЗ БД ИЛИ ПРИНИМАТЬ НА ВХОД (ТОЖЕ ПОДТЯНУТОЕ ИЗ БД)
        groups_campaigns = {
            'РСЯ': [704011362, 704010325, 704011628, 704011482, 704011760, 704013108, 704010283],
            'Поиск': [704004623, 704002660, 704002262, 704002942, 704004722, 704005046, 704001940]
        }

        # campaign_name заменяем на group_name
        columns = list(general_df.columns)
        del columns[1]
        columns.insert(1, 'group_name')
        dfs = []

        for key in groups_campaigns:
            df_group = general_df[general_df['campaign_id'].isin(map(str, groups_campaigns[key]))]
            df_group.insert(2, column='group_name', value=key)
            df_group = df_group.groupby(['group_name']).agg(
                {
                    'campaign_id': lambda ids: ', '.join(ids),
                    'clicks': 'sum',
                    'installs': 'sum',
                    'conversion_clicks': 'mean',
                    'active_users': 'sum',
                    'sessions': 'sum',
                    'session_per_install': 'mean',
                    'events_count': 'sum',
                    'events_per_session': 'mean',
                    'mean_timespent': 'mean',
                    'median_timespent': 'mean',
                    'sessions_lt_10': 'mean',
                    'sessions_10_30': 'mean',
                    'sessions_gt_30': 'mean',
                }
            ).reset_index()
            dfs.append(df_group)

        result = pd.concat(dfs)
        total_row = pd.DataFrame({
            'campaign_id': ['Все'],
            'group_name': ['Итого и средние'],
            'clicks': [result['clicks'].sum()],
            'installs': [result['installs'].sum()],
            'conversion_clicks': [result['conversion_clicks'].mean()],
            'active_users': [result['active_users'].sum()],
            'sessions': [result['sessions'].sum()],
            'session_per_install': [result['sessions'].sum() / result['installs'].sum()],
            'events_count': [result['events_count'].sum()],
            'events_per_session': [result['events_count'].sum() / result['sessions'].sum()],
            'mean_timespent': [result['mean_timespent'].mean()],
            'median_timespent': [general_df['median_timespent'].mean()],
            'sessions_lt_10': [result['sessions_lt_10'].mean()],
            'sessions_10_30': [result['sessions_10_30'].mean()],
            'sessions_gt_30': [result['sessions_gt_30'].mean()],
        })
        result = pd.concat([total_row, result])
        return result

    @fillna_decorator
    def get_week_distribution(self):
        """
        Получение и обработка данных для листа "Распределение по неделям"
        :return:
        """
        # установки сгруппированные по дате
        install_metrics = 'ym:i:advInstallDevices'
        install_dimensions = 'ym:i:dateTime'

        # сессии сгруппированные по дате
        sessions_metrics = 'ym:s:sessions'
        sessions_dimensions = 'ym:s:dateTime'

        logger.info('Запрос установок, сгруппированных по дате.')
        installs_request = self._make_request(install_metrics, install_dimensions, 'ym:ts:urlParameter')

        logger.info('Запрос сессий, сгруппированных по дате.')
        sessions_request = self._make_request(sessions_metrics, sessions_dimensions, 'ym:ts:urlParameter')

        # DataFrame-ы с удаленной строкой итогов (index=0), т.к не требуется при отображении
        installs_df = pd.read_csv(io.StringIO(installs_request.text)).drop(index=[0]).reset_index(drop=True)
        sessions_df = pd.read_csv(io.StringIO(sessions_request.text)).drop(index=[0]).reset_index(drop=True)

        # обработка и группировка данных по неделям
        installs_df.columns = ['datetime', 'installs']
        installs_df['datetime'] = pd.to_datetime(installs_df['datetime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        installs_df['week_number'] = installs_df['datetime'].dt.isocalendar().week
        installs_df = installs_df.drop(columns=['datetime']).groupby('week_number').sum()

        sessions_df.columns = ['datetime', 'sessions']
        sessions_df['datetime'] = pd.to_datetime(sessions_df['datetime'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        sessions_df['week_number'] = sessions_df['datetime'].dt.isocalendar().week
        sessions_df = sessions_df.drop(columns=['datetime']).groupby('week_number').sum()

        # объединение установок и сессий по номерам недели
        result = installs_df.merge(on='week_number', how='left', right=sessions_df).reset_index()

        return result

    @fillna_decorator
    def get_retention_by_weeks(self):
        """
        Данные по retention за период
        :return:
        """
        # ДОЛЖЕН ПРИХОДИТЬ СО СКРИПТА СТЁПЫ
        # параметр, в котором хранится номер кампании в Yandex App Metrica
        campaign_id_param = 'utm_campaign'

        # целое количество недель в периоде
        weeks_num = int((self.date2 - self.date1).days / 7)

        # отдельный url api-запрос для получения retention
        api_url = 'https://api.appmetrica.yandex.ru/v2/user/acquisition.csv'

        # метрика retention
        metric = r'retentionWeek{{week_num}}Percentage'
        # группировка по кампаниям
        dimension = fr"urlParameter{{'{campaign_id_param}'}}"

        # метрики для запроса
        metrics = f','.join([metric.replace('{{week_num}}', str(_)) for _ in range(1, weeks_num + 1)])

        logger.info(f'Запрашиваю retention-rate за {weeks_num} недель.')
        retention_request = self._make_request(metrics, dimension, "ym:ts:urlParameter", url=api_url)

        retention_df = pd.read_csv(io.StringIO(retention_request.text))
        # удаление строки итогов
        retention_df = retention_df.drop(index=[0]).reset_index(drop=True)
        labels = retention_df.columns.tolist()
        labels[0] = 'campaign_id'
        retention_df.columns = labels

        return retention_df

    @fillna_decorator
    def get_events(self) -> pd.DataFrame:
        """
        Получение и обработка данных для листа "События"
        :return:
        """
        # ДОБАВИТЬ ФИЛЬТРЫ ПО КАМПАНИЯМ (см. постман)
        metrics = 'ym:ce2:allEvents,ym:ce2:devicesWithEvent,ym:ce2:eventsPerDevice,ym:ce2:devicesPercent'
        dimensions = 'ym:ce2:eventLabel'

        logger.info('Запрос суммарного количества событий.')
        response = self._make_request(metrics, dimensions, 'ym:ts:urlParameter')

        events_df = pd.read_csv(io.StringIO(response.text))
        labels = ['event', 'count_event', 'users', 'event_per_user', 'perc_all_users']
        events_df.columns = labels

        return events_df

    @fillna_decorator
    def get_installs_info(self):
        metrics = 'ym:i:advInstallDevices'
        dimensions = 'ym:i:regionCity,ym:i:operatingSystem,ym:i:mobileDeviceModel'

        logger.info('Запрос данных по установкам (регион, ОС, марка).')
        installs_info_request = self._make_request(metrics, dimensions, 'ym:ts:urlParameter')

        installs_info_df = pd.read_csv(io.StringIO(installs_info_request.text))
        installs_info_labels = ['city', 'oc', 'device_type', 'installs']
        installs_info_df.columns = installs_info_labels

        return installs_info_df

    @status_decorator
    def _make_request(self, metrics: str, dimensions: str, filter_label: str, url: str = None) -> requests.Response:
        """
        Выполнение запроса к App Metrica
        :param metrics:
        :param dimensions:
        :param filter_label:
        :return:
        """
        parameters = self._get_parameters(metrics, dimensions, filter_label)

        if url:
            request = requests.get(url, headers=self.header, params=parameters)
            return request

        request = requests.get(self.api_url, headers=self.header, params=parameters)
        return request

    def _get_parameters(self, metrics: str, dimensions: str, filter_label: str) -> dict:
        """
        Извлекает шаблон и заполняет параметры запроса в соответствии с переданными параметрами
        :param metrics: метрики из AppMetrica
        :param dimensions: группировки из AppMetrica
        :param filter_label: параметр фильтрации из AppMetrica
        :return: словарь dict с параметрами запроса
        """
        with open('params_config.json', encoding='utf-8') as file:
            data = json.load(file)
            data = json.dumps(data)

        filters = map(str, self.campaign_ids)
        filters = map(lambda item: f"{filter_label}{'utm_campaign'}==" + item, filters)
        filters = ' OR '.join(filters)

        data = data.replace('{{app_id}}', self.app_id)
        data = data.replace('{{metrics}}', metrics)
        data = data.replace('{{dimensions}}', dimensions)
        data = data.replace('{{filters}}', filters)
        data = data.replace('{{date1}}', self.date1_repr)
        data = data.replace('{{date2}}', self.date2_repr)
        data = json.loads(data)
        return data


CAMPAIGNS = [704011362, 704010325, 704011628, 704011482, 704011760, 704013108, 704010283, 704004623, 704002660,
             704002262, 704002942, 704004722, 704005046, 704001940]


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

    # добавление в retention DataFrame поля с установками
    retention = retention.merge(general[['campaign_id', 'installs']], on='campaign_id', how='left')
    labels = retention.columns.tolist()
    # делаем колонку с кол-вом установок второй по счёту
    labels.insert(1, labels.pop(-1))
    retention = retention[labels]

    # создание файла в оперативной памяти
    with io.BytesIO() as file:
        workbook = xlsxwriter.Workbook(file, options={'in_memory': True})
        workbook.filename = 'yapp_report.xlsx'

    # форматирование даты в ру-формат для вставки в заголовок листа
    form_d1 = datetime.strptime(date1, '%Y-%m-%d').date().strftime('%d.%m.%Y')
    form_d2 = datetime.strptime(date2, '%Y-%m-%d').date().strftime('%d.%m.%Y')

    # формирование листов
    xlsx_form = XlsxForm(workbook, f'{doc_header} {form_d1} - {form_d2}')
    xlsx_form.write_general(general, sheet_name='Все кампании')
    xlsx_form.write_general(general_groups, sheet_name='Группы кампаний')
    xlsx_form.write_week_distribution(week_distribution)
    xlsx_form.write_retention_by_weeks(retention)
    xlsx_form.write_events(events)
    xlsx_form.write_installs_by_regions(installs_info)
    xlsx_form.write_installs_by_oc(installs_info)
    xlsx_form.write_installs_by_brand(installs_info)

    # закрытие и сохранение файла
    workbook.close()


# НЕОБХОДИМО ДОБАВИТЬ ВО ВСЕ ДАТАФРЕЙМЫ ПРОВЕРКУ НА ПОЛУЧАЕМЫЕ ИЗ МЕТРИКИ ДАННЫЕ, ЕСЛИ ИЗ МЕТРИКИ НИЧЕГО НЕ ВЕРНУЛОСЬ В DATAFRAME
# ДОЛЖНЫ БЫТЬ ВСЕ!!! КАМПАНИИ С 0 ПАРАМЕТРАМИ
create_report('2777872', '2025-11-1', '2025-11-30', CAMPAIGNS, 'Отчёт - приложение "Узнай Москву"')
