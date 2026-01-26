from datetime import datetime
import io
import json
import logging
from time import perf_counter
from functools import wraps

import requests
import dotenv
import pandas as pd
import numpy as np

from get_utm_tag.test_part2 import get_campaign_params

dotenv.load_dotenv()

logging.basicConfig(level=logging.INFO, format='[{asctime}] #{levelname:4} {name}:{lineno} - {message}', style='{')
logger = logging.getLogger('main.py')
pd.set_option('future.no_silent_downcasting', True)


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
            # print(result.url)
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
    def __init__(self, yapp_token, app_id, date1, date2, campaigns_data: list[tuple], yd_login: str):
        self.yapp_token = yapp_token
        self.api_url = 'https://api.appmetrica.yandex.ru/stat/v1/data.csv'
        self.header = {'Authorization': yapp_token}
        self.app_id = app_id

        self.date1_repr = date1
        self.date2_repr = date2
        # перевод строкового значения даты в python-объект date для вычислений
        self.date1 = datetime.strptime(date1, '%Y-%m-%d').date()
        self.date2 = datetime.strptime(date2, '%Y-%m-%d').date()

        self.campaigns_data = pd.DataFrame(campaigns_data, columns=['campaign_id', 'campaign_name', 'campaign_group'])
        self.campaign_ids = self.campaigns_data['campaign_id'].tolist()
        # заполнитель для подстановки параметра содержащего campaign_id
        self.url_param_placeholder = "{{URL_PARAM}}"
        self.ids_by_parameter = self._get_campaign_url_param(yd_login)

    @fillna_decorator
    def get_all_campaigns(self) -> pd.DataFrame:
        """
        Данные (сбор и обработка) для листа "Все кампании"
        :return:
        """
        general_metrics = 'ym:ts:userClicks,ym:ts:advInstallDevices,ym:ts:clickToInstallConversion'
        general_dimensions = f"ym:ts:urlParameter{{'{self.url_param_placeholder}'}}"

        session_metrics = 'ym:s:sessions,ym:s:totalSessionDurationPerUser'
        session_dimensions = f"ym:s:profileUrlParameter{{'{self.url_param_placeholder}'}},ym:s:session"

        event_count_metrics = 'ym:ce2:allEvents'
        event_count_dimensions = f"ym:ce2:profileUrlParameter{{'{self.url_param_placeholder}'}},ym:ce2:device,ym:ce2:eventLabel"

        general_labels = ['campaign_id', 'clicks', 'installs', 'conversion_clicks']

        session_labels = ['campaign_id', 'session_id', 'sessions', 'timespent']
        events_count_labels = ['campaign_id', 'device_id', 'event', 'events_count']

        # базовый датафрейм с ID и именами кампаний
        base_df = pd.concat([
            pd.DataFrame({'campaign_id': ['Итого и средние'], 'campaign_name': ['Итого и средние']}),
            self.campaigns_data.drop(columns=['campaign_group'])])

        # запрос данных из API AppMetrica
        logger.info('Запрос основных параметров.')
        # request_general = self._make_request(general_metrics, general_dimensions, 'ym:ts:urlParameter')
        general_df = self.get_data(general_metrics, general_dimensions, 'ym:ts:urlParameter')
        general_df.columns = general_labels

        logger.info('Запрос количества сессий.')
        # request_sessions = self._make_request(session_metrics, session_dimensions, 'ym:ts:urlParameter')
        sessions_df = self.get_data(session_metrics, session_dimensions, 'ym:ts:urlParameter')
        sessions_df.columns = session_labels

        logger.info('Запрос количества событий.')
        # request_events_count = self._make_request(event_count_metrics, event_count_dimensions, 'ym:ts:urlParameter')
        events_count_df = self.get_data(event_count_metrics, event_count_dimensions, 'ym:ts:urlParameter')
        events_count_df.columns = events_count_labels

        # общие показатели кликов, установок, конверсии кликов
        # general_df = pd.read_csv(io.StringIO(request_general.text))
        # general_df.columns = general_labels
        general_df = base_df.merge(general_df, on='campaign_id', how='left')
        general_df.conversion_clicks = general_df.conversion_clicks.apply(lambda x: round(x, 2))

        # добавление имён кампаний
        general_labels.insert(1, 'campaign_name')
        # восстановление порядка следования колонок
        general_df = general_df[general_labels]

        # датафрейм с общей информацией о событиях
        # events_count_df = pd.read_csv(io.StringIO(request_events_count.text))
        # events_count_df.columns = events_count_labels

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
        # sessions_df = pd.read_csv(io.StringIO(request_sessions.text)).fillna(0)
        # sessions_df.columns = session_labels

        # Формирование результирующего датафрейма (со всеми параметрами)
        # добавление столбца с количеством новых пользователей
        general_df = general_df.merge(on='campaign_id', how='left', right=log_count_df)

        # общие показатели количества сессий
        sessions_count_df = sessions_df.drop(columns=['session_id', 'timespent']).groupby('campaign_id').sum()
        general_df = general_df.merge(on='campaign_id', how='left', right=sessions_count_df)
        general_df['sessions'] = pd.to_numeric(general_df['sessions'], errors='coerce')
        general_df['installs'] = pd.to_numeric(general_df['installs'])

        # столбец с количеством сессий на 1 установку
        general_df['session_per_install'] = np.where((general_df['sessions'] != 0) & (general_df['installs'] != 0),
                                                     (general_df['sessions'] / general_df['installs']).round(2), 0)

        # добавление столбца с количеством событий
        general_df = general_df.merge(on='campaign_id', how='left', right=total_events_df).fillna(0)
        # столбец с количеством событий на 1 сессию
        general_df['events_per_session'] = general_df.apply(
            lambda x: 0 if x['sessions'] == 0 else x['events_count'] / x['sessions'], axis=1)

        # среднее время сессий в секундах
        mean_session_time_df = sessions_df.drop(columns=['session_id', 'sessions']).groupby(
            'campaign_id').mean().round(2).rename(columns={'timespent': 'mean_timespent'}).reset_index()
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
            lambda x: (x['timespent'] < 10).mean(), include_groups=False)
        if not sessions_time_less_10_df.empty:
            sessions_time_less_10_df = sessions_time_less_10_df.reset_index(name='sessions_lt_10')
        else:
            sessions_time_less_10_df = sessions_time_less_10_df.rename(
                columns={'timespent': 'sessions_lt_10'}).reset_index()
        summary_session_time_less_10 = sessions_time_less_10_df['campaign_id'] == 'Итого и средние'
        mean_perc_session_less_10 = sessions_time_less_10_df.loc[~summary_session_time_less_10, 'sessions_lt_10'].mean()
        sessions_time_less_10_df.loc[summary_session_time_less_10, 'sessions_lt_10'] = mean_perc_session_less_10
        general_df = general_df.merge(on='campaign_id', how='left', right=sessions_time_less_10_df)

        # доля сессий продолжительностью больше 10 но меньше 30 секунд
        sessions_time_10_30_df = sessions_df.drop(columns=['session_id', 'sessions']).groupby('campaign_id').apply(
            lambda x: ((x['timespent'] >= 10) & (x['timespent'] <= 30)).mean(), include_groups=False)
        if not sessions_time_10_30_df.empty:
            sessions_time_10_30_df = sessions_time_10_30_df.reset_index(name='sessions_10_30')
        else:
            sessions_time_10_30_df = sessions_time_10_30_df.rename(
                columns={'timespent': 'sessions_10_30'}).reset_index()
        summary_session_time_10_30 = sessions_time_10_30_df['campaign_id'] == 'Итого и средние'
        mean_perc_session_10_30 = sessions_time_10_30_df.loc[~summary_session_time_10_30, 'sessions_10_30'].mean()
        sessions_time_10_30_df.loc[summary_session_time_10_30, 'sessions_10_30'] = mean_perc_session_10_30
        general_df = general_df.merge(on='campaign_id', how='left', right=sessions_time_10_30_df)

        # доля сессий продолжительностью больше 30
        sessions_time_gt_30_df = sessions_df.drop(columns=['session_id', 'sessions']).groupby('campaign_id').apply(
            lambda x: (x['timespent'] > 30).mean(), include_groups=False)
        if not sessions_time_gt_30_df.empty:
            sessions_time_gt_30_df = sessions_time_gt_30_df.reset_index(name='sessions_gt_30')
        else:
            sessions_time_gt_30_df = sessions_time_gt_30_df.rename(
                columns={'timespent': 'sessions_gt_30'}).reset_index()
        summary_session_time_gt_30 = sessions_time_gt_30_df['campaign_id'] == 'Итого и средние'
        mean_perc_session_gt_30 = sessions_time_gt_30_df.loc[~summary_session_time_gt_30, 'sessions_gt_30'].mean()
        sessions_time_gt_30_df.loc[summary_session_time_gt_30, 'sessions_gt_30'] = mean_perc_session_gt_30
        general_df = general_df.merge(on='campaign_id', how='left', right=sessions_time_gt_30_df)

        return general_df.sort_values(by='clicks', ascending=False)

    @fillna_decorator
    def get_campaign_groups(self, general_df: pd.DataFrame):
        """
        Данные (обработка) для листа "Группы кампаний"
        :param general_df:
        :return: DataFrame
        """
        # ПОДТЯГИВАТЬ ИЗ БД ИЛИ ПРИНИМАТЬ НА ВХОД (ТОЖЕ ПОДТЯНУТОЕ ИЗ БД)
        groups_campaigns = self.campaigns_data.drop(columns=['campaign_name'])
        groups_campaigns = groups_campaigns.groupby('campaign_group')['campaign_id'].apply(list).to_dict()

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
            'campaign_id': ['Итого и средние'],
            'group_name': ['Итого и средние'],
            'clicks': [result['clicks'].sum()],
            'installs': [result['installs'].sum()],
            'conversion_clicks': [result['conversion_clicks'].mean()],
            'active_users': [result['active_users'].sum()],
            'sessions': [result['sessions'].sum()],
            'session_per_install': [
                0 if result['installs'].sum() == 0 else result['sessions'].sum() / result['installs'].sum()],
            'events_count': [result['events_count'].sum()],
            'events_per_session': [
                0 if result['sessions'].sum() == 0 else result['events_count'].sum() / result['sessions'].sum()],
            'mean_timespent': [result['mean_timespent'].mean()],
            'median_timespent': [general_df['median_timespent'].mean()],
            'sessions_lt_10': [result['sessions_lt_10'].mean()],
            'sessions_10_30': [result['sessions_10_30'].mean()],
            'sessions_gt_30': [result['sessions_gt_30'].mean()],
        })
        result = pd.concat([total_row, result]).sort_values(by='clicks', ascending=False)
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
        install_campaign_filter = 'ym:ts:urlParameter'

        # сессии сгруппированные по дате
        sessions_metrics = 'ym:s:sessions'
        sessions_dimensions = 'ym:s:dateTime'
        sessions_campaign_filter = 'ym:ts:urlParameter'

        logger.info('Запрос установок, сгруппированных по дате.')
        # installs_request = self._make_request(install_metrics, install_dimensions, install_campaign_filter)
        installs_df = self.get_data(install_metrics, install_dimensions, sessions_campaign_filter)

        logger.info('Запрос сессий, сгруппированных по дате.')
        # sessions_request = self._make_request(sessions_metrics, sessions_dimensions, sessions_campaign_filter)
        sessions_df = self.get_data(sessions_metrics, sessions_dimensions, sessions_campaign_filter)

        try:
            # DataFrame-ы с удаленной строкой итогов (index=0), т.к не требуется при отображении
            # installs_df = pd.read_csv(io.StringIO(installs_request.text)).drop(index=[0]).reset_index(drop=True)
            # sessions_df = pd.read_csv(io.StringIO(sessions_request.text)).drop(index=[0]).reset_index(drop=True)
            installs_df = installs_df.drop(index=[0]).reset_index(drop=True)
            sessions_df = sessions_df.drop(index=[0]).reset_index(drop=True)
        except KeyError:
            return pd.DataFrame()

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
        # целое количество недель в периоде
        weeks_num = max(1, int((self.date2 - self.date1).days / 7))

        # отдельный url api-запрос для получения retention
        api_url = 'https://api.appmetrica.yandex.ru/v2/user/acquisition.csv'

        # метрика retention
        metric = r'retentionWeek{{week_num}}Percentage'
        # метрики для запроса
        metrics = f','.join([metric.replace('{{week_num}}', str(_)) for _ in range(1, weeks_num + 1)])

        # группировка по кампаниям
        dimension = fr"urlParameter{{'{self.url_param_placeholder}'}}"

        filters = 'ym:ts:urlParameter'


        logger.info(f'Запрашиваю retention-rate за {weeks_num} недель.')
        # retention_request = self._make_request(metrics, dimension, "ym:ts:urlParameter", url=api_url)
        # получение данных по отдельном api-адресу
        retention_df = self.get_data(metrics, dimension, filters, url=api_url)

        # retention_df = pd.read_csv(io.StringIO(retention_request.text))
        # удаление строки итогов
        try:
            retention_df = retention_df.drop(index=[0]).reset_index(drop=True)
        except KeyError:
            logger.warning('За указанный период не удалось получить параметр retention')
            return pd.DataFrame()

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
        filters = 'ym:ts:urlParameter'

        logger.info('Запрос суммарного количества событий.')
        # response = self._make_request(metrics, dimensions, filters)
        events_df = self.get_data(metrics, dimensions, filters)

        # events_df = pd.read_csv(io.StringIO(response.text))
        labels = ['event', 'count_event', 'users', 'event_per_user', 'perc_all_users']
        events_df.columns = labels

        return events_df

    @fillna_decorator
    def get_installs_info(self):
        metrics = 'ym:i:advInstallDevices'
        dimensions = 'ym:i:regionCity,ym:i:operatingSystem,ym:i:mobileDeviceModel'
        filters = 'ym:ts:urlParameter'

        logger.info('Запрос данных по установкам (регион, ОС, марка).')
        # installs_info_request = self._make_request(metrics, dimensions, filters)
        installs_info_df = self.get_data(metrics, dimensions, filters)

        # installs_info_df = pd.read_csv(io.StringIO(installs_info_request.text))
        installs_info_labels = ['city', 'oc', 'device_type', 'installs']
        installs_info_df.columns = installs_info_labels

        return installs_info_df

    @fillna_decorator
    def get_data(self, metrics: str, dimensions: str, filter_label: str, url: str = None) -> pd.DataFrame:
        data = pd.DataFrame()
        for url_parameter in self.ids_by_parameter:
            dimensions = dimensions.replace(self.url_param_placeholder, url_parameter)
            parameters = self._get_parameters(
                self.ids_by_parameter[url_parameter], metrics, dimensions, filter_label, url_parameter)
            print(parameters)
            request = self._make_request(parameters, url)
            data = pd.concat([data, pd.read_csv(io.StringIO(request.text))]).reset_index(drop=True)

        return data

    @status_decorator
    def _make_request(self, parameters, url: str | None = None) -> requests.Response:
        """
        Выполнение запроса к App Metrica
        :param metrics:
        :param dimensions:
        :param filter_label:
        :return:
        """

        # в случае если передан альтернативный api-адрес
        if url:
            request = requests.get(url, headers=self.header, params=parameters)
            return request

        request = requests.get(self.api_url, headers=self.header, params=parameters)
        return request

    def _get_campaign_url_param(self, yd_login) -> dict:
        logger.info('Получаю параметры, содержащие campaign_id...')
        result = []
        url_params = get_campaign_params(self.campaign_ids, yd_login)

        for campaign_id in url_params:
            if url_params[campaign_id]:
                param = next(filter(lambda param: '{campaign_id}' in param, url_params[campaign_id].split('&')))
                result.append((campaign_id, param))
            else:
                result.append((campaign_id, 'utm_campaign={campaign_id}'))

        result = map(lambda t: (t[0], t[1].split('=')[0]), result)
        result = pd.DataFrame(result, columns=['campaign_id', 'param'])
        result = result.groupby('param')['campaign_id'].apply(list).to_dict()

        return result

    def _get_parameters(
            self, campaign_ids: list, metrics: str, dimensions: str, filter_label: str, url_parameter: str) -> dict:
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

        filters = map(str, campaign_ids)
        filters = map(lambda item: f"{filter_label}{{'{url_parameter}'}}==" + item, filters)
        filters = ' OR '.join(filters)

        data = data.replace('{{app_id}}', self.app_id)
        data = data.replace('{{metrics}}', metrics)
        data = data.replace('{{dimensions}}', dimensions)
        data = data.replace('{{filters}}', filters)
        data = data.replace('{{date1}}', self.date1_repr)
        data = data.replace('{{date2}}', self.date2_repr)
        data = json.loads(data)
        return data
