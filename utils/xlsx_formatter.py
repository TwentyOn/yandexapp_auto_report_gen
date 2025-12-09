import io
import logging
import string

import numpy as np
import pandas as pd
import xlsxwriter
from xlsxwriter.worksheet import Worksheet

logger = logging.getLogger(__name__)


class CreateXlsx:
    def __init__(self, workbook: xlsxwriter.Workbook, header: str):
        self.workbook = workbook
        self.header = header

        self.header_format = workbook.add_format(
            {'bold': True, 'align': 'center', 'text_wrap': True, 'valign': 'vcenter', 'font_size': 11, 'border': 2,
             'bg_color': '#B0E0E6'})
        self.text_format = workbook.add_format({'align': 'left', 'valign': 'vcenter', 'text_wrap': True, 'border': 1})
        self.number_format = workbook.add_format({'num_format': '#,##0', 'align': 'center', 'valign': 'vcenter',
                                                  'border': 1})
        self.float_format = workbook.add_format({'num_format': '0.00', 'align': 'center', 'valign': 'vcenter',
                                                 'border': 1})
        self.percent_format = workbook.add_format({'num_format': '0.00%', 'align': 'center', 'valign': 'vcenter',
                                                   'border': 1})

    def _write_campaigns_header(self, sheet: Worksheet):
        """
        Метод формирует заголовки для первых двух листов отчёта
        :param sheet: xlsx-лист
        :return:
        """
        sheet.merge_range('A1:O1', self.header, self.header_format)
        sheet.set_column('A:A', 16)
        sheet.set_column('B:B', 60)
        sheet.set_column('C:O', 16)
        sheet.set_row(0, 20)
        sheet.set_row(1, 75)

        # запись заголовка листа
        sheet.write(1, 0, 'Номер кампании', self.header_format)
        sheet.write(1, 1, 'Наименование кампании', self.header_format)
        sheet.write(1, 2, 'Количество кликов', self.header_format)
        sheet.write(1, 3, 'Количество установок', self.header_format)
        sheet.write(1, 4, 'Конверсия клика в установку', self.header_format)
        sheet.write(
            1, 5, 'Количество пользователей, которые зашли в приложение минимум 1 раз',
            self.header_format)
        sheet.write(1, 6, 'Количество сессий всего', self.header_format)
        sheet.write(1, 7, 'Количество сессий на установку', self.header_format)
        sheet.write(1, 8, 'Количество событий всего', self.header_format)
        sheet.write(1, 9, 'Количество событий на 1 сессию', self.header_format)
        sheet.write(1, 10, 'Среднее время 1 сессии в секундах', self.header_format)
        sheet.write(1, 11, 'Медианное время 1 сессии в секундах', self.header_format)
        sheet.write(1, 12, 'Доля сессий с продолжительностью до 10 секунд', self.header_format)
        sheet.write(1, 13, 'Доля сессий с продолжительностью от 10 до 30 секунд', self.header_format)
        sheet.write(1, 14, 'Доля сессий с продолжительностью более 30 секунд', self.header_format)

    def write_general(self, general_params: pd.DataFrame, sheet_name):
        logger.info(f'Записываю лист "{sheet_name}".')
        general_sheet = self.workbook.add_worksheet(sheet_name)
        self._write_campaigns_header(general_sheet)
        general_params = general_params.fillna(0)

        for row, i in enumerate(range(len(general_params)), start=2):
            item = general_params.iloc[i]
            for col, data in enumerate(item):
                # определение формата ячейки
                if col in (0, 1):
                    cur_format = self.text_format
                elif col in (2, 3, 5, 6, 8):
                    cur_format = self.number_format
                elif col in (4, 12, 13, 14):
                    cur_format = self.percent_format
                else:
                    cur_format = self.float_format
                general_sheet.write(row, col, data, cur_format)

        # условное форматирование
        for col in string.ascii_uppercase[2:15]:
            general_sheet.conditional_format(f"{col}4:{col}{len(general_params) + 2}", {'type': '3_color_scale'})

        logger.info('Успех.')

    def write_week_distribution(self, installs_sessions_by_week: pd.DataFrame):
        logger.info('Записываю лист "Распределение по неделям"')

        distribution_sheet = self.workbook.add_worksheet('Распределение по неделям')
        distribution_sheet.set_column('A:C', 16)
        distribution_sheet.set_row(0, 60)

        # запись заголовка
        for i, header in enumerate(['Номер недели', 'Количество установок', 'Количество сессий']):
            distribution_sheet.write(0, i, header, self.header_format)

        if installs_sessions_by_week.empty:
            distribution_sheet.merge_range('A2:C2', 'Недостаточно данных для вывода', self.text_format)
            logger.warning('Недостаточно данных для вывода')
            return

        # запись данных
        for row, i in enumerate(range(len(installs_sessions_by_week)), start=1):
            item = installs_sessions_by_week.iloc[i]
            for col, data in enumerate(item):
                distribution_sheet.write(row, col, data, self.number_format)

        # добавление графика
        chart = self.workbook.add_chart({"type": "scatter", "subtype": "smooth_with_markers"})
        chart.set_title({
            'name': 'Распределение установок и сессий по неделям',
            'name_font': {
                'name': 'Calibri',
            },
        })
        chart.set_size({'width': 948, 'height': 378})
        chart.set_x_axis({"name": "Недели"})
        chart.set_y_axis({"name": "Количество"})

        chart.add_series({
            'name': f"='Распределение по неделям'!B1",
            'categories': f"='Распределение по неделям'!$A$2:$A${len(installs_sessions_by_week) + 1}",
            'values': f"='Распределение по неделям'!$B$2:$B${len(installs_sessions_by_week) + 1}",
            'marker': {'type': 'circle'},
        })
        chart.add_series({
            'name': f"='Распределение по неделям'!C1",
            'categories': f"='Распределение по неделям'!$A$2:$A${len(installs_sessions_by_week) + 1}",
            'values': f"='Распределение по неделям'!$C$2:$C${len(installs_sessions_by_week) + 1}",
            'marker': {'type': 'circle'},
        })

        distribution_sheet.insert_chart('E2', chart)

        logger.info('Успех.')

    def write_retention_by_weeks(self, retention_df: pd.DataFrame, general_df: pd.DataFrame):
        logger.info('Записываю лист "Retention-анализ".')

        retention_sheet = self.workbook.add_worksheet('Retention-анализ')

        if retention_df.empty or general_df.empty:
            retention_sheet.merge_range('A2:C2', 'Недостаточно данных для вывода', self.text_format)
            logger.warning('Недостаточно данных для вывода')
            return

        # добавление в retention поля с установками
        retention_df = retention_df.merge(general_df[['campaign_id', 'installs']], on='campaign_id', how='left')
        labels = retention_df.columns.tolist()
        # делаем колонку с кол-вом установок второй по счёту
        labels.insert(1, labels.pop(-1))
        retention_df = retention_df[labels]

        # количество колонок
        cols_count = len(retention_df.columns)

        retention_sheet.set_column(f'A:{string.ascii_uppercase[cols_count]}', 16)
        retention_sheet.set_row(0, 60)

        # количество недель в датафрейме
        weeks_count = cols_count - 2

        # заголовки для листа
        headers = ['Номер кампании', 'Количество установок']
        headers.extend([f'Retention {str(i + 1)} недели' for i in range(weeks_count)])

        # запись заголовка
        for i, header in enumerate(headers):
            retention_sheet.write(0, i, header, self.header_format)

        # запись данных
        for row, i in enumerate(range(len(retention_df)), start=1):
            item = retention_df.iloc[i]
            for col, data in enumerate(item):
                if col in range(2, cols_count):
                    # переводим в процентное соотношение, форматируем
                    retention_sheet.write(row, col, data / 100, self.percent_format)
                else:
                    retention_sheet.write(row, col, data, self.number_format)

        # добавление графика
        chart = self.workbook.add_chart({"type": "scatter", "subtype": "smooth_with_markers"})
        chart.set_title({
            'name': 'Распределение retention-rate по кампаниям',
            'name_font': {
                'name': 'Calibri',
            },
        })

        chart.set_size({'width': 1365, 'height': 500})
        chart.set_x_axis({"name": "Недели"})
        chart.set_y_axis({"name": "Retention"})
        for row_ind, col in enumerate(range(len(retention_df)), start=2):
            chart.add_series({
                'name': f"='Retention-анализ'!A{row_ind}",
                'categories': f"='Retention-анализ'!$C$1:${string.ascii_uppercase[cols_count - 1]}1",
                'values': f"='Retention-анализ'!$C${row_ind}:${string.ascii_uppercase[cols_count - 1]}${row_ind}",
                'marker': {'type': 'circle'},
            })

        retention_sheet.insert_chart(f"B{len(retention_df) + 4}", chart)

        logger.info('Успех.')

    def write_events(self, events: pd.DataFrame):
        logger.info('Записываю лист "События".')
        events_sheet = self.workbook.add_worksheet('События')

        events_sheet.set_column('A:A', 60)
        events_sheet.set_column('B:E', 16)
        events_sheet.set_row(0, 60)

        # запись заголовка листа
        events_sheet.write(0, 0, 'Наименование события', self.header_format)
        events_sheet.write(0, 1, 'Количество событий', self.header_format)
        events_sheet.write(0, 2, 'Количество пользователей', self.header_format)
        events_sheet.write(0, 3, 'Событий на пользователя', self.header_format)
        events_sheet.write(0, 4, 'Доля от всех пользователей', self.header_format)

        events.sort_values('count_event', inplace=True, ascending=False)
        for i in range(1, len(events)):
            events_sheet.write(i, 0, events.iloc[i].event, self.text_format)
            events_sheet.write(i, 1, events.iloc[i].count_event, self.number_format)
            events_sheet.write(i, 2, events.iloc[i].users, self.number_format)
            events_sheet.write(i, 3, events.iloc[i].event_per_user, self.float_format)
            events_sheet.write(i, 4, events.iloc[i].perc_all_users, self.percent_format)

        # условное форматирование
        for col in string.ascii_uppercase[1:5]:
            events_sheet.conditional_format(f"{col}2:{col}{len(events) + 1}", {'type': '3_color_scale'})

        # events_sheet.autofit(400)
        logger.info('Успех.')

    def write_installs_by_regions(self, installs_info: pd.DataFrame = None):
        logger.info('Записываю лист "Регионы (Установки)"')
        installs_by_regions_sheet = self.workbook.add_worksheet('Регионы (Установки)')

        if installs_info.empty:
            installs_by_regions_sheet.merge_range('A2:C2', 'Недостаточно данных для вывода', self.text_format)
            logger.warning('Недостаточно данных для вывода')
            return

        installs_by_regions_sheet.set_column(f'A:A', 25)
        installs_by_regions_sheet.set_column(f'B:B', 20)
        installs_by_regions_sheet.set_row(0, 60)

        # группируем данные по региону
        regions = installs_info.drop(columns=['oc', 'device_type'])
        regions = regions.groupby('city').sum().reset_index()
        regions = regions.sort_values(by='installs', ascending=False).reset_index(drop=True)
        regions = regions.drop(index=[0], errors='ignore')

        # запись заголовка
        installs_by_regions_sheet.write(0, 0, 'Город', self.header_format)
        installs_by_regions_sheet.write(0, 1, 'Количество установок', self.header_format)

        # запись данных
        for row, i in enumerate(range(len(regions)), start=1):
            item = regions.iloc[i]
            for col, data in enumerate(item):
                if col == 0:
                    installs_by_regions_sheet.write(row, col, data, self.text_format)
                else:
                    installs_by_regions_sheet.write(row, col, data, self.number_format)

        # условное форматирование
        installs_by_regions_sheet.conditional_format(f'B2:B{len(regions) + 1}', {'type': '3_color_scale'})

        logger.info('Успех.')

    def write_installs_by_oc(self, installs_info: pd.DataFrame):
        logger.info('Записываю лист "ОС (Установки)"')
        installs_by_oc_sheet = self.workbook.add_worksheet('ОС (Установки)')

        if installs_info.empty:
            installs_by_oc_sheet.merge_range('A2:C2', 'Недостаточно данных для вывода', self.text_format)
            logger.warning('Недостаточно данных для вывода')
            return

        installs_by_oc_sheet.set_column(f'A:A', 25)
        installs_by_oc_sheet.set_column(f'B:B', 20)
        installs_by_oc_sheet.set_row(0, 60)

        # группируем данные по операционной системе
        oc_df = installs_info.drop(columns=['city', 'device_type'])
        oc_df = oc_df.groupby('oc').sum().reset_index()
        oc_df = oc_df.sort_values(by='installs', ascending=False).reset_index(drop=True)
        oc_df = oc_df.drop(index=[0])

        # запись заголовка
        installs_by_oc_sheet.write(0, 0, 'Операционная система', self.header_format)
        installs_by_oc_sheet.write(0, 1, 'Количество установок', self.header_format)

        # запись данных
        for row, i in enumerate(range(len(oc_df)), start=1):
            item = oc_df.iloc[i]
            for col, data in enumerate(item):
                if col == 0:
                    installs_by_oc_sheet.write(row, col, data, self.text_format)
                else:
                    installs_by_oc_sheet.write(row, col, data, self.number_format)

        # условное форматирование
        installs_by_oc_sheet.conditional_format(f'B2:B{len(oc_df) + 1}', {'type': '3_color_scale'})

        logger.info('Успех.')

    def write_installs_by_brand(self, installs_info: pd.DataFrame):
        logger.info('Записываю лист "Марка (Установки)"')
        installs_by_brand_sheet = self.workbook.add_worksheet('Марка (Установки)')

        if installs_info.empty:
            installs_by_brand_sheet.merge_range('A2:C2', 'Недостаточно данных для вывода', self.text_format)
            logger.warning('Недостаточно данных для вывода')
            return

        installs_by_brand_sheet.set_column(f'A:A', 25)
        installs_by_brand_sheet.set_column(f'B:B', 20)
        installs_by_brand_sheet.set_row(0, 60)

        # группируем данные по операционной системе
        brand_df = installs_info.drop(columns=['city', 'oc'])
        brand_df = brand_df.groupby('device_type').sum().reset_index()
        brand_df = brand_df.sort_values(by='installs', ascending=False).reset_index(drop=True)
        brand_df = brand_df.drop(index=[0])

        # запись заголовка
        installs_by_brand_sheet.write(0, 0, 'Бренд устройства', self.header_format)
        installs_by_brand_sheet.write(0, 1, 'Количество установок', self.header_format)

        # запись данных
        for row, i in enumerate(range(len(brand_df)), start=1):
            item = brand_df.iloc[i]
            for col, data in enumerate(item):
                if col == 0:
                    installs_by_brand_sheet.write(row, col, data, self.text_format)
                else:
                    installs_by_brand_sheet.write(row, col, data, self.number_format)

        # условное форматирование
        installs_by_brand_sheet.conditional_format(f'B2:B{len(brand_df) + 1}', {'type': '3_color_scale'})

        logger.info('Успех.')
