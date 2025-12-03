import io
import logging

import numpy as np
import pandas as pd
import xlsxwriter
from xlsxwriter.worksheet import Worksheet

logger = logging.getLogger(__name__)


class XlsxForm:
    def __init__(self, workbook: xlsxwriter.Workbook, header: str):
        self.workbook = workbook
        self.header = header

        self.header_format = workbook.add_format(
            {'bold': True, 'align': 'center', 'text_wrap': True, 'valign': 'vcenter', 'font_size': 11, 'border': 2,
             'bg_color': '#B0E0E6'})
        self.number_format = workbook.add_format({'num_format': '#,##0', 'align': 'center', 'border': 1})
        self.float_format = workbook.add_format({'num_format': '0.00', 'align': 'center', 'border': 1})
        self.percent_format = workbook.add_format({'num_format': '0.00%', 'align': 'center', 'border': 1})
        self.event_format = workbook.add_format({'align': 'left', 'border': 1})

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
        sheet.set_row(1, 60)

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

    def general_writer(self, general_params: pd.DataFrame, sheet_name):
        logger.info(f'Записываю лист "{sheet_name}".')
        general_sheet = self.workbook.add_worksheet(sheet_name)
        self._write_campaigns_header(general_sheet)
        general_params = general_params.fillna(0)

        for row, i in enumerate(range(len(general_params)), start=2):
            item = general_params.iloc[i]
            for col, data in enumerate(item):
                if col == 4:
                    general_sheet.write(row, col, data, self.percent_format)
                else:
                    general_sheet.write(row, col, data)
        logger.info('Успех.')

    def write_week_distribution(self, installs_sessions_by_week: pd.DataFrame):
        logger.info('Записываю лист "Распределение по неделям"')

        distribution_sheet = self.workbook.add_worksheet('Распределение по неделям')
        distribution_sheet.set_column('A:C', 16)
        distribution_sheet.set_row(0, 60)

        # запись заголовка
        for i, header in enumerate(['Номер недели', 'Количество установок', 'Количество сессий']):
            distribution_sheet.write(0, i, header, self.header_format)

        # запись данных
        for row, i in enumerate(range(len(installs_sessions_by_week)), start=1):
            item = installs_sessions_by_week.iloc[i]
            for col, data in enumerate(item):
                distribution_sheet.write(row, col, data)

        # добавление графика
        chart = self.workbook.add_chart({'type': 'scatter', 'subtype': 'smooth_with_markers'})
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
            'line': {'none': True},
            'marker': {'type': 'automatic'},
        })
        chart.add_series({
            'name': f"='Распределение по неделям'!C1",
            'categories': f"='Распределение по неделям'!$A$2:$A${len(installs_sessions_by_week) + 1}",
            'values': f"='Распределение по неделям'!$C$2:$C${len(installs_sessions_by_week)}",
            'line': {'none': True},
            'marker': {'type': 'automatic'},
        })

        distribution_sheet.insert_chart('E2', chart)

        logger.info('*Заглушка* Успех')

    def event_writter(self, events: pd.DataFrame):
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
            events_sheet.write(i, 0, events.iloc[i].event, self.event_format)
            events_sheet.write(i, 1, events.iloc[i].count_event, self.number_format)
            events_sheet.write(i, 2, events.iloc[i].users, self.number_format)
            events_sheet.write(i, 3, events.iloc[i].event_per_user, self.float_format)
            events_sheet.write(i, 4, events.iloc[i].perc_all_users, self.percent_format)

        events_sheet.conditional_format(f'B2:B{len(events) + 1}', {'type': '3_color_scale'})
        events_sheet.conditional_format(f'C2:C{len(events) + 1}', {'type': '3_color_scale'})
        events_sheet.conditional_format(f'D2:D{len(events) + 1}', {'type': '3_color_scale'})
        events_sheet.conditional_format(f'E2:E{len(events) + 1}', {'type': '3_color_scale'})

        # events_sheet.autofit(400)
        logger.info('Успех.')

    def installs_by_regions(self, installs__by_regions: pd.DataFrame = None):
        logger.info('Записываю лист "Регионы (Установки)"')
        distribution_sheet = self.workbook.add_worksheet('Регионы (Установки)')
        logger.info('*Заглушка* Успех')

    def installs_by_oc(self, installs__by_oc: pd.DataFrame = None):
        logger.info('Записываю лист "ОС (Установки)"')
        distribution_sheet = self.workbook.add_worksheet('ОС (Установки)')
        logger.info('*Заглушка* Успех')

    def installs_by_brand(self, installs__by_brand: pd.DataFrame = None):
        logger.info('Записываю лист "Марка (Установки)"')
        distribution_sheet = self.workbook.add_worksheet('Марка (Установки)')
        logger.info('*Заглушка* Успех')
