import string

import pandas as pd
import numpy as np
import json
from datetime import datetime, date
from datetime import timedelta

# df_i = pd.read_csv('installs.csv')
# df_i.columns = ['campaign_id', 'device_id', 'installs']
# df_e = pd.read_csv('new_event.csv')
# df_e.columns = ['device_id', 'event', 'count']
# df_e = df_e[df_e['event'] == 'Запуск приложения и отображение экрана заставки.']
# df_e = df_e[df_e['count'] != 0]
# print(len(df_e))

# d = df_e[df_e.device_id.isin(df_i['device_id'])]
# d = d[d['count'] != 0]
# print(len(d))
# =============================================================
# c = [704011362, 704010325, 704011628, 704011482, 704011760, 704013108, 704010283, 704004623, 704002660,
#      704002262, 704002942, 704004722, 704005046, 704001940]
#
# installs_df = pd.read_csv('installs.csv')
# installs_df.columns = ['campaign_id', 'device_id', 'installs']
# users_df = pd.read_csv('users.csv')
# users_df.columns = ['week', 'device_id', 'users']
# users_df['week'] = pd.to_datetime(users_df['week'], errors='coerce', format='%Y-%m-%d')
# users_df = users_df.drop_duplicates('device_id', keep='first')
#
# device_ids = installs_df.device_id[1:].values
# start_date = users_df.week[1:].min()
# users_df = users_df[users_df['device_id'].isin(device_ids)]
#
# print(len(users_df))
# users_df = users_df[users_df['week'] == datetime(year=2025, month=11, day=3)]
# print(users_df)
# print(len(users_df))

installs_df = pd.read_csv('installs.csv')
installs_df.columns = ['device_id', 'installs']
device_ids = installs_df.drop_duplicates('device_id', keep='first')['device_id'].tolist()

events_df = pd.read_csv('events.csv')
events_df.columns = ['campaign_id', 'device_id', 'event', 'login_count']

total_events_df = events_df.copy()
total_events_df = total_events_df.drop(columns=['event', 'device_id'])
total_events_df = total_events_df.groupby('campaign_id').sum().reset_index()
total_events_df = total_events_df.sort_values(by='login_count', ascending=False)

log_count_df = events_df.copy()
log_count_df = log_count_df[log_count_df['event'] == 'Запуск приложения и отображение экрана заставки.']
log_count_df = log_count_df.drop(columns=['device_id', 'event'])
log_count_df['login_count'] = np.where(log_count_df['login_count'] > 0, 1, 0)
log_count_df = log_count_df.groupby('campaign_id').sum().reset_index()

print(log_count_df)
