import random

from utm_tag.test_part2 import get_campaign_params
import pandas as pd

# campaigns = [704011362, 704010325, 704011628, 704011482, 704011760, 704013108, 704010283, 704004623, 704002660,
#              704002262, 704002942, 704004722, 704005046, 704001940]
# campaigns = list(map(str, campaigns))
# print(campaigns)
# print(get_campaign_params(campaigns))

camp_ids = ['704011362', '704010325', '704011628', '704011760',
            '704013108', '704010283', '704004623', '704002660',
            '704002262', '704002942', '704004722', '704005046', '704001940', '704011482']
campaigns_df = pd.DataFrame(
    {'campaign_id': camp_ids})

names = pd.DataFrame({'campaign_id': camp_ids,
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
                                        ],
                      })

sessions = pd.DataFrame({'campaign_id': camp_ids[:-1],
                         'sessions': [random.random() for _ in range(len(camp_ids)-1)]})

campaigns_df = campaigns_df.merge(names, on='campaign_id', how='left')
empty = pd.DataFrame()
print(campaigns_df)
campaigns_df = campaigns_df.merge(sessions, how='left', on='campaign_id').fillna(0)
print(campaigns_df)