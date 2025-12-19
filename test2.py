import pandas as pd

cols = ['param', 'clicks', 'installs', 'conv']
df1 = pd.read_csv('data1.csv')
df1.columns = cols
df2 = pd.read_csv('data2.csv')
df2.columns = cols
df_names = pd.DataFrame({'campaign_id': ['704011362', '704011760'], 'campaign_name': ['Абоба1', 'Абоба2']})
df_names = df_names.set_index('campaign_id')['campaign_name'].to_dict()
df2 = df2[['param', 'clicks']]
df2['param'] = df2['param'].apply(lambda x: df_names.get(x, x))
print(df_names)
print(df2)
df2['week0'] = 100
print(df2)