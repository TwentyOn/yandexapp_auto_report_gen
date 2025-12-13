import pandas as pd

df1 = pd.read_csv('data1.csv')
df2 = pd.read_csv('data2.csv')

df3 = pd.concat([df1, df2]).reset_index(drop=True)
# df3 = df3.groupby(df3.columns[0]).sum().reset_index()
total_rows = df3[df3['Параметр'] == 'Итого и средние']
print(total_rows)