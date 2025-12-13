import pandas as pd

url_params = {
    '703986845': 'utm_source=yandex&utm_medium=mospaynew&utm_campaign={campaign_id}&utm_content={ad_id}&utm_term={keyword}',
    '703723040': None,
    '702469969': 'utm_source=msk2030&utm_medium=yandex&utm_campaign=media&utm_content=banner&utm_term={campaign_id}',
    '702468674': 'https://moscow2030.mos.ru/areas/put_it_geroya/utm_source=msk2030&utm_medium=yandex&utm_campaign=mk&utm_content=banner&utm_term={campaign_id}',
    '702470368': None,
    '702496972': None,
    '702498562': None
}

result = []

for campaign_id in url_params:
    if url_params[campaign_id]:
        param = next(filter(lambda param: '{campaign_id}' in param, url_params[campaign_id].split('&')))
        result.append((campaign_id, param))
    else:
        result.append((campaign_id, 'utm_campaign={campaign_id}'))

result = map(lambda t: (t[0], t[1].split('=')[0]), result)

df = pd.DataFrame(result, columns=['campaign_id', 'param'])
df = df.groupby('param')['campaign_id'].apply(list).to_dict()
d = "ym:ts:urlParameter{'{{URL_PARAM}}'}"
for url_param in df:
    ids = df[url_param]
    dimension = d.replace('{{URL_PARAM}}', url_param)
    print(ids)
    print(dimension)
