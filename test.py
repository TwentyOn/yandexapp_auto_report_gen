url_params = {
    '703986845': 'utm_source=yandex&utm_medium=mospaynew&utm_campaign={campaign_id}&utm_content={ad_id}&utm_term={keyword}',
    '703723040': None,
    '702469969': 'utm_source=msk2030&utm_medium=yandex&utm_campaign=media&utm_content=banner&utm_term={campaign_id}',
    '702468674': 'https://moscow2030.mos.ru/areas/put_it_geroya/utm_source=msk2030&utm_medium=yandex&utm_campaign=mk&utm_content=banner&utm_term={campaign_id}',
    '702470368': None,
    '702496972': None,
    '702498562': None
}

for campaign_id in params:
    if params[campaign_id]:
        result.append(next(filter(lambda param: '{campaign_id}' in param, params[campaign_id].split('&'))))

print(result)