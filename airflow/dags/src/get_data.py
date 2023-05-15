import requests
import json
import csv
import os
import pandas as pd

def get_init_data():
    limit = 1000
    offset = 0
    df=[]

    api_url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?limit={}&offset={}'.format(limit, offset)
    data_info = requests.get(api_url)
    set_info = json.loads(data_info.text)
    total = set_info['total']
    count = min(total, 5*limit) # 100 requests limit per minute
    print('Loading', count, 'data')

    while offset < count:
        api_url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?limit={}&offset={}'.format(limit, offset)
        data_info = requests.get(api_url)
        set_info = json.loads(data_info.text)
        for data in set_info['results']:
            df.append(data)
        offset += limit
        print(offset, 'data loaded')
    print('Finished loading data')

    csv_path = os.getcwd()+"/dags/src/data/raw.csv"
    field_names = list(data.keys())

    print('Saving raw data')

    with open(csv_path, 'w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=field_names)
        writer.writeheader()
        for i in df:
            writer.writerow(i)

    # raw = pd.DataFrame(df)
    # print('Saving raw data')
    # raw.to_excel(open(os.getcwd()+"/dags/src/data/raw.xlsx", "wb"))