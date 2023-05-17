import requests
import json
import csv
import os
from datetime import date
from datetime import timedelta

def get_yesterday():
    today = date.today()
    return today - timedelta(days = 1)

def get_data_set(limit, offset, start, end):
    api_url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?limit={}&offset={}&start={}&end={}'.format(limit, offset, start, end)
    data_info = requests.get(api_url)
    return json.loads(data_info.text)

def get_init_data(**kwargs):
    limit = 1000
    offset = 0
    end = get_yesterday()
    start = end - timedelta(days = 60)
    df = []

    set_info = get_data_set(limit, offset, start, end)
    total = set_info['total']
    count = min(total, 50*limit) # 100 requests limit per minute
    print('Loading', count, 'data')

    while offset < count:
        set_info = get_data_set(limit, offset, start, end)
        for data in set_info['results']:
            df.append(data)
        offset += limit
        print(offset, 'data loaded')
    print('Finished loading data')

    csv_path = os.getcwd()+kwargs['path_data']+"/raw.csv"
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

def update_daily_data(**kwargs):
    limit = 1000
    offset = 0
    end = get_yesterday()
    start = end
    df = []

    set_info = get_data_set(limit, offset, start, end)
    total = set_info['total']
    count = min(total, 50*limit) # 100 requests limit per minute
    print('Loading', count, 'data from', end)

    while offset < count:
        set_info = get_data_set(limit, offset, start, end)
        for data in set_info['results']:
            df.append(data)
        offset += limit
        print(offset, 'data loaded')
    print('Finished loading data')

    csv_path = os.getcwd()+kwargs['path_data']+"/raw.csv"
    field_names = list(data.keys())

    print('Updating data')

    with open(csv_path, 'a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=field_names)
        for i in df:
            writer.writerow(i)