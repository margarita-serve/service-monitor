import requests
import os

import json
import pandas as pd

from datetime import datetime, timedelta, timezone

HEADERS = {'Accept': 'application/json', 'Content-Type': 'application/json; charset=utf-8',
           'Access-Control-Allow-Origin': "*"}

KSERVE_API_DEFAULT_GRAFANA_ENDPOINT = os.environ.get("KSERVE_API_DEFAULT_GRAFANA_ENDPOINT")


def get_resource_data(query):
    url = f"{KSERVE_API_DEFAULT_GRAFANA_ENDPOINT}/api/v1/query_range"
    start = (datetime.now(tz=timezone.utc) - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:00.000Z")
    end = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:00.000Z")
    query = f"query={query}&start={start}&end={end}&step=1m"
    response = requests.get(url, query, headers=HEADERS)
    text = response.text
    json_text = json.loads(text)
    if len(json_text['data']['result']) == 0:
        return {"x": [0], "y": [0]}
    else:
        df = pd.DataFrame(json_text['data']['result'][0]['values'], columns=['x', 'y'])
    x = df['x'].to_list()
    x = [i * 1000 for i in x]
    y = df['y'].to_list()

    return {"x": x, "y": y}

# def get_cpu_data_one(query):
#     url = f"{KSERVE_API_DEFAULT_GRAFANA_ENDPOINT}/api/v1/query"
#     query = f"query={query}"
#     response = requests.get(url, query, headers=HEADERS)
#     text = response.text
#     json_text = json.loads(text)
#     x = json_text['data']['result'][0]['value'][0]
#     x = datetime.utcfromtimestamp(x).strftime("%Y-%m-%dT%H:%M:00.000Z")
#     y = json_text['data']['result'][0]['value'][1]
#
#     return {"x": x, "y": y}
