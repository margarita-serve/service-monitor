import os
import time
import warnings
from concurrent import futures
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests
from dateutil import parser

import utils

warnings.filterwarnings("ignore", category=UserWarning)

KSERVE_API_DEFAULT_GRAFANA_ENDPOINT = os.environ.get("KSERVE_API_DEFAULT_GRAFANA_ENDPOINT")


def get_servicehealth_timeline(inference_name, model_id, start_time, end_time, cal_hour, offset):
    with futures.ThreadPoolExecutor() as executor:
        total_requests = executor.submit(get_total_requests, inference_name, model_id, start_time, end_time, cal_hour,
                                         offset)
        data_error_rate = executor.submit(get_data_error_rate, inference_name, model_id, start_time, end_time, cal_hour,
                                          offset)
        system_error_rate = executor.submit(get_system_error_rate, inference_name, model_id, start_time, end_time,
                                            cal_hour, offset)
        median_peak_load = executor.submit(get_median_peak_load, inference_name, model_id, start_time, end_time)
        response_time = executor.submit(get_response_time, inference_name, model_id, start_time, end_time)

        result, total_requests = total_requests.result()
        if result is False:
            return False, total_requests
        result, data_error_rate = data_error_rate.result()
        if result is False:
            return False, data_error_rate
        result, system_error_rate = system_error_rate.result()
        if result is False:
            return False, system_error_rate
        result, median_peak_load = median_peak_load.result()
        if result is False:
            return False, median_peak_load
        result, response_time = response_time.result()
        if result is False:
            return False, response_time

    # result, total_requests = get_total_requests(inference_name, model_id, start_time, end_time, cal_hour,
    #                                             offset)
    # if result is False:
    #     return False, total_requests
    # result, data_error_rate = get_data_error_rate(inference_name, model_id, start_time, end_time, cal_hour, offset)
    # if result is False:
    #     return False, data_error_rate
    # result, system_error_rate = get_system_error_rate(inference_name, model_id, start_time, end_time, cal_hour, offset)
    # if result is False:
    #     return False, system_error_rate
    # result, median_peak_load = get_median_peak_load(inference_name, model_id, start_time, end_time)
    # if result is False:
    #     return False, median_peak_load
    # result, response_time = get_response_time(inference_name, model_id, start_time, end_time)
    # if result is False:
    #     return False, response_time

    data = {
        "total_requests": total_requests,
        "data_error_rate": data_error_rate,
        "system_error_rate": system_error_rate,
        "median_peak_load": median_peak_load,
        "response_time": response_time
    }

    return True, data


def get_servicehealth_metrics(inference_name, model_id, start_time, end_time):
    if parser.parse(end_time) > datetime.now(tz=timezone.utc):
        now_end_time = datetime.now(tz=timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%dT%H:%M:00.000Z")
        cal_hour = True
    else:
        now_end_time = end_time

    result, total_requests = get_agg_total_requests(inference_name, model_id, start_time, now_end_time)
    if result is False:
        return False, total_requests
    result, data_error_rate = get_agg_data_error_rate(inference_name, model_id, start_time, now_end_time)
    if result is False:
        return False, data_error_rate
    result, system_error_rate = get_agg_system_error_rate(inference_name, model_id, start_time, now_end_time)
    if result is False:
        return False, system_error_rate
    result, median_peak_load = get_agg_median_peak_load(inference_name, model_id, start_time, now_end_time)
    if result is False:
        return False, median_peak_load
    result, response_time = get_agg_response_time(inference_name, model_id, start_time, end_time)
    if result is False:
        return False, response_time

    data = {
        "total_requests": total_requests,
        "data_error_rate": data_error_rate,
        "system_error_rate": system_error_rate,
        "median_peak_load": median_peak_load,
        "response_time": response_time
    }

    return True, data


def get_total_requests(inference_name, model_id, start_time, end_time, cal_hour, offset):
    url = KSERVE_API_DEFAULT_GRAFANA_ENDPOINT + "/api/v1/query"
    url_range = KSERVE_API_DEFAULT_GRAFANA_ENDPOINT + "/api/v1/query_range"

    destination_service_name = "{" + f"destination_service_name=~'{inference_name}-.*'" + "}"

    start_time, end_time, interval, period = utils.get_interval(start_time, end_time)

    query = f'query=sum(istio_requests_total{destination_service_name} - istio_requests_total{destination_service_name} offset 1h or istio_requests_total{destination_service_name} unless istio_requests_total{destination_service_name} offset 1h)&start={start_time}&end={end_time}&step=1h'

    response = ''
    count = 0
    while response == '':
        try:
            response = requests.get(url_range, params=query)
            break
        except:
            count += 1
            print(f"Connection refused by server.. retry {count}/5")
            if count > 5:
                return False, "Connection refused by server"
            time.sleep(3)
            continue

    if cal_hour is True:
        query_hour = f'query=sum(istio_requests_total{destination_service_name} - istio_requests_total{destination_service_name} offset {offset}m or istio_requests_total{destination_service_name} unless istio_requests_total{destination_service_name} offset {offset}m)'
        cal_response = ''
        count = 0
        while cal_response == '':
            try:
                cal_response = requests.get(url, params=query_hour)
                break
            except:
                count += 1
                print(f"Connection refused by server.. retry {count}/5")
                if count > 5:
                    return False, "Connection refused by server"
                time.sleep(3)
                continue
        result, hour_data = get_hour_data(cal_response)
        if result is False:
            return False, hour_data
        date_end_time = (datetime.now(tz=timezone.utc) + timedelta(hours=1)).strftime('%Y-%m-%d %H:00')
        # data['value'][date_end_time] = float(hour_data)
    else:
        date_end_time = False
        hour_data = False

    result, data = get_data(response, start_time, end_time, interval, period, "total_requests", date_end_time,
                            hour_data)
    if result is False:
        return False, data

    return True, data


def get_data_error_rate(inference_name, model_id, start_time, end_time, cal_hour, offset):
    url = KSERVE_API_DEFAULT_GRAFANA_ENDPOINT + "/api/v1/query"
    url_range = KSERVE_API_DEFAULT_GRAFANA_ENDPOINT + "/api/v1/query_range"

    destination_service_name = "{" + f"destination_service_name=~'{inference_name}-.*'" + "}"
    destination_with_code = "{" + f"destination_service_name=~'{inference_name}-.*',response_code=~'4.*'" + "}"

    start_time, end_time, interval, period = utils.get_interval(start_time, end_time)

    query = f'query=sum(istio_requests_total{destination_with_code} - istio_requests_total{destination_with_code} offset 1h or istio_requests_total{destination_with_code} unless istio_requests_total{destination_with_code} offset 1h) / sum(istio_requests_total{destination_service_name} - istio_requests_total{destination_service_name} offset 1h or istio_requests_total{destination_service_name} unless istio_requests_total{destination_service_name} offset 1h)&start={start_time}&end={end_time}&step=1h'

    response = ''
    count = 0
    while response == '':
        try:
            response = requests.get(url_range, params=query)
            break
        except:
            count += 1
            print(f"Connection refused by server.. retry {count}/5")
            if count > 5:
                return False, "Connection refused by server"
            time.sleep(3)
            continue

    if cal_hour is True:
        query_hour = f'query=sum(istio_requests_total{destination_with_code} - istio_requests_total{destination_with_code} offset {offset}m or istio_requests_total{destination_with_code} unless istio_requests_total{destination_with_code} offset {offset}m) / sum(istio_requests_total{destination_service_name} - istio_requests_total{destination_service_name} offset {offset}m or istio_requests_total{destination_service_name} unless istio_requests_total{destination_service_name} offset {offset}m)'
        cal_response = ''
        count = 0
        while cal_response == '':
            try:
                cal_response = requests.get(url, params=query_hour)
                break
            except:
                count += 1
                print(f"Connection refused by server.. retry {count}/5")
                if count > 5:
                    return False, "Connection refused by server"
                time.sleep(3)
                continue
        result, hour_data = get_hour_data(cal_response)
        if result is False:
            return False, hour_data
        date_end_time = (datetime.now(tz=timezone.utc) + timedelta(hours=1)).strftime('%Y-%m-%d %H:00')
        # data['value'][date_end_time] = float(hour_data)
    else:
        date_end_time = False
        hour_data = False

    result, data = get_data(response, start_time, end_time, interval, period, "data_error_rate", date_end_time,
                            hour_data)
    if result is False:
        return False, data
    return True, data


def get_system_error_rate(inference_name, model_id, start_time, end_time, cal_hour, offset):
    url = KSERVE_API_DEFAULT_GRAFANA_ENDPOINT + "/api/v1/query"
    url_range = KSERVE_API_DEFAULT_GRAFANA_ENDPOINT + "/api/v1/query_range"

    destination_service_name = "{" + f"destination_service_name=~'{inference_name}-.*'" + "}"
    destination_with_code = "{" + f"destination_service_name=~'{inference_name}-.*',response_code=~'5.*'" + "}"

    start_time, end_time, interval, period = utils.get_interval(start_time, end_time)

    query = f'query=sum(istio_requests_total{destination_with_code} - istio_requests_total{destination_with_code} offset 1h or istio_requests_total{destination_with_code} unless istio_requests_total{destination_with_code} offset 1h) / sum(istio_requests_total{destination_service_name} - istio_requests_total{destination_service_name} offset 1h or istio_requests_total{destination_service_name} unless istio_requests_total{destination_service_name} offset 1h)&start={start_time}&end={end_time}&step=1h'

    response = ''
    count = 0
    while response == '':
        try:
            response = requests.get(url_range, params=query)
            break
        except:
            count += 1
            print(f"Connection refused by server.. retry {count}/5")
            if count > 5:
                return False, "Connection refused by server"
            time.sleep(3)
            continue

    if cal_hour is True:
        query_hour = f'query=sum(istio_requests_total{destination_with_code} - istio_requests_total{destination_with_code} offset {offset}m or istio_requests_total{destination_with_code} unless istio_requests_total{destination_with_code} offset {offset}m) / sum(istio_requests_total{destination_service_name} - istio_requests_total{destination_service_name} offset {offset}m or istio_requests_total{destination_service_name} unless istio_requests_total{destination_service_name} offset {offset}m)'
        cal_response = ''
        count = 0
        while cal_response == '':
            try:
                cal_response = requests.get(url, params=query_hour)
                break
            except:
                count += 1
                print(f"Connection refused by server.. retry {count}/5")
                if count > 5:
                    return False, "Connection refused by server"
                time.sleep(3)
                continue
        result, hour_data = get_hour_data(cal_response)
        if result is False:
            return False, hour_data
        date_end_time = (datetime.now(tz=timezone.utc) + timedelta(hours=1)).strftime('%Y-%m-%d %H:00')
        # data['value'][date_end_time] = float(hour_data)
    else:
        date_end_time = False
        hour_data = False
    result, data = get_data(response, start_time, end_time, interval, period, "system_error_rate", date_end_time,
                            hour_data)
    if result is False:
        return False, data
    return True, data


def get_median_peak_load(inference_name, model_id, start_time, end_time):
    url = KSERVE_API_DEFAULT_GRAFANA_ENDPOINT + "/api/v1/query_range"
    destination_service_name = "{" + f"destination_service_name=~'{inference_name}-.*'" + "}"

    start_time, end_time, interval, period = utils.get_interval(start_time, end_time)

    # median_query = f'query=avg_over_time(sum(istio_requests_total{destination_service_name} unless istio_requests_total{destination_service_name} offset 1m)[1h:1m]) or avg_over_time(sum(istio_requests_total{destination_service_name} - istio_requests_total{destination_service_name} offset 1m)[1h:1m])&start={start_time}&end={end_time}&step=1h'
    median_query = f'query=avg_over_time(sum(istio_requests_total{destination_service_name} - istio_requests_total{destination_service_name} offset 1m)[1h:1m])&start={start_time}&end={end_time}&step=1h'

    median_response = ''
    count = 0
    while median_response == '':
        try:
            median_response = requests.get(url, params=median_query)
            break
        except:
            print(f"Connection refused by server.. retry {count}/5")
            if count > 5:
                return False, "Connection refused by server"
            time.sleep(3)
            continue

    result, median = get_data(median_response, start_time, end_time, interval, period, "median_peak_load", False, False)
    if result is False:
        return False, median

    # peak_query = f'query=max_over_time(sum(istio_requests_total{destination_service_name} unless istio_requests_total{destination_service_name} offset 1m)[1h:1m]) or max_over_time(sum(istio_requests_total{destination_service_name} - istio_requests_total{destination_service_name} offset 1m)[1h:1m])&start={start_time}&end={end_time}&step=1h'
    peak_query = f'query=max_over_time(sum(istio_requests_total{destination_service_name} - istio_requests_total{destination_service_name} offset 1m)[1h:1m])&start={start_time}&end={end_time}&step=1h'

    peak_response = ''
    count = 0
    while peak_response == '':
        try:
            peak_response = requests.get(url, params=peak_query)
            break
        except:
            print(f"Connection refused by server.. retry {count}/5")
            if count > 5:
                return False, "Connection refused by server"
            time.sleep(3)
            continue

    result, peak = get_data(peak_response, start_time, end_time, interval, period, "median_peak_load", False, False)
    if result is False:
        return False, peak

    data = {
        "median": median,
        "peak": peak
    }
    return True, data


def get_response_time(inference_name, model_id, start_time, end_time):
    url = KSERVE_API_DEFAULT_GRAFANA_ENDPOINT + "/api/v1/query_range"
    destination_with_value = "{" + f"destination_service_name=~'{inference_name}-.*'" + "}"

    start_time, end_time, interval, period = utils.get_interval(start_time, end_time)

    query = f'query=histogram_quantile(0.5, sum by (le) (rate(istio_request_duration_milliseconds_bucket{destination_with_value}[1h])))&start={start_time}&end={end_time}&step=1h'

    response = ''
    count = 0
    while response == '':
        try:
            response = requests.get(url, params=query)
            break
        except:
            count += 1
            print(f"Connection refused by server.. retry {count}/5")
            if count > 5:
                return False, "Connection refused by server"
            time.sleep(3)
            continue

    result, data = get_data(response, start_time, end_time, interval, period, "response_time", False, False)
    if result is False:
        return False, data
    return True, data


def get_agg_total_requests(inference_name, model_id, start_time, end_time):
    # url = KSERVE_API_DEFAULT_GRAFANA_ENDPOINT + "/api/v1/query_range"
    url = KSERVE_API_DEFAULT_GRAFANA_ENDPOINT + "/api/v1/query"
    destination_service_name = "{" + f"destination_service_name=~'{inference_name}-.*'" + "}"

    step = utils.get_step(start_time, end_time, True)

    query = f'query=sum(istio_requests_total{destination_service_name} - istio_requests_total{destination_service_name} offset {step} or istio_requests_total{destination_service_name} unless istio_requests_total{destination_service_name} offset {step})&time={end_time}'

    response = ''
    count = 0
    while response == '':
        try:
            response = requests.get(url, params=query)
            break
        except:
            count += 1
            print(f"Connection refused by server.. retry {count}/5")
            if count > 5:
                return False, "Connection refused by server"
            time.sleep(3)
            continue
    text = response.text
    try:
        eval_text = eval(text)
        value = eval_text['data']['result'][0]['value'][1]
        value = round(float(value))
        if len(eval_text['data']['result']) == 0:
            raise
    except:
        return True, 0

    return True, value


def get_agg_data_error_rate(inference_name, model_id, start_time, end_time):
    url = KSERVE_API_DEFAULT_GRAFANA_ENDPOINT + "/api/v1/query"
    destination_service_name = "{" + f"destination_service_name=~'{inference_name}-.*'" + "}"
    destination_with_code = "{" + f"destination_service_name=~'{inference_name}-.*',response_code=~'4.*'" + "}"

    step = utils.get_step(start_time, end_time, True)

    query = f'query=(sum(istio_requests_total{destination_with_code} - istio_requests_total{destination_with_code} offset {step} or istio_requests_total{destination_with_code} unless istio_requests_total{destination_with_code} offset {step}))  / (sum(istio_requests_total{destination_service_name} - istio_requests_total{destination_service_name} offset {step} or istio_requests_total{destination_service_name} unless istio_requests_total{destination_service_name} offset {step}))&time={end_time}'

    response = ''
    count = 0
    while response == '':
        try:
            response = requests.get(url, params=query)
            break
        except:
            count += 1
            print(f"Connection refused by server.. retry {count}/5")
            if count > 5:
                return True, 0.000
            time.sleep(3)
            continue
    text = response.text
    try:
        eval_text = eval(text)
        value = eval_text['data']['result'][0]['value'][1]
        value = round(float(value), 3)
    except:
        return True, 0.000

    return True, value


def get_agg_system_error_rate(inference_name, model_id, start_time, end_time):
    url = KSERVE_API_DEFAULT_GRAFANA_ENDPOINT + "/api/v1/query"
    destination_service_name = "{" + f"destination_service_name=~'{inference_name}-.*'" + "}"
    destination_with_code = "{" + f"destination_service_name=~'{inference_name}-.*',response_code=~'5.*'" + "}"

    step = utils.get_step(start_time, end_time, True)

    query = f'query=sum(istio_requests_total{destination_with_code} - istio_requests_total{destination_with_code} offset {step} or istio_requests_total{destination_with_code} unless istio_requests_total{destination_with_code} offset {step})  / sum(istio_requests_total{destination_service_name} - istio_requests_total{destination_service_name} offset {step} or istio_requests_total{destination_service_name} unless istio_requests_total{destination_service_name} offset {step})&time={end_time}'
    response = ''
    count = 0
    while response == '':
        try:
            response = requests.get(url, params=query)
            break
        except:
            count += 1
            print(f"Connection refused by server.. retry {count}/5")
            if count > 5:
                return False, "Connection refused by server"
            time.sleep(3)
            continue
    text = response.text
    try:
        eval_text = eval(text)
        value = eval_text['data']['result'][0]['value'][1]
        value = round(float(value), 3)
    except:
        return True, 0.000

    return True, value


def get_agg_median_peak_load(inference_name, model_id, start_time, end_time):
    url = KSERVE_API_DEFAULT_GRAFANA_ENDPOINT + "/api/v1/query"
    destination_service_name = "{" + f"destination_service_name=~'{inference_name}-.*'" + "}"

    step = utils.get_step(start_time, end_time, True)

    # median_query = f'query=avg_over_time(sum(increase(istio_requests_total{destination_service_name}[1m:1m]))[{step}:1m])&start={start_time}&end={end_time}&step={step}'
    median_query = f"query=avg_over_time(sum(istio_requests_total{destination_service_name} - istio_requests_total{destination_service_name} offset 1m or istio_requests_total{destination_service_name} unless istio_requests_total{destination_service_name} offset 1m)[{step}:1m])&time={end_time}"

    median_response = ''
    count = 0
    while median_response == '':
        try:
            median_response = requests.get(url, params=median_query)
            break
        except:
            print(f"Connection refused by server.. retry {count}/5")
            if count > 5:
                return False, "Connection refused by server"
            time.sleep(3)
            continue

    median_text = median_response.text
    try:
        median_eval_text = eval(median_text)
        median_value = median_eval_text['data']['result'][0]['value'][1]
        median = round(float(median_value), 3)
    except:
        return True, {"median": 0, "peak": 0}

    # peak_query = f'query=max_over_time(sum(increase(istio_requests_total{destination_service_name}[1m:1m]))[{step}:1m])&start={start_time}&end={end_time}&step={step}'
    peak_query = f"query=max_over_time(sum(istio_requests_total{destination_service_name} - istio_requests_total{destination_service_name} offset 1m or istio_requests_total{destination_service_name} unless istio_requests_total{destination_service_name} offset 1m)[{step}:1m])&time={end_time}"

    peak_response = ''
    count = 0
    while peak_response == '':
        try:
            peak_response = requests.get(url, params=peak_query)
            break
        except:
            print(f"Connection refused by server.. retry {count}/5")
            if count > 5:
                return False, "Connection refused by server"
            time.sleep(3)
            continue

    peak_text = peak_response.text
    try:
        peak_eval_text = eval(peak_text)
        peak_value = peak_eval_text['data']['result'][0]['value'][1]
        peak = round(float(peak_value))
    except:
        return True, {"median": 0, "peak": 0}

    data = {
        "median": median,
        "peak": peak
    }

    return True, data


def get_agg_response_time(inference_name, model_id, start_time, end_time):
    url = KSERVE_API_DEFAULT_GRAFANA_ENDPOINT + "/api/v1/query_range"
    destination_with_value = "{" + f"destination_service_name=~'{inference_name}-.*'" + "}"

    step = utils.get_step(start_time, end_time, True)

    # response time은 start 를 1hour 뺀다.
    date_time = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%SZ") - timedelta(hours=1)
    start_time = date_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    query = f'query=histogram_quantile(0.5, sum by (le) (rate(istio_request_duration_milliseconds_bucket{destination_with_value}[{step}])))&start={start_time}&end={end_time}&step={step}'

    response = ''
    count = 0
    while response == '':
        try:
            response = requests.get(url, params=query)
            break
        except:
            count += 1
            print(f"Connection refused by server.. retry {count}/5")
            if count > 5:
                return False, "Connection refused by server"
            time.sleep(3)
            continue

    text = response.text
    try:
        eval_text = eval(text)
        values = eval_text['data']['result'][0]['values']
        s = 0
        c = 0
        for _, j in values:
            if j == "NaN":
                pass
            else:
                s += float(j)
                c += 1
        if c == 0:
            value = 0
        else:
            value = s / c
        value = round(float(value), 3)
    except:
        return True, "N/A"

    return True, value


def get_data(response, start_time, end_time, interval, period, metric, data_end_time, hour_data):
    text = response.text
    eval_text = eval(text)
    if eval_text['status'] == 'success':
        if len(eval_text['data']['result']) > 0:
            values = eval_text['data']['result'][0]['values']
            v = {
                'date': [],
                'value': []
            }
            for i, j in values:
                v['date'].append(i)
                v['value'].append(j)

        else:
            v = {
                'date': [],
                'value': []
            }
    else:
        return False, "status Error"

    time_list = pd.date_range(start_time, end_time, freq='1h')
    df = pd.DataFrame(time_list, columns=['date'])
    df['value'] = 0

    if data_end_time is not False:
        data_end_time = datetime.strptime(data_end_time, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc).timestamp()
        if data_end_time in v['date']:
            index = v['date'].index(data_end_time)
            v['value'][index] = hour_data
        else:
            v['date'].append(data_end_time)
            v['value'].append(hour_data)

    df2 = pd.DataFrame(v)
    df2['date'] = pd.to_datetime(df2['date'], unit='s', utc=True)

    df = df.merge(df2, left_on="date",
                  right_on="date",
                  how="left"
                  )

    df.set_index(['date', 'value_x', 'value_y'])
    df.pop('value_x')
    df.columns = ['date', 'value']
    df['value'] = df['value'].astype(float, errors="raise")

    df['date'].dt.tz_localize(None)

    if metric == "total_requests":
        period_df = df['value'].groupby(df['date'].dt.to_period(freq=interval)).sum()
    elif metric == "data_error_rate" or metric == "system_error_rate" or metric == "response_time":
        period_df = df['value'].groupby(df['date'].dt.to_period(freq=interval)).mean()
    else:
        period_df = df['value'].groupby(df['date'].dt.to_period(freq=interval)).max()
    new_df = pd.DataFrame(period_df, columns=['value'])

    new_df = new_df.resample(period).agg({"value": "sum"})
    new_df.index = new_df.index.astype(str)

    data = new_df.to_dict()

    return True, data


def get_hour_data(response):
    text = response.text
    eval_text = eval(text)
    if eval_text['status'] == 'success':
        if len(eval_text['data']['result']) > 0:
            value = eval_text['data']['result'][0]['value'][1]
            if value == "NaN":
                value = 0.000
            return True, value
        else:
            return True, 0.000
    else:
        return False, "status Error"


def check_status(inference_name, model_id):
    # 현재 모델 아이디 사용하지않는중.. 추후 해결예정

    url = KSERVE_API_DEFAULT_GRAFANA_ENDPOINT + "/api/v1/query"
    destination_200 = "{" + f"destination_service_name=~'{inference_name}-.*', response_code=~'2.*' " + "}"
    destination_400 = "{" + f"destination_service_name=~'{inference_name}-.*', response_code=~'4.*' " + "}"
    destination_500 = "{" + f"destination_service_name=~'{inference_name}-.*', response_code=~'5.*' " + "}"

    end_time = datetime.now(tz=timezone.utc)
    start_time = end_time - timedelta(days=1)

    end_time = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    # start_time = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    status = "unknown"

    # query_500 = f'query=(sum(istio_requests_total{destination_500} - istio_requests_total{destination_500} offset 1h)) or (sum(istio_requests_total{destination_500} unless istio_requests_total{destination_500} offset 1h))&start={start_time}&end={end_time}&step=1h'
    query_500 = f'query=sum(istio_requests_total{destination_500} - istio_requests_total{destination_500} offset 24h or istio_requests_total{destination_500} unless istio_requests_total{destination_500} offset 24h)&time={end_time}'
    response = requests.get(url, params=query_500)
    text = response.text
    eval_text = eval(text)
    if len(eval_text['data']['result']) != 0:
        if eval_text['status'] == 'success':
            if eval_text['data']['result'][0]['value'][1] != "0":
                status = "failing"
                return status

    # query_400 = f'query=(sum(istio_requests_total{destination_400} - istio_requests_total{destination_400} offset 1h)) or (sum(istio_requests_total{destination_400} unless istio_requests_total{destination_400} offset 1h))&start={start_time}&end={end_time}&step=1h'
    query_400 = f'query=sum(istio_requests_total{destination_400} - istio_requests_total{destination_400} offset 24h or istio_requests_total{destination_400} unless istio_requests_total{destination_400} offset 24h)&time={end_time}'
    response = requests.get(url, params=query_400)
    text = response.text
    eval_text = eval(text)
    if len(eval_text['data']['result']) != 0:
        if eval_text['status'] == 'success':
            if eval_text['data']['result'][0]['value'][1] != "0":
                status = "atrisk"
                return status

    # query_200 = f'query=(sum(istio_requests_total{destination_200} - istio_requests_total{destination_200} offset 1h)) or (sum(istio_requests_total{destination_200} unless istio_requests_total{destination_200} offset 1h))&start={start_time}&end={end_time}&step=1h'
    query_200 = f'query=sum(istio_requests_total{destination_200} - istio_requests_total{destination_200} offset 24h or istio_requests_total{destination_200} unless istio_requests_total{destination_200} offset 24h)&time={end_time}'
    response = requests.get(url, params=query_200)
    text = response.text
    eval_text = eval(text)

    if len(eval_text['data']['result']) != 0:
        if eval_text['status'] == 'success':
            if eval_text['data']['result'][0]['value'][1] != "0":
                status = "pass"

    return status
