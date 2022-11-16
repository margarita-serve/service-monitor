import os
import logging
import time

from dateutil import parser
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, exceptions

KSERVE_API_DEFAULT_DATABASE_ENDPOINT = os.environ.get('KSERVE_API_DEFAULT_DATABASE_ENDPOINT')

ES = Elasticsearch(KSERVE_API_DEFAULT_DATABASE_ENDPOINT)

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s]: {} %(levelname)s %(message)s'.format(os.getpid()),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    handlers=[logging.StreamHandler()])
logger = logging.getLogger()


def convertTimestamp(timeString, add_time):
    if add_time is True:
        date_time = datetime.strptime(timeString, "%Y-%m-%d:%H") + timedelta(hours=1)
    else:
        date_time = datetime.strptime(timeString, "%Y-%m-%d:%H")
    convTime = date_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    return date_time, convTime


def make_index(index_name):
    if not ES.indices.exists(index=index_name):
        mapping = {"settings": {'mapping': {'ignore_malformed': True}}}
        ES.indices.create(index=index_name, body=mapping)
        logger.info(f"Success create Index : {index_name}")


def save_data(index, name, document):
    make_index(index)
    try:
        ES.index(index=index, id=name, document=document)
        logger.info(f"Success input data in Index: {index} ID: {name}")
        return True, f"Success input data in Index: {index} ID: {name}"

    except Exception as err:
        logger.exception(err)
        return False, err


def get_interval(start_time, end_time):
    start_parse = parser.parse(start_time)
    start_time = start_parse.strftime('%Y-%m-%dT%H:00:00.000Z')
    end_parse = parser.parse(end_time)
    end_time = end_parse.strftime('%Y-%m-%dT%H:00:00.000Z')

    term = end_parse - start_parse
    term_day = term.days

    # 현재 interval 을 1시간으로 고정 후 후처리중
    if term_day < 7:
        return start_time, end_time, '1h', '1h'
    elif 7 <= term_day < 60:
        return start_time, end_time, '1d', '1d'
    elif 60 <= term_day < 730:
        return start_time, end_time, '1d', '7d'
    else:
        return start_time, end_time, '1d', '30d'


def get_step(start_time, end_time, ishour):
    start_parse = parser.parse(start_time)
    end_parse = parser.parse(end_time) + timedelta(hours=1)

    term = end_parse - start_parse
    if ishour is True:
        hour = term.total_seconds() / 3600
        if hour < 1:
            minute = term.total_seconds() / 60
            return f"{int(minute)}m"
        return f"{int(hour)}h"
    else:
        if term.seconds > 0:
            hour = term.seconds / 3600
            day = 0
        else:
            day = term.days
            hour = 0
        if day != 0:
            step = f"{int(day)}d"
        else:
            if hour == 0:
                hour = 1
            step = f"{int(hour)}h"

        return step


def search_index(index, query):
    try:
        items = ES.search(index=index, query=query, scroll='30s', size=100)
        sid = items['_scroll_id']
        fetched = items['hits']['hits']
        total = []

        for i in fetched:
            total.append(i)
        while len(fetched) > 0:
            items = ES.scroll(scroll_id=sid, scroll='30s')
            fetched = items['hits']['hits']
            for i in fetched:
                total.append(i)
            time.sleep(0.001)

        return True, total
    except exceptions.BadRequestError as err:
        return False, err
    except Exception as err:
        return False, err


def update_data(index, name, document):
    try:
        ES.update(index=index, id=name, doc=document)
        return True, f"index: {index}, id: {name} update"
    except Exception as err:
        return False, err
