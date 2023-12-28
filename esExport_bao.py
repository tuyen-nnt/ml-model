#!/usr/bin/python3

import logging
import os
import sys
import time
import datetime
import requests
import urllib3
import json
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
import math
from xlsxwriter import Workbook
from collections import OrderedDict
import random

load_dotenv(os.getenv("ENV_FILE"))

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def millify(n):
    millnames = ['', ' Thousand', ' Million', ' Billion', ' Trillion']
    n = float(n)
    millidx = max(0,min(len(millnames)-1,
                        int(math.floor(0 if n == 0 else math.log10(abs(n))/3))))

    return '{:.0f}{}'.format(n / 10**(3 * millidx), millnames[millidx])

def seconds_to_hms(seconds):
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return int(hours), int(minutes), int(seconds)

def get_env(env_name):
    if not os.getenv(env_name):
        sys.exit("Missing env $%s" % env_name)
    return os.getenv(env_name)

logging.basicConfig(filename=get_env("APP_LOG_FILE"),
                    filemode='a',
                    format=get_env("APP_LOG_FORMAT"),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.DEBUG)

data_out_handler = logging.StreamHandler(open(get_env("DATA_LOG_FILE"), 'w', buffering=20*(1024**2)))
data_out_handler.setFormatter(logging.Formatter(get_env("DATA_LOG_FORMAT")))

data_logger = logging.getLogger("data_logger")
data_logger.addHandler(data_out_handler)
data_logger.propagate = False

ES_ENDPOINT = get_env("ES_ENDPOINT")
ES_INDEX_NAME = get_env("ES_INDEX_NAME")
ES_SCROLL_TIME = get_env("ES_SCROLL_TIME")
ES_SCROLL_BATCH_SIZE = get_env("ES_SCROLL_BATCH_SIZE")

ES_USER=get_env("ES_USER")
ES_PASSWORD=get_env("ES_PASSWORD")

ES_QUERY=json.loads(get_env("ES_QUERY"))
ES_SORT=json.loads(get_env("ES_SORT"))
ES_DOCVALUE_FIELDS=json.loads(get_env("ES_DOCVALUE_FIELDS"))
ES_FIELDS=get_env("ES_FIELDS")
ES_MAX_DOC_PER_SHARD=os.getenv("ES_MAX_DOC_PER_SHARD") or ''

EXPORT_EXCEL=bool(get_env("EXPORT_EXCEL"))
EXPORT_EXCEL_FILE=get_env("EXPORT_EXCEL_FILE")

EXPORT_SAMPLE_PERCENT=int(get_env("EXPORT_SAMPLE_PERCENT"))

def remove_percent(l,n):
    return random.sample(l,int(len(l)*(1-n)))

def get_doc_count():

    export_request = requests.get(
        f"{ES_ENDPOINT}/{ES_INDEX_NAME}/_count" + (f"?terminate_after={ES_MAX_DOC_PER_SHARD}" if len(ES_MAX_DOC_PER_SHARD) > 0 else ''),
        auth=HTTPBasicAuth(ES_USER, ES_PASSWORD),
        json={"query": ES_QUERY},
        verify=False
    )

    logging.info(export_request.text)

    return export_request.json()["count"]

def output(list_doc):

    items = []

    for doc in list_doc:
        output_dict = {
            "_index": doc["_index"],
            "_id": doc["_id"],
        }

        for key in doc["fields"].keys():
            if len(doc["fields"][key]) < 2:
                output_dict[key] = doc["fields"][key][0]
            else:
                output_dict[key] = doc["fields"][key]

        data_logger.debug(json.dumps(output_dict))

        if EXPORT_EXCEL:
            items.append(OrderedDict((key, output_dict[key]) for key in sorted(output_dict.keys())))

    return items

def create_xlsx_file(file_path: str, headers: dict, items: list):
    with Workbook(file_path) as workbook:
        worksheet = workbook.add_worksheet()
        worksheet.write_row(row=0, col=0, data=headers.values())
        header_keys = list(headers.keys())
        for index, item in enumerate(items):
            row = map(lambda field_id:
                      item.get(field_id, ''),
                      header_keys
                      )
            worksheet.write_row(row=index + 1, col=0, data=row)

def export():

    start_time = time.time()

    search_body = {
        "query": ES_QUERY,
        "sort": ES_SORT,
        "fields": ES_FIELDS.split(","),
        "docvalue_fields": ES_DOCVALUE_FIELDS,
        "_source": False
    }
    logging.info("search_body: " + json.dumps(search_body))

    total_doc = get_doc_count()
    logging.info("-------------------------------------------------")
    logging.info(f"Total doc will be exported: {millify(total_doc)}")
    logging.info("-------------------------------------------------")
    logging.info("Sleeping for 5 seconds ...")
    time.sleep(5)

    search_body["size"] = int(ES_SCROLL_BATCH_SIZE)
    export_request = requests.post(
        f"{ES_ENDPOINT}/{ES_INDEX_NAME}/_search?scroll=" + ES_SCROLL_TIME + (f"&terminate_after={ES_MAX_DOC_PER_SHARD}" if len(ES_MAX_DOC_PER_SHARD) > 0 else ''),
        auth=HTTPBasicAuth(ES_USER, ES_PASSWORD),
        json=search_body,
        verify=False
    )

    exported_doc = 0
    excel_items = []

    for i in range (0, int(total_doc / int(ES_SCROLL_BATCH_SIZE)) + 10):         # Prevent infinite loop
        if export_request.status_code != 200:
            logging.error(export_request.status_code)
            logging.error(export_request.text)
            return

        export_response = export_request.json()
        logging.info(f"Elasticsearch scroll {i} took: {export_response['took']} ms")


        list_doc = export_response["hits"]["hits"]
        exported_doc = exported_doc + len(list_doc)
        if len(list_doc) == 0:
            break

        remaining_seconds = (total_doc - exported_doc) / (exported_doc / (time.time() - start_time))
        hours, minutes, seconds = seconds_to_hms(remaining_seconds)
        logging.info(f"Estimate time remain: {hours} hour(s), {minutes} minute(s), {seconds} second(s)")

        process_time_start = time.time()

        output_items = output(export_response["hits"]["hits"])

        if EXPORT_EXCEL:
            excel_items = excel_items + remove_percent(output_items, (100-EXPORT_SAMPLE_PERCENT)/100)

        logging.info(f"Process took: {int((time.time() - process_time_start)*1000)} ms")

        for j in range(0, 10):
            try:
                request_start_time = time.time()
                export_request = requests.post(
                    f"{ES_ENDPOINT}/_search/scroll",
                    auth=HTTPBasicAuth(ES_USER, ES_PASSWORD),
                    json={"scroll_id": export_response["_scroll_id"], "scroll": ES_SCROLL_TIME},
                    verify=False
                )
                logging.info(f"Request took: {int((time.time() - request_start_time) * 1000)} ms")

                if export_request.status_code != 200:
                    logging.error(export_request.text)
                    continue

                break

            except Exception as e:
                logging.error(e)
                logging.error(f"Retrying {j} after 30 seconds")
                time.sleep(30)

    if EXPORT_EXCEL:
        excel_headers = {key: key for key in (ES_FIELDS.split(",") + ["_id", "_index"])}
        sorted_excel_headers = OrderedDict((key, excel_headers[key]) for key in sorted(excel_headers.keys()))

        logging.info(f"Writing excel file {EXPORT_EXCEL_FILE} ...")
        create_xlsx_file(EXPORT_EXCEL_FILE, sorted_excel_headers, excel_items)

    total_run_time_seconds = time.time() - start_time
    hours, minutes, seconds = seconds_to_hms(total_run_time_seconds)
    logging.info(f"-- Total doc: {total_doc} ({millify(total_doc)})")
    logging.info(f"-- Total run time: {datetime.timedelta(seconds=int(total_run_time_seconds))} ({hours}h:{minutes}m:{seconds}s)")

if __name__ == '__main__':

    export()