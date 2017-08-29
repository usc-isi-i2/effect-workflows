#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from elasticsearch import Elasticsearch
from os import listdir
from os.path import isfile, join
import math
import datetime

__author__ = "Tozammel Hossain"
__email__ = "tozammel@isi.edu"


def read_gsr_data(filelist, verbose=True):
    list_df = list()
    for filepath in filelist:
        # d = dp.load_json(filepath)
        # print(d)
        with open(filepath, 'r') as fh:
            # print(fh.read().decode('utf-8-sig'))
            js = json.loads(fh.read())
            # events = js['events']
            # print("#events =", len(events))
            # # df = pd.io.json.json_normalize(events[0])
            # # print(df)
            # for event in events:
            #     for key in event:
            #         print(key)

            # df = pd.DataFrame(d)
            normalized_df = pd.io.json.json_normalize(js['events'])
            # print(normalized_df)
            # print(normalized_df.columns)
            list_df.append(normalized_df)

            # for row in normalized_df.iterrows():
            #     print(row)
            # new_df = flattenColumn(normalized_df, 'events')
            # print(new_df)
    df = pd.concat(list_df)

    df['occurred'] = pd.to_datetime(df['occurred'], errors='coerce').dt.date
    #df['detected'] = pd.to_datetime(df['detected'], errors='coerce').dt.date

    return df


def get_series_event_type(df, event_type=None):
    if event_type is None:
        df_malware = df
    else:
        df_malware = df[df['event_type'] == event_type]

    # print(df_malware.head())
    # input("press a key")
    # ts_malware = df_malware.groupby('detected').size()
    ts_malware = df_malware.groupby('occurred').size()

    if ts_malware is not None:
        min_index = ts_malware.index.min()
        if type(min_index) == datetime.date:
            date_range = pd.date_range(min_index, ts_malware.index.max())
            #ts_malware = ts_malware.reindex(date_range, fill_value=0)
            ts_malware.name = 'count'
            ts_malware.index.name = 'date'
    return ts_malware


def convert_time_series_in_json(company, event_type, ts):
    data_arr = []
    ts_data = json.loads(ts.to_json(date_format='iso'))
    for date_value in ts_data:
        data = dict()
        data['company'] = company
        data['event_type'] = event_type
        data['date'] = date_value
        data['count'] = ts_data[date_value]
        data_arr.append(data)
    return data_arr


def get_curated_data(company_name, files):
    df = read_gsr_data(files)
    result = []

    ts_epmal = get_series_event_type(df, event_type='endpoint-malware')
    epmal_data = convert_time_series_in_json(company_name, "endpoint-malware", ts_epmal)
    result.extend(epmal_data)

    ts_malemail = get_series_event_type(df, event_type='malicious-email')
    malemail_data = convert_time_series_in_json(company_name, "malicious-email", ts_malemail)
    result.extend(malemail_data)

    if company_name == 'dexter':
        ts_maldest = get_series_event_type(df, event_type='malicious-url')
    else: 
        ts_maldest = get_series_event_type(df, event_type='malicious-destination')
    maldest_data = convert_time_series_in_json(company_name, "malicious-destination", ts_maldest)
    result.extend(maldest_data)

    return result

def main(argv):
    # curate ground truth data
    company_name = argv[1]
    folder = argv[2]
    es_username = argv[3]
    es_pwd = argv[4]
    es_index = argv[5]
    out_folder = argv[6]

    filenames = [join(folder, f) for f in listdir(folder) if isfile(join(folder, f))]
    print filenames
    data = get_curated_data(company_name, filenames)

    print "Auth:", es_username, ":", es_pwd
    es = Elasticsearch(['http://cloudweb01.isi.edu/es/'], http_auth=(es_username, es_pwd), port=80)
    print(es.info())

    es.indices.create(index=es_index, ignore=400)
    f = open(out_folder + "/" + company_name + ".jl", "w")
    for entry in data:
        es.index(index=es_index, doc_type="event", body=entry)
        f.write(json.dumps(entry))
        f.write("\n")
        print "Added event:", entry

if __name__ == "__main__":
    import sys

    sys.exit(main(sys.argv))
