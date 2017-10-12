__author__ = 'dipsy'

from argparse import ArgumentParser
from os import listdir
from os.path import isfile, join
import json
from datetime import datetime, timedelta
import collections

from math import ceil

def week_of_month(date):
        month = date.month
        week = 0
        while date.month == month:
            week += 1
            date -= timedelta(days=7)

        return week

def get_events(js):
    events = None
    if "events" in js:
        events = js["events"]
    else:
        events = []
        for element in js:
            event = element["ground_truth"]
            events.append(event)
    return events

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-i", "--input", help="Input Folder", required=True)
    args = parser.parse_args()

    folder = args.input
    filenames = [join(folder, f) for f in listdir(folder) if isfile(join(folder, f))]
    week_map = {}
    for filepath in filenames:
        if not filepath.endswith(".json"):
            continue
        with open(filepath, 'r') as fh:
            js = json.loads(fh.read())
            events = get_events(js)
            for event in events:
                #2016-11-29T14:56:31.000000Z
                print(json.dumps(event))
                print("----------------------------------------------")
                if "occurred" in event:
                    date_occured_str = event["occurred"][0:19]
                else:
                    date_occured_str = event["detected"][0:19]
                print date_occured_str
                try:
                    date_occured = datetime.strptime(date_occured_str, "%Y-%m-%d %H:%M:%S")
                except:
                    date_occured = datetime.strptime(date_occured_str, "%Y-%m-%dT%H:%M:%S")
                month = date_occured.strftime("%m")
                year = date_occured.strftime("%Y")
                day =  date_occured.strftime("%d")
                date_formatted = year + "/" + month + "/" + day #+ "-wk" + str(week_of_month(date_occured))
                if date_formatted in week_map:
                    count = week_map[date_formatted]
                else:
                    count = 0
                count += 1
                week_map[date_formatted] = count


    ordered_dates = collections.OrderedDict(sorted(week_map.items()))
    for date_formatted in ordered_dates:
        print date_formatted, "\t", week_map[date_formatted]