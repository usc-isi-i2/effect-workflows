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

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-i", "--input", help="Input Folder", required=True)
    args = parser.parse_args()

    folder = args.input
    filenames = [join(folder, f) for f in listdir(folder) if isfile(join(folder, f))]
    map = {}
    for filepath in filenames:
        with open(filepath, 'r') as fh:
            js = json.loads(fh.read())
            events = js["events"]

            for event in events:
                print(json.dumps(event))
                print("---------------------------------")

                if "detector" in event or "detector_classification" in event:
                    if "detector" in event:
                        sensor = event["detector"]
                    else:
                        sensor = "unknown"
                    if "detector_classification" in event:
                        sensor = sensor + ": " + json.dumps(event["detector_classification"])

                    if sensor in map:
                        count = map[sensor]
                    else:
                        count = 0

                    count += 1
                    map[sensor] = count
                else:
                    for details in event["event_details"]:
                        if type(details) != list:
                            details = [details]
                        for detail in details:
                            if "sensor" in detail:
                                sensor = detail["sensor"]
                                if "sensor_classification" in detail:
                                    sensor = sensor + ": " + detail["sensor_classification"]

                                if sensor in map:
                                    count = map[sensor]
                                else:
                                    count = 0

                                count += 1
                                map[sensor] = count


    ordered_map = collections.OrderedDict(sorted(map.items()))
    for key in ordered_map:
        print key, "\t", map[key]