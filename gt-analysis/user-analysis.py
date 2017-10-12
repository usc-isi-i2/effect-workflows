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

def is_email(email_str):
    if email_str.find("@") != -1:
        if email_str.find(".com") != -1 or email_str.find(".org") != -1 or email_str.find(".edu"):
            return True
    return False

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
    map = {}
    isOffuscated = False
    for filepath in filenames:
        if not filepath.endswith(".json"):
            continue
        with open(filepath, 'r') as fh:
            js = json.loads(fh.read())
            events = get_events(js)

            for event in events:
                print(json.dumps(event))
                print("---------------------------------")

                if "target_entity" in event or "target_organization" in event:
                    if "target_entity" in event:
                        t = event["target_entity"]
                        if not isinstance(t, list):
                            targets = [t]
                        else:
                            targets = t
                    else:
                        targets = ["unknown"]
                    for target in targets:
                        if target in map:
                            count = map[target]
                        else:
                            count = 0
                        count += 1
                        map[target] = count
                else:
                    for target_object in event["targets"]:

                        type = target_object["identity_class"]
                        if type == "user":
                            target = target_object["name"]
                            if target.find("user") == 0:
                                target = int(target[4:])
                                isOffuscated = True
                            if target in map:
                                count = map[target]
                            else:
                                count = 0
                            count += 1
                            map[target] = count


    ordered_map = collections.OrderedDict(sorted(map.items()))
    email_count = 1
    user_count = 1
    for key in ordered_map:
        if isOffuscated:
            print "user" + str(key), "\t", map[key]
        else:
            if is_email(key):
                print "email" + str(email_count), "\t", map[key]
                email_count += 1
            else:
                print "user" + str(user_count), "\t", map[key]
                user_count += 1