__author__ = 'dipsy'

from argparse import ArgumentParser
from os import listdir
from os.path import isfile, join
import json
from datetime import datetime, timedelta
import collections

from urlparse import urlparse

def get_url_domain(url):
    parsed_uri = urlparse(url)
    domain = '{uri.netloc}'.format(uri=parsed_uri)
    return domain

# gives
'http://stackoverflow.com/'

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
                event_type= event["event_type"]
                if event_type == "malicious-email":
                    #print(json.dumps(event))
                    #print("----------------------------------------------")
                    if "occurred" in event:
                        date_occured_str = event["occurred"][0:19]
                    else:
                        date_occured_str = event["detected"][0:19]
                    try:
                        date_occured = datetime.strptime(date_occured_str, "%Y-%m-%d %H:%M:%S")
                    except:
                        date_occured = datetime.strptime(date_occured_str, "%Y-%m-%dT%H:%M:%S")
                    month = date_occured.strftime("%m")
                    year = date_occured.strftime("%Y")
                    day =  date_occured.strftime("%d")
                    occured = year + "/" + month + "/" + day
                    if "addresses" in event:
                        addresses = event["addresses"]
                        for address in addresses:
                            url = address["url"]
                            domain = get_url_domain(url)
                            if len(domain) == 0:
                                domain = "unknown"
                            if domain in map:
                                dates = map[domain]
                            else:
                                dates = []
                            if not occured in dates:
                                dates.append(occured)
                            map[domain] = dates

    ordered_map = collections.OrderedDict(sorted(map.items()))
    for key in ordered_map:
        print key, "\t", "\t".join(map[key])