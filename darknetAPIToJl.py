import json

import datetime
import requests
from argparse import ArgumentParser
from urllib import urlopen

if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument("-a", "--api", type=str, help="API url", required=True)
    parser.add_argument("-o", "--output", type=str, help="Output filename", required=True)
    parser.add_argument("-f", "--fromDate", type=str, help="from date", required=True)
    parser.add_argument("-k", "--apiKey", type=str, help="api key for darknet", required=True)

    args = parser.parse_args()
    print ("Got arguments:", args)

    out_file = open(args.output, 'w')
    def write_output(line):
        line = json.dumps(line)
        out_file.write(line + "\n")
    url = args.api+ "?limit=10000&language=english&from=" + args.fromDate
    headers = {"userId" :"usc","apiKey": args.apiKey}
    response = requests.get(url, verify=False,	headers=headers)
    result = json.loads(response.text)
    for line in result.results:
    	write_output(line)