import json

import requests
from argparse import ArgumentParser
from requests.auth import HTTPBasicAuth
from urllib import urlopen

if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument("-o", "--output", type=str, help="Output filename", required=True)
    parser.add_argument("-g", "--gte", type=str, help="Greater than equal date", required=True)

    args = parser.parse_args()
    print ("Got arguments:", args)

    out_file = open(args.output, 'w')
    gte_date = args.gte
    #gte = "2016-10-02T14:05:39.057-05:00"
    def write_output(line):
        line = json.dumps(line)
        out_file.write(line + "\n")
    url = "https://effect.hyperiongray.com/api/cve/?query={\"vulnerability_scoring.cvss:base_metrics.cvss:generated-on-datetime\":{\"$gte\":\"" + gte_date + "\"}}"
    response = requests.get(url, verify=False,	auth=HTTPBasicAuth('isi', 'KSIDOOIWHJu8ewhui8923y8gYGuYGASYUHjksahuihIHU'))
    result = json.loads(response.text)
    for line in result:
    	write_output(line)
