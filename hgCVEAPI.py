import json

import datetime
import requests
from argparse import ArgumentParser
from requests.auth import HTTPBasicAuth
from urllib import urlopen
from pyhive import hive

if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument("-o", "--output", type=str, help="Output filename", required=True)
    parser.add_argument("-g", "--gte", type=str, help="Greater than equal date", required=True)
    parser.add_argument("-p", "--password", type=str, help="password for connecting to hyperion gray api", required=True)

    args = parser.parse_args()
    print ("Got arguments:", args)

    out_file = open(args.output, 'w')
    gte_date = args.gte
    #gte = "2016-10-02T14:05:39.057-05:00"
    def write_output(line):
        line = json.dumps(line)
        out_file.write(line + "\n")
    def load_data_into_hive():
        conn = hive.Connection(host="localhost", port=9083, username="effect")
        create_table = "CREATE TABLE hg_cve (raw_content STRING) STORED AS TEXTFILE"
        conn.execute(create_table)
        load_table = "LOAD DATA INPATH" + args.output + "INTO TABLE hg_cve"
        conn.execute(load_table)
        now = datetime.datetime.now()
    url = "https://effect.hyperiongray.com/api/cve/?query={\"vulnerability_scoring.cvss:base_metrics.cvss:generated-on-datetime\":{\"$gte\":\"" + gte_date + "\"}}"
    response = requests.get(url, verify=False,	auth=HTTPBasicAuth('isi', args.password))
    result = json.loads(response.text)
    for line in result:
    	write_output(line)
    load_data_into_hive()
