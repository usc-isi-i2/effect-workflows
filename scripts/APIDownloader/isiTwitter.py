import json

import requests
from argparse import ArgumentParser
from requests.auth import HTTPBasicAuth
from urllib import urlopen
import re
from APIDownloader import APIDownloader
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext

'''
spark-submit --deploy-mode client  \
--files /etc/hive/conf/hive-site.xml \
--py-files /home/hadoop/effect-workflows/lib/python-lib.zip isiTwitter.py \
--date 1970-01-01 \
--team isi \
--outputFolder hdfs://ip-172-31-19-102.us-west-2.compute.internal:8020/user/effect/data/isiTwitter/20170225
'''

if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = HiveContext(sc)
    parser = ArgumentParser()
    parser.add_argument("-f", "--outputFolder", type=str, help="Output foldername", required=True)
    parser.add_argument("-t", "--team", type=str, help="Team Name", required=True)
    parser.add_argument("-d", "--date", type=str, help="Greater than equal date", required=True)
    parser.add_argument("-p", "--password", type=str, help="api key", required=False)

    args = parser.parse_args()
    print ("Got arguments:", args)
    
    def write_output_to_file(file_name, result):
        out_file = open(file_name, 'w')
        for line in result:
            line = json.dumps(line, ensure_ascii=False)
            out_file.write(line + "\n")

    def get_all_urls():
        date_filter = "from=" + args.date + "&to=" + args.date
        return {
            "twitter": "http://luxo.isi.edu:5000/getTwitterData?" + date_filter
        }

    apiDownloader = APIDownloader(sc, sqlContext)
    urls = get_all_urls()

    for api_name in urls:
        source = args.team + "-" + api_name
        done = False
        start = 0
        max_limit = 5000
        count = 0
        while done is False:
            paging_url = urls[api_name] + "&start=" + str(start) + "&limit=" + str(max_limit)
            res = apiDownloader.download_api(paging_url, None, None, None)
            total_count = res['count']
            if (res is not None) and 'results' in res:
                num_results=len(res['results'])
                count+=num_results
                print api_name, ": num results:", num_results
                if num_results > 0:
                    print res['results'][0]
                    rdd = sc.parallelize(res['results'])
                    apiDownloader.load_into_cdr(res['results'], source, args.team, source)
                    rdd.map(lambda x: (source, json.dumps(x))).saveAsSequenceFile(args.outputFolder + "/" + source + "/" + str(start))

            if (total_count == count) or (num_results == 0):
                done = True
            else:
                start = start + count