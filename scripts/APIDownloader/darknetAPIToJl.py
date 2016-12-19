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
--py-files /home/hadoop/effect-workflows/lib/python-lib.zip darknetAPIToJl.py \
--date 1970-01-01 \
--team asu \
--outputFolder hdfs://ip-172-31-19-102.us-west-2.compute.internal:8020/user/effect/data/hive/19700101
--password <APIKEY>
'''

if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = HiveContext(sc)
    parser = ArgumentParser()
    parser.add_argument("-f", "--outputFolder", type=str, help="Output foldername", required=True)
    parser.add_argument("-t", "--team", type=str, help="Team Name", required=True)
    parser.add_argument("-d", "--date", type=str, help="Greater than equal date", required=True)
    parser.add_argument("-p", "--password", type=str, help="api key", required=True)

    args = parser.parse_args()
    print ("Got arguments:", args)

    headers = {"userId" :"usc","apiKey": args.password, "Connection" : "close"}

    def write_output_to_file(file_name, result):
        out_file = open(file_name, 'w')
        for line in result:
            line = json.dumps(line, ensure_ascii=False)
            out_file.write(line + "\n")

    def get_all_urls():
        return {
            "zero-day-products": "https://apigargoyle.com/GargoyleApi/getZerodayProducts?from=" + args.date,
            "hacking-items":  "https://apigargoyle.com/GargoyleApi/getHackingItems?from=" + args.date,
            "hacking-items-cve": "https://apigargoyle.com/GargoyleApi/getVulnerabilityInfo?indicator=Item&from=" + args.date,
            "hacking-posts": "https://apigargoyle.com/GargoyleApi/getHackingPosts?from=" + args.date,
            "hacking-posts-cve": "https://apigargoyle.com/GargoyleApi/getVulnerabilityInfo?indicator=Post&from=" + args.date,
        }

    apiDownloader = APIDownloader(sc, sqlContext)
    urls = get_all_urls()
    max_limit = 10000

    for url in urls:
        source = args.team + "-" + url
        done = False
        start = 0
        rdd_result = None
        while done is False:
            paging_url = urls[url] + "&start=" + str(start) + "&limit=" + str(max_limit)
            num_results = 0
            res = apiDownloader.download_api(paging_url, None, None, headers)
            if (res is not None) and 'results' in res:
                num_results = len(res['results'])
                print url, ": num results:", num_results
                if num_results > 0:
                    rdd = sc.parallelize(res['results'])
                    if rdd_result is None:
                        rdd_result = rdd
                    else:
                        rdd_result = rdd_result.union(rdd)
                    apiDownloader.load_into_cdr(res['results'], source, args.team, source)

            if (num_results < max_limit) or (num_results == 0):
                done = True
            else:
                start = start + num_results

        if rdd_result is not None:
            rdd_result.map(lambda x: (source, json.dumps(x))).saveAsSequenceFile(args.outputFolder + "/" + source)