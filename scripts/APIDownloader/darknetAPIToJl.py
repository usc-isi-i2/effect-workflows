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
--fromDate 1970-01-01 \
--team asu \
--outputFolder hdfs://ip-172-31-19-102.us-west-2.compute.internal:8020/user/effect/data/hive/19700101
--apiKey <APIKEY>
'''

if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = HiveContext(sc)
    parser = ArgumentParser()
    parser.add_argument("-f", "--fromDate", type=str, help="from date", required=True)
    parser.add_argument("-k", "--apiKey", type=str, help="api key for darknet", required=True)

    args = parser.parse_args()
    print ("Got arguments:", args)

    headers = {"userId" :"usc","apiKey": args.apiKey, "Connection" : "close"}

    def write_output_to_file(file_name, result):
        out_file = open(file_name, 'w')
        for line in result:
            line = json.dumps(line, ensure_ascii=False)
            out_file.write(line + "\n")

    def get_all_urls():
        zeroDayUrl = "https://apigargoyle.com/GargoyleApi/getZerodayProducts?limit=10000&from=" + args.fromDate
        hackingItemsUrl = "https://apigargoyle.com/GargoyleApi/getHackingItems?limit=20000&from=" + args.fromDate
        dictionaryUrl = "https://apigargoyle.com/GargoyleApi/getDictionary?limit=10000"
        clusterStatisticsUrl = "https://apigargoyle.com/GargoyleApi/getClusterStatistics?limit=10000"
        hackingPostsUrl = "https://apigargoyle.com/GargoyleApi/getHackingPosts?limit=10000&from=" + args.fromDate
        hackingThreadsUrl = "https://apigargoyle.com/GargoyleApi/getHackingThreads?limit=10000&from=" + args.fromDate
        return {"zero-day-products" : zeroDayUrl,
                "hacking-items" : hackingItemsUrl,
                "dictionary" : dictionaryUrl,
                "cluster-statistics" : clusterStatisticsUrl,
                "hacking-posts" : hackingPostsUrl,
                "hacking-threads" : hackingThreadsUrl}

    apiDownloader = APIDownloader(sc, sqlContext)
    urls = get_all_urls()
    for url in urls:
        source = args.team + "-" + url
        if (url == "hacking-threads"):
            res = apiDownloader.download_api(urls[url], None, None, headers)
            result = []
            for each_number in res['results'].keys():
                result.append(res['results'][each_number])
            if result:
                rdd = sc.parallelize(result)
                rdd.map(lambda x: (args.source, json.dumps(x))).saveAsSequenceFile(args.outputFolder + "/" + source)
                apiDownloader.load_into_cdr(result, url, args.team, source)
        else:
            res = apiDownloader.download_api(urls[url],None,None,headers)
            if res is not None:
                rdd = sc.parallelize(res['results'])
                rdd.map(lambda x: (args.source, json.dumps(x))).saveAsSequenceFile(args.outputFolder + "/" + source)
                apiDownloader.load_into_cdr(res['results'], url, args.team, source)