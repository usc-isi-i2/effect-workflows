__author__ = 'dipsy'
import json
from datetime import date, timedelta
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
--userData False
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


    apiDownloader = APIDownloader(sc, sqlContext)
    if(args.date == "1970-01-01"):
        url = "http://cloudeffect02.isi.edu:5620?"
    else:
        url = "http://cloudeffect02.isi.edu:5620?start_date=" + str(args.date) + "&end_date=" + str(args.date) +"&"

    apiDownloader = APIDownloader(sc, sqlContext)

    page_num = 0
    total_pages = 1
    batch_size = 50

    while page_num < total_pages:
        url_query = url + "start=" + str(page_num) + "&limit=" + str(batch_size)
        results_json = apiDownloader.download_api(url_query)

        if results_json is not None and "results" in results_json:
            results = results_json["results"]
            num_results = len(results)
            total_pages = results_json["total-pages"]
            print "Downloaded ", num_results, " new news data rows. Adding them to CDR. Page:", (page_num+1), " of ", total_pages
            if num_results > 0:
                apiDownloader.load_into_cdr(results, "isi_news", args.team, "isi-news")
                print "Done loading into CDR"
                print "Taking backup on S3"

                rdd = sc.parallelize(results)
                rdd.map(lambda x: ("isi-news", json.dumps(x))).saveAsSequenceFile(args.outputFolder + "/isi-news/" + str(page_num))
                print "Done taking backing on S3"
        else:
            print "No data found:", results_json
        page_num += 1
