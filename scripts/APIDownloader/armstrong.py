from APIDownloader import APIDownloader
from argparse import ArgumentParser
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
import json
from dateUtil import DateUtil
from datetime import datetime, timedelta

'''
spark-submit --deploy-mode client \
    --py-files /home/hadoop/effect-workflows/lib/python-lib.zip \
    armstrong.py \
    --outputFolder <HDFS or s3 output folder> \
    --team "armstrong" \
    --date 2016-10-02T12:00:00Z
'''

if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = HiveContext(sc)

    parser = ArgumentParser()
    parser.add_argument("-f", "--outputFolder", type=str, help="Output foldername", required=True)
    parser.add_argument("-t", "--team", type=str, help="Team Name", required=True)
    parser.add_argument("-d", "--date", type=str, help="Greater than equal date", required=True)

    args = parser.parse_args()
    print ("Got arguments:", args)

    curr_date = args.date[:10]
    curr_date = datetime.strptime(curr_date, "%Y-%m-%d")
    prev_date = curr_date - timedelta(days=1)

    url_query = "http://cloudeffect02.isi.edu:5001/api?ds=proofpoint&createdFrom={}&createdTo={}".format(
        prev_date.isoformat(), curr_date.isoformat()
    )

    apiDownloader = APIDownloader(sc, sqlContext)
    results_json = apiDownloader.download_api(url_query)

    page_num = 0
    if isinstance(results_json, list):
        results = results_json
        num_results = len(results)
        print "Downloaded ", num_results, " new arm data rows. Adding them to CDR"
        if num_results > 0:
            apiDownloader.load_into_cdr(results, "armstrong", args.team, "armstrong")
            print "Done loading into CDR"
            print "Taking backup on S3"

            rdd = sc.parallelize(results)
            rdd.map(lambda x: ("armstrong", json.dumps(x))).saveAsSequenceFile(args.outputFolder + "/armstrong/" + str(page_num))
            print "Done taking backing on S3"
    else:
        print "No data found:", results_json
