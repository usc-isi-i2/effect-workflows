from APIDownloader import APIDownloader
from argparse import ArgumentParser
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
import json
from dateUtil import DateUtil
'''
spark-submit --deploy-mode client \
    --py-files /home/hadoop/effect-workflows/lib/python-lib.zip \
    hgCVEZDIAPI.py \
    --outputFolder <HDFS or s3 output folder> \
    --team "hyperiongray" \
    --password <PASSWORD> \
'''
if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = HiveContext(sc)

    apiDownloader = APIDownloader(sc, sqlContext)

    parser = ArgumentParser()
    parser.add_argument("-f", "--outputFolder", type=str, help="Output foldername", required=True)
    parser.add_argument("-t", "--team", type=str, help="Team Name", required=True)
    parser.add_argument("-p", "--password", type=str, help="password for connecting to hyperion gray api", required=True)

    args = parser.parse_args()
    print ("Got arguments:", args)

    url = "https://effect.hyperiongray.com/api/ms-bulletin"
    out_file = open(args.output, 'w')

    results = apiDownloader.download_api(url, "isi", args.password)
    if results is not None:
        print "Downloaded ", len(results), " new ms bulletin data rows. Adding them to CDR"
        if len(results) > 0:
            rdd = sc.parallelize(results)
            rdd.map(lambda x: ("hg-msbulletin", json.dumps(x))).saveAsSequenceFile(args.outputFolder + "/hg-msbulletin")
            apiDownloader.load_into_cdr(results, "hg_msbulletin", args.team, "hg-msbulletin")