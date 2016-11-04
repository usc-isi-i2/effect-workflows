from APIDownloader import APIDownloader
from argparse import ArgumentParser
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
import json

'''
spark-submit --deploy-mode client \
    --py-files /home/hadoop/effect-workflows/lib/python-lib.zip \
    hgCVEAPI.py \
    --output hg_cve1 \
    --outputFolder hdfs://ip-172-31-19-102.us-west-2.compute.internal:8020/user/effect/data/hive/20161002 \
    --team "hyperiongray" \
    --source "hg-cve" \
    --password <PASSWORD> \
    --date 2016-10-02T12:00:00+00:00
'''

if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = HiveContext(sc)

    parser = ArgumentParser()
    parser.add_argument("-f", "--outputFolder", type=str, help="Output foldername", required=True)
    parser.add_argument("-o", "--output", type=str, help="Output tablename", required=True)
    parser.add_argument("-t", "--team", type=str, help="Team Name", required=True)
    parser.add_argument("-s", "--source", type=str, help="Source Name", required=True)
    parser.add_argument("-d", "--date", type=str, help="Greater than equal date", required=True)
    parser.add_argument("-p", "--password", type=str, help="password for connecting to hyperion gray api", required=True)

    args = parser.parse_args()
    print ("Got arguments:", args)

    url = "https://effect.hyperiongray.com/api/cve/?query=" \
          "{\"vulnerability_scoring.cvss:base_metrics.cvss:generated-on-datetime\":{\"$gte\":\"" + args.date + "\"}}"


    apiDownloader = APIDownloader(sc, sqlContext)
    results = apiDownloader.download_api(url, "isi", args.password)
    if results is not None:
        print "Downloaded ", len(results), " new data rows. Adding them to CDR"
        rdd = sc.parallelize(results)
        rdd.map(lambda x: (args.source, json.dumps(x))).saveAsSequenceFile(args.outputFolder + "/" + args.source)
        apiDownloader.load_into_cdr(results, args.output, args.team, args.source)
