from APIDownloader import APIDownloader
from argparse import ArgumentParser
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
import json
from dateUtil import DateUtil

'''
spark-submit --deploy-mode client \
    --py-files /home/hadoop/effect-workflows/lib/python-lib.zip \
    hgAbuseCh.py \
    --outputFolder <HDFS or s3 output folder> \
    --team "hyperiongray" \
    --password <PASSWORD> \
    --date 2016-10-02T12:00:00Z
'''

if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = HiveContext(sc)

    parser = ArgumentParser()
    parser.add_argument("-f", "--outputFolder", type=str, help="Output foldername", required=True)
    parser.add_argument("-t", "--team", type=str, help="Team Name", required=True)
    parser.add_argument("-d", "--date", type=str, help="Greater than equal date", required=True)
    parser.add_argument("-p", "--password", type=str, help="password for connecting to hyperion gray api", required=True)

    args = parser.parse_args()
    print ("Got arguments:", args)

    feeds = {
        "abusech": "/api/abuse_ch",
        "lehingh": "/api/lehigh_edu",
        "phistank": "/api/phishtank"
    }
    for feed_name in feeds:
        url = feeds[feed_name]

        if(args.date == "1970-01-01T00:00:00Z"):
            url_taxii = "https://effect.hyperiongray.com" + url
        else:
            url_taxii = "https://effect.hyperiongray.com" + url + "/updates/" + str(args.date)

        apiDownloader = APIDownloader(sc, sqlContext)

        results = apiDownloader.download_api(url_taxii, "isi", args.password)
        if results is not None:
            print "Downloaded ", len(results), " new taxii data rows. Adding them to CDR"
            if len(results) > 0:
                rdd = sc.parallelize(results)
                rdd.map(lambda x: ("hg-taxii", json.dumps(x))).saveAsSequenceFile(args.outputFolder + "/hg-taxii/" + feed_name)
                apiDownloader.load_into_cdr(results, "hg_taxii", args.team, "hg-taxii")
