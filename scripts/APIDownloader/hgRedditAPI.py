from APIDownloader import APIDownloader
from argparse import ArgumentParser
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
import json
from dateUtil import DateUtil

'''
spark-submit --deploy-mode client \
    --py-files /home/hadoop/effect-workflows/lib/python-lib.zip \
    hgRedditAPI.py \
    --outputFolder <HDFS or s3 output folder> \
    --team "hyperiongray" \
    --password <PASSWORD> \
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

    if(args.date == "1970-01-01T00:00:00Z"):
        url_cve = "https://effect.hyperiongray.com/api/reddit" #To get everything
    else:
        url_cve = "https://effect.hyperiongray.com/api/reddit/updates/" + str(args.date)

    apiDownloader = APIDownloader(sc, sqlContext)

    page_num = 0
    total_pages = 1
    batch_size = 100

    while page_num < total_pages:
        url_query = url_cve + "/pages/" + str(page_num) + "?limit=" + str(batch_size)
        results_json = apiDownloader.download_api(url_query, "isi", args.password)

        if results_json is not None and "results" in results_json:
            results = results_json["results"]
            num_results = len(results)
            total_pages = results_json["total_pages"]
            print "Downloaded ", num_results, " new Reddit data rows. Adding them to CDR. Page:", (page_num+1), " of ", total_pages
            if num_results > 0:
                apiDownloader.load_into_cdr(results, "hg_reddit", args.team, "hg-reddit")
                print "Done loading into CDR"
                print "Taking backup on S3"

                rdd = sc.parallelize(results)
                rdd.map(lambda x: ("hg-reddit", json.dumps(x))).saveAsSequenceFile(args.outputFolder + "/hg-reddit/" + str(page_num))
                print "Done taking backing on S3"
        else:
            print "No data found:", results_json
        page_num += 1
