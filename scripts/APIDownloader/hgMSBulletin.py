from APIDownloader import APIDownloader
from argparse import ArgumentParser
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
import json
'''
spark-submit --deploy-mode client \
    --py-files /home/hadoop/effect-workflows/lib/python-lib.zip \
    hgMSBulletin.py \
    --outputFolder <HDFS or s3 output folder> \
    --team "hyperiongray" \
    --password <PASSWORD> \
    --date 2016-10-02T12:00:00+00:00
'''
if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = HiveContext(sc)

    apiDownloader = APIDownloader(sc, sqlContext)

    parser = ArgumentParser()
    parser.add_argument("-f", "--outputFolder", type=str, help="Output foldername", required=True)
    parser.add_argument("-t", "--team", type=str, help="Team Name", required=True)
    parser.add_argument("-d", "--date", type=str, help="Greater than equal date", required=True)
    parser.add_argument("-p", "--password", type=str, help="password for connecting to hyperion gray api", required=True)

    args = parser.parse_args()
    print ("Got arguments:", args)

    if(args.date == "1970-01-01T00:00:00Z"):
        url = "https://effect.hyperiongray.com/api/ms-bulletin/" #To get everything
    else:
        url = "https://effect.hyperiongray.com/api/ms-bulletin/updates/" + str(args.date)

    page_num = 0
    total_pages = 1
    batch_size = 100

    while page_num < total_pages:
        url_query = url + "/pages/" + str(page_num) + "?limit=" + str(batch_size)
        results_json = apiDownloader.download_api(url_query, "isi", args.password)

        if results_json is not None and "results" in results_json:
            results = results_json["results"]
            num_results = len(results)
            total_pages = results_json["total_pages"]
            print "Downloaded ", num_results, " new msbulletin data rows. Adding them to CDR. Page:", (page_num+1), " of ", total_pages
            if num_results > 0:
                apiDownloader.load_into_cdr(results, "hg_msbulletin", args.team, "hg-msbulletin")
                print "Done loading into CDR"
                print "Taking backup on S3"

                rdd = sc.parallelize(results)
                rdd.map(lambda x: ("hg-msbulletin", json.dumps(x))).saveAsSequenceFile(args.outputFolder + "/hg-msbulletin/" + str(page_num))
                print "Done taking backing on S3"
        else:
            print "No data found:", results_json
        page_num += 1