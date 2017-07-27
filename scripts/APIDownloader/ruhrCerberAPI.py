from APIDownloader import APIDownloader
from argparse import ArgumentParser
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
import json
from sshtunnel import SSHTunnelForwarder

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

    server = SSHTunnelForwarder('134.147.203.229',
                                ssh_username='guest71037',
                                ssh_password=args.password,
                                remote_bind_address=('134.147.203.219', 18000),
                                local_bind_address=('127.0.0.1', 18000))

    if(args.date == "1970-01-01T00:00:00Z"):
        url = 'http://127.0.0.1:18000/ransomware_enrichment/api/v1/predicted_domains' #To get everything
    else:
        url = 'http://127.0.0.1:18000/ransomware_enrichment/api/v1/predicted_domains'

    server.start()
    results = apiDownloader.download_api(url)

    outputFolder = args.outputFolder + "/ruhr-cerber-domains"
    if results is not None:
        num_results = len(results)
        print "Downloaded ", num_results, " new domains from Ruhr Cerber API"
        if num_results > 0:
            apiDownloader.load_into_cdr(results, "ruhr_cerberdomains", args.team, "ruhr-cerber-domains")
            print "Done loading into CDR"

            rdd = sc.parallelize(results)
            print "Take a backup in folder:", outputFolder
            rdd.map(lambda x: ("ruhr-cerber-domains", json.dumps(x))).saveAsSequenceFile(outputFolder)
            print "Done taking backup"
    else:
        print "No data found:", results

    server.stop()
    exit(0)