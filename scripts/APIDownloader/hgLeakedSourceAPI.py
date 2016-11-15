from APIDownloader import APIDownloader
from argparse import ArgumentParser
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext
import json

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

    url = "https://effect.hyperiongray.com/api/leaked-source/email?query=*@"
    domains = [ "alaska.edu",
                "apple.afsmith.bm",  #No results
                "bremertonhousing.org",
                "clixsense.com",
                "Empireminecraft.com", #No results
                "eurekalert.org",
                "feverclan.com",
                "floridabar.org", #No results
                "i-dressup.com",
                "jivesoftware.com",
                "justformen.com",  #No result
                "Last.fm",
                "manaliveinc.org",
                "newseasims.com",
                "saintfrancis.com",
                "ssctech.com",  #No result
                "unm.edu",  #No result
                "usc.edu", #No result
                "wpcapital.com"
            ]

    apiDownloader = APIDownloader(sc, sqlContext)

    result_rdds = list()
    for domain in domains:
        results = apiDownloader.download_api(url + domain, "isi", args.password)
        if results is not None:
            if "results" in results:
                if len(results["results"]) > 0:
                    rdd = sc.parallelize(results["results"])
                    apiDownloader.load_into_cdr(results["results"], "hg_leaked_source", args.team, "hg-leaked-source")
                    result_rdds.append(rdd)

    if len(result_rdds) > 0:
        all_rdd = result_rdds[0]
        for rdd in result_rdds[1:]:
            all_rdd = all_rdd.union(rdd)

        all_rdd.map(lambda x: ("hg-leaked-source", json.dumps(x))).saveAsSequenceFile(args.outputFolder + "/hg-leaked-source")


