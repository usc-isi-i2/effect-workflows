from APIDownloader import APIDownloader
from argparse import ArgumentParser
from pyspark import SparkContext, StorageLevel
from pyspark.sql import HiveContext

'''
spark-submit --deploy-mode client \
    --py-files /home/hadoop/effect-workflows/lib/python-lib.zip \
    hgCVEAPI.py --output cve.jl \
    --password KSIDOOIWHJu8ewhui8923y8gYGuYGASYUHjksahuihIHU \
    --date 2016-10-02T12:00:00+00:00
'''

if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = HiveContext(sc)

    parser = ArgumentParser()
    parser.add_argument("-o", "--output", type=str, help="Output filename", required=True)
    parser.add_argument("-d", "--date", type=str, help="Greater than equal date", required=True)
    parser.add_argument("-p", "--password", type=str, help="password for connecting to hyperion gray api", required=True)

    args = parser.parse_args()
    print ("Got arguments:", args)

    #out_file = open(args.output, 'w')

    url = "https://effect.hyperiongray.com/api/cve/?query=" \
          "{\"vulnerability_scoring.cvss:base_metrics.cvss:generated-on-datetime\":{\"$gte\":\"" + args.date + "\"}}"


    apiDownloader = APIDownloader(sc, sqlContext)
    out_file = open(args.output, 'w')
    results = apiDownloader.download_api(url, "isi", args.password)
    if results is not None:
        #apiDownloader.write_as_json_lines(results, out_file)
        apiDownloader.load_into_cdr(results, "hg_cve")

    #out_file.close()
