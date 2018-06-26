import json

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
--py-files /home/hadoop/effect-workflows/lib/python-lib.zip darknetAPIToJl.py \
--date 1970-01-01 \
--team asu \
--outputFolder hdfs://ip-172-31-19-102.us-west-2.compute.internal:8020/user/effect/data/hive/19700101
--password <APIKEY>
'''
import sys, traceback, json, requests


def login(password):
    "Login to the system and renew the apiKey"
    print "\nLogin called or ApiKey expired, relogin called..."

    data = {'userId': 'usc', 'password': password}
    response = requests.post(url='https://apigargoyle.com/GargoyleApi/login',data=data,verify=False)

    if response.status_code == 200:
        #Successful login
        json_response = response.json()
        print (json.dumps(json_response))
        """ Sample json_response. Use any parameters like expiry_time to determine when to renew apiKey or else
           renew apiKey on errorCode of 401(Unauthorized) indicating expired apiKey
        {
        "creation_time": 1504912689016,
        "twoFactor": true,
        "role": "SAdmin",
        "apiKey": "c570ff56-d85e-4f4a-b91b-ac67cf0bf1a6",
        "expiry_time": 1504999089016,
        "userId": "some_value",
        "status": 200
        } """
        return json_response['apiKey']
    else:
        #On failure, display error and exit
        print 'Relogin failed with errorcode:{} and reply:{}'.format(str(response.status_code), str(response.text))
        return ''

if __name__ == "__main__":

    sc = SparkContext()
    sqlContext = HiveContext(sc)
    parser = ArgumentParser()
    parser.add_argument("-f", "--outputFolder", type=str, help="Output foldername", required=True)
    parser.add_argument("-t", "--team", type=str, help="Team Name", required=True)
    parser.add_argument("-d", "--date", type=str, help="Greater than equal date", required=True)
    parser.add_argument("-p", "--password", type=str, help="api key", required=True)

    args = parser.parse_args()
    print ("Got arguments:", args)

    headers = {"userId" :"usc","apiKey": login('Nyd7NYLGt8DC'), "Connection" : "close"}

    def write_output_to_file(file_name, result):
        out_file = open(file_name, 'w')
        for line in result:
            line = json.dumps(line, ensure_ascii=False)
            out_file.write(line + "\n")

    def get_all_urls():
        date_filter = ''
        if(args.date != "1970-01-01"):
            date_filter = "from=" + args.date + "&to=" + args.date
        return {
            "zero-day-products": "https://apigargoyle.com/GargoyleApi/getZerodayProducts?" + date_filter,
            "hacking-items":  "https://apigargoyle.com/GargoyleApi/getHackingItems?order=scrapedDate&" + date_filter,
            "hacking-posts": "https://apigargoyle.com/GargoyleApi/getHackingPosts?order=scrapedDate&" + date_filter,
            "twitter": "https://apigargoyle.com/GargoyleApi/getTwitterData?" + date_filter,
            "exploit-db": "https://apigargoyle.com/GargoyleApi/getExploitDBData?" + date_filter,
            "dark-mentions": "https://apigargoyle.com/GargoyleApi/getDarkMentions?", #+ date_filter,
            "dark-mention-rules": "https://apigargoyle.com/GargoyleApi/getDarkMentionRules?" + date_filter,
            "dumps-s3-mapping": "https://apigargoyle.com/GargoyleApi/getDumpsS3Mapping?" + date_filter
        }

    apiDownloader = APIDownloader(sc, sqlContext)
    urls = get_all_urls()

    exception_occured = False
    max_num_tries_per_call = 3

    error = ""
    for api_name in urls:
        try:
            source = args.team + "-" + api_name
            done = False
            start = 0
            max_limit = 1000

            while done is False:
                paging_url = urls[api_name] + "&start=" + str(start) + "&limit=" + str(max_limit)
                num_tries_per_call = 0
                got_results = False

                while (num_tries_per_call < max_num_tries_per_call) and got_results is False:
                    num_results = 0
                    res = apiDownloader.download_api(paging_url, None, None, headers)

                    if (res is not None) and 'results' in res:
                        num_results = len(res['results'])
                        got_results = True
                        print api_name, ": num results:", num_results
                        if num_results > 0:
                            results = res['results']
                            print results[0]

                            #Remove CVE extractions
                            for r in results:
                                if "postCve" in r:
                                    r['postCve'] = ''
                                if "itemCve" in r:
                                    r['itemCve'] = ''
                            rdd = sc.parallelize(results)
                            apiDownloader.load_into_cdr(results, source, args.team, source)
                            rdd.map(lambda x: (source, json.dumps(x))).saveAsSequenceFile(args.outputFolder + "/" + source + "/" + str(start))
                    else:
                        print ("Got back no results JSON, num-tries:", num_tries_per_call)
                    if (num_results < max_limit) or (num_results == 0):
                        done = True
                    else:
                        start = start + num_results

                    num_tries_per_call += 1
        except:
            error = error + "\nError running " + source + ":" + str(sys.exc_info()[0])
            error += traceback.format_exc()


    if len(error) > 0:
        print "Error:", error
        raise Exception(error)