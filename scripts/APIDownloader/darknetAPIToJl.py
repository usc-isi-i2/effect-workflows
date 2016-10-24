import json

import requests
from argparse import ArgumentParser
from requests.auth import HTTPBasicAuth
from urllib import urlopen
import re

if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument("-f", "--fromDate", type=str, help="from date", required=True)
    parser.add_argument("-k", "--apiKey", type=str, help="api key for darknet", required=True)

    args = parser.parse_args()
    print ("Got arguments:", args)

    headers = {"userId" :"usc","apiKey": args.apiKey, "Connection" : "close"}

    def write_output_to_file(file_name, result):
        out_file = open(file_name, 'w')
        for line in result:
            line = json.dumps(line, ensure_ascii=False)
            out_file.write(line + "\n")

    def get_all_urls():
        zeroDayUrl = "https://54.186.69.219/GargoyleApi/getZerodayProducts?limit=10000&from=" + args.fromDate
        hackingItemsUrl = "https://54.186.69.219/GargoyleApi/getHackingItems?limit=20000&from=" + args.fromDate
        dictionaryUrl = "https://54.186.69.219/GargoyleApi/getDictionary?limit=10000&from=" + args.fromDate
        clusterStatisticsUrl = "https://54.186.69.219/GargoyleApi/getClusterStatistics?limit=10000&from" + args.fromDate
        hackingPostsUrl = "https://54.186.69.219/GargoyleApi/getHackingPosts?limit=10000&from=" + args.fromDate
        return [zeroDayUrl,hackingItemsUrl,dictionaryUrl,clusterStatisticsUrl,hackingPostsUrl]


    def byteify(input):
        if isinstance(input, dict):
            return {byteify(key): byteify(value)
                    for key, value in input.iteritems()}
        elif isinstance(input, list):
            return [byteify(element) for element in input]
        elif isinstance(input, unicode):
            return input.encode('utf-8')
        else:
            return input

    def get_result(url):
        response = requests.get(url, verify=False,  headers=headers)
        return byteify(json.loads(response.text))

    urls = get_all_urls()
    for url in urls:
        api_name = "zerodayproducts"
        if (re.search(api_name, url, re.IGNORECASE)):
            res = get_result(url)
            write_output_to_file(api_name + ".jl", res['results'])
        api_name = "hackingitems"
        if (re.search(api_name, url, re.IGNORECASE)):
            res = get_result(url)
            write_output_to_file(api_name + ".jl", res['results'])
        api_name = "dictionary"
        if (re.search(api_name, url, re.IGNORECASE)):
            res = get_result(url)
            write_output_to_file(api_name + ".jl", res['results'])
        api_name = "clusterstatistics"
        if (re.search(api_name, url, re.IGNORECASE)):
            res = get_result(url)
            write_output_to_file(api_name + ".jl", res['results'])
        api_name = "hackingposts"
        if (re.search(api_name, url, re.IGNORECASE)):
            res = get_result(url)
            out_file = open(api_name + ".jl", 'w')
            for each_number in res['results'].keys():
                out_file.write(json.dumps(res['results'][each_number],ensure_ascii=False))