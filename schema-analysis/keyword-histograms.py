__author__ = 'dipsy'

from elasticsearch import Elasticsearch
import collections
import json

if __name__ == "__main__":
    es = Elasticsearch(['http://cloudweb01.isi.edu/es/'], http_auth=("effect", "c@use!23"), port=80)
    print(es.info())

    keywords = ["Overdue Payment",
                "Invoice",
                "Unusual Activity",
                "Update",
                "your opinion",
                "your advice"]
    fields = ["startDate", "datePublished", "observedDate"]
    time_taken = 0
    for keyword in keywords:
        aggs = {}

        for field in fields:
            entry = {
                "query": {
                  "match_phrase": {
                    "_all": keyword
                  }
                },
                "size": 0,
                "aggs" : {
                    "term_over_time" : {
                        "date_histogram" : {
                            "field": field,
                            "interval" : "1M",
                            "format" : "yyyy-MM-dd"
                        }
                    }
                }
            }
            results = es.search(index="effect", body=entry)
            # print(json.dumps(results))

            buckets = results["aggregations"]["term_over_time"]["buckets"]
            for bucket in buckets:
                key = bucket["key_as_string"]
                if key in aggs:
                    aggs[key] += aggs[key] + bucket["doc_count"]
                else:
                    aggs[key] =  bucket["doc_count"]

        od = collections.OrderedDict(sorted(aggs.items()))
        print("Aggregations for ", keyword)
        for key, value in od.items():
            print(key, value)

        print("===========================================================================")