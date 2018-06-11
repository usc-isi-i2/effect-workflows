__author__ = 'dipsy'

from elasticsearch import Elasticsearch

def main(argv):
    # curate ground truth data
    company_name = argv[1]
    folder = argv[2]
    es_username = argv[3]
    es_pwd = argv[4]
    es_index = argv[5]
    out_folder = argv[6]

    filenames = [join(folder, f) for f in listdir(folder) if isfile(join(folder, f))]
    print filenames
    data = get_curated_data(company_name, filenames)

    print "Auth:", es_username, ":", es_pwd
    es = Elasticsearch(['http://cloudweb01.isi.edu/es/'], http_auth=(es_username, es_pwd), port=80)
    print(es.info())

    es.indices.create(index=es_index, ignore=400)
    f = open(out_folder + "/" + company_name + ".jl", "w")
    for entry in data:
        es.index(index=es_index, doc_type="event", body=entry)
        f.write(json.dumps(entry))
        f.write("\n")
        print "Added event:", entry

if __name__ == "__main__":
    import sys
    es = Elasticsearch(['http://cloudweb01.isi.edu/es/'], http_auth=("effect", "c@use!23"), port=80)
    print(es.info())

    keywords = ["CVE-2017", "adobe", "zeroday", "wannaCry", "ransomeware", "bitcoin"]

    time_taken = 0
    for keyword in keywords:
        entry = {
          "query": {
            "match_phrase": {
              "_all": keyword
            }
          }
        }
        results = es.search(index="effect", body=entry)
        time_taken += results["took"]

    print "Average query time:", (time_taken/len(keywords)),  "msec"

    #Other evaluation queries
    #1. Find a specific CVE
    entry = {
      "query": {
        "match": {
          "uri": "http://effect.isi.edu/data/vulnerability/CVE-2011-2140"
        }
      }
    }
    results = es.search(index="effect", body=entry, doc_type="vulnerability")
    print "Find a specific CVE:", results["took"], "msec"

    #2. Find all mentions of a MS Bulletin
    entry = {
              "query": {
                "match_phrase": {
                  "mentionsSecurityUpdate.name.raw": "MS17-010"
                }
              }
            }
    results = es.search(index="effect", body=entry)
    print "Find all mentions of a MS Bulletin in all sources:", results["took"], "msec"

    entry = {
          "query": {
            "match_phrase": {
              "name": "Locky"
            }
          },
          "sort": [
            {
              "observedDate": {
                "order": "desc"
              }
            }
          ]
        }
    results = es.search(index="effect", body=entry, doc_type="malware")
    print "Get all Malware named `Locky` and sort results by observedDate:", results["took"], "msec"

    entry = {
      "query": {
        "bool": {
          "must": [
            {
              "match": {
                "text": "microsoft"
              }
            },
            {
              "range": {
                "datePublished": {
                  "gte": "01/01/2017",
                  "lte": "01/08/2017",
                  "format": "MM/dd/yyyy"
                }
              }
            }
          ]
        }
      }
    }
    results = es.search(index="effect", body=entry, doc_type="malware")
    print "Get all Blogs mentioning keyword `microsoft` within a date range:", results["took"], "msec"

    entry = {
      "query": {
        "match_all": {}
      },
      "size": 0,
      "aggs": {
        "source_agg": {
          "terms": {
            "field": "publisher",
            "size": 100
          }
        }
      }
    }
    results = es.search(index="effect", body=entry)
    print "Aggregate and give document counts for each publisher / Sensor:", results["took"], "msec"