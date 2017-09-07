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

    keywords = ["cve", "microsoft", "cerber", "cisco", "phish", "malware", "security", "SQL Injection", "0day", "ddos"]

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