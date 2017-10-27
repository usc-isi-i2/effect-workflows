from elasticsearch import Elasticsearch

if __name__ == "__main__":

    es = Elasticsearch(['http://cloudweb01.isi.edu/es/'], http_auth=("effect", "c@use!23"), port=80)

    indices = es.cat.indices()
    aliases = es.indices.get_alias()

    for index in indices:
        index_name = index["index"]
        if index_name.startswith("effect"):
            #print index_name, index["status"], index["health"]
            if index["status"] == "open":
                alias = aliases[index_name]["aliases"]
                if not any(alias):
                    print "Close:", index_name
                    es.indices.close(index=index_name)