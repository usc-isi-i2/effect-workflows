import json
import sys

gt_filename = sys.argv[1]
f = open(gt_filename)
events = {}

json_doc = json.load(f)
recipients_map = {}
sender = {}
network_hosts = {}
files = {}
urls = {}
subjects = {}

all_events = json_doc
print("There are ", len(all_events), " events")
event_types = {"malicious-url" : 0,
               "malicious-email": 0,
               "endpoint-malware": 0,
               "malicious-destination": 0}

def make_arr(r):
    if r is None:
        return []
    if type(r) is list:
        return r
    return [r]

for event_json_outer in all_events:
    event_json = event_json_outer["ground_truth"]

    event_type = event_json["event_type"]
    event_types[event_type] += 1

    occured = event_json["occurred"]
    if "target_entity" in event_json:
        recipients = make_arr(event_json["target_entity"])
        # print(recipients)
        for r in recipients:
            if r in recipients_map:
                count = recipients_map[r]
            else:
                count = 0
            recipients_map[r] = count+1

    if "email_sender" in event_json:
        senders = make_arr(event_json["email_sender"])
        for s in senders:
            sender[s] = occured

    if "addresses" in event_json:
        addresses_array = make_arr(event_json["addresses"])
        for addr in addresses_array:
            if "ip" in addr:
                network_hosts[addr["ip"]] = occured
            if "url" in addr:
                urls[addr["url"]] = occured

    if "email_subject" in event_json:
        subject = event_json["email_subject"]
        subjects[subject] = occured


    if "files" in event_json:
        files_array = make_arr(event_json["files"])
        for file in files_array:
            if "filename" in file:
                files[file["filename"]] = occured


print("Event types:")
for event_type in event_types:
    print(event_type, ":" , event_types[event_type])

# print "---------------------------------------------------------"
# print "Recipients:"
# for r in recipients_map:
#     print r, "\t", recipients_map[r]
# print "---------------------------------------------------------"

def get_dates_array_as_string(object, attribute, prefix):
    result = []
    if isinstance(object[attribute], list):
        for d in object[attribute]:
            result.append(prefix + str(d))
    else:
        result.append(prefix + str(object[attribute]))
    return result

def print_hit(query, hit, event_type, query_date):
    source = hit["_source"]
    all_dates = []
    if "datePublished" in source:
        all_dates = get_dates_array_as_string(source, "datePublished", "date: ")
    elif "startDate" in source:
        all_dates = get_dates_array_as_string(source, "startDate", "date: ")
    elif "observedDate" in source:
        all_dates = get_dates_array_as_string(source, "observedDate", "date: ")
    elif "datePosted" in source:
        all_dates = get_dates_array_as_string(source, "datePosted", "date: ")
    elif "dateRecorded" in source:
        all_dates = get_dates_array_as_string(source, "dateRecorded", "dateRecorded: ")
    publisher = "effect-malware"
    if "publisher" in source:
        publisher = source["publisher"]

    publisher_list = []
    if isinstance(publisher, list):
        publisher_list = publisher
    else:
        publisher_list.append(publisher)

    for publisher in publisher_list:
        if publisher in publisher_hits:
            ps = publisher_hits[publisher]
        else:
            ps = {}
        hit_def = event_type + ":: " + query + ",occurred=" + query_date
        hit_matches = []
        if hit_def in ps:
            hit_matches = ps[hit_def]
        hit_matches.extend(all_dates)
        ps[hit_def] = hit_matches
        publisher_hits[publisher] = ps
    #print json.dumps(source)

# Match if the data exists anywhere
from elasticsearch import Elasticsearch

es = Elasticsearch(['http://cloudweb01.isi.edu/es/'], http_auth=('effect', 'c@use!23'), port=80)
publisher_hits = {}

# for sender_email in sender:
#
#     res = es.search(index='effect',
#               body={"query": {"match_phrase": {"_all": sender_email}}, "size":100})
#     num_results = res['hits']['total']
#     if num_results > 0:
#         print "---------------------------------------------------------"
#         print "Search sender", sender_email
#         for hit in res['hits']['hits']:
#             print_hit(sender_email, hit, "Sender Email", sender[sender_email])


for subject in subjects:

    res = es.search(index='effect',
              body={"query": {"match_phrase": {"_all": subject}}, "size":100})
    num_results = res['hits']['total']
    if num_results > 0:
        print("---------------------------------------------------------")
        print("Search Subject", subject)

        for hit in res['hits']['hits']:
            print_hit(subject, hit, "Email Subject", subjects[subject])

# for file in files:
#
#     res = es.search(index='effect',
#               body={"query": {"match_phrase": {"_all": file}}, "size":100})
#     num_results = res['hits']['total']
#     if num_results > 0:
#         print "---------------------------------------------------------"
#         print "Search file", file
#
#         for hit in res['hits']['hits']:
#             print_hit(file, hit, "File", files[file])
#
# for host in network_hosts:
#     res = es.search(index='effect',
#                     body={"query": {"match_phrase": {"_all": host}}, "size": 100})
#     num_results = res['hits']['total']
#     if num_results > 0:
#         print "---------------------------------------------------------"
#         print "Search host", host
#
#         for hit in res['hits']['hits']:
#             print_hit(host, hit, "Host", network_hosts[host])
#
# for url in urls:
#
#     res = es.search(index='effect',
#               body={"query": {"match_phrase": {"_all": url}}, "size":100})
#     num_results = res['hits']['total']
#     if num_results > 0:
#         print "---------------------------------------------------------"
#         print "Search URL", url
#
#         for hit in res['hits']['hits']:
#             print_hit(url, hit, "URL", urls[url])
#
print("\n\nSummary:")
print("---------------------------------------------------------")
for publisher in publisher_hits:
    print("--------------------------------")
    print(publisher)
    print("--------------------------------")
    for hit_def in publisher_hits[publisher]:
        dates = publisher_hits[publisher][hit_def]
        min_date = ""
        max_date = ""
        for d in dates:

            if min_date == "" or d < min_date:
                min_date = d
            if max_date == "" or d > max_date:
                max_date = d

        if min_date == max_date:
            min_max_date = min_date
        else:
            min_max_date = min_date + " to " + max_date
        print(hit_def, ", ", min_max_date)


# import urllib
# import requests

# print "ASU API Search results:"
# print "============================"
# asu_url = "https://apigargoyle.com/GargoyleApi/getHackingPosts?"
# headers = {"userId" :"usc","apiKey": "7a417415-d5f8-4883-82b6-b55c3a0e3d3a", "Connection" : "close"}
# for sender_email in sender:
#     url = asu_url + urllib.urlencode({'postContent': sender_email})
#     response = requests.get(url, verify=False,	auth=None, headers=headers, timeout=60)
#     json_response = (json.loads(response.text))
#     results = json_response["results"]
#     num_results = len(results)
#     if num_results > 0:
#         print "---------------------------------------------------------"
#         print "Search sender", sender_email
#         print(json.dumps(results))
#
# for host in network_hosts:
#     url = asu_url + urllib.urlencode({'postContent': host})
#     response = requests.get(url, verify=False,	auth=None, headers=headers, timeout=60)
#     json_response = (json.loads(response.text))
#     results = json_response["results"]
#     num_results = len(results)
#     if num_results > 0:
#         print "---------------------------------------------------------"
#         print "Search host", host
#         print(json.dumps(results))

# print "ASU Raw HTML Search results:"
# print "============================"
# asu_url = "https://apigargoyle.com/GargoyleApi/search?"
# headers = {"userId" :"usc","apiKey": "7a417415-d5f8-4883-82b6-b55c3a0e3d3a", "Connection" : "close"}
# for sender_email in sender:
#     url = asu_url + urllib.urlencode({'q': sender_email, 'size':1000})
#     response = requests.get(url, verify=False,	auth=None, headers=headers, timeout=600)
#     json_response = (json.loads(response.text))
#     # print url
#     # print(json_response)
#     num_results = json_response['hits']['total']
#     if num_results > 0:
#         print "---------------------------------------------------------"
#         print "Search sender", sender_email
#         for hit in json_response['hits']['hits']:
#             content = hit['_source']["content"]
#             if content.find(sender_email) != -1:
#                 print json.dumps(hit['_source'])
#                 break

# network_hosts = ["pub-voiture.com", "medihometown-usa.com"]
# for host in network_hosts:
#     url = asu_url + urllib.urlencode({'q': host, 'size':1000})
#     response = requests.get(url, verify=False,	auth=None, headers=headers, timeout=600)
#     json_response = (json.loads(response.text))
#     num_results = json_response['hits']['total']
#     print num_results
#     print json.dumps(json_response['hits']['hits'])
#     if num_results > 0:
#         print "---------------------------------------------------------"
#         print "Search host", host
#         for hit in json_response['hits']['hits']:
#             content = hit['_source']["content"]
#             if content.find(host) != -1:
#                 print json.dumps(hit['_source'])
#                 break