import json
import sys

reload(sys)
sys.setdefaultencoding('utf8')

events = {}


target_entity = {}
email_sender = {}
addresses_ip = {}
addresses_url = {}
email_subject = {}
files_filename = {}
threat_actor = {}
threat_designation_family = {}
threat_designation_type = {}
attack_method = {}
detector_classification = {}

event_details = {}
event_details["target_entity"] = target_entity
event_details["email_sender"] = email_sender
event_details["addresses_ip"] =addresses_ip
event_details["addresses_url"] = addresses_url
event_details["email_subject"] = email_subject
event_details["files_filename"] = files_filename
event_details["threat_actor"] = threat_actor
event_details["threat_designation_family"] = threat_designation_family
event_details["threat_designation_type"] = threat_designation_type
event_details["attack_method"] = attack_method
event_details["detector_classification"] = detector_classification



event_types = {"malicious-url" : 0,
               "malicious-email": 0,
               "endpoint-malware": 0,
               "malicious-destination": 0,
               "atoifs": 0}


def add_attribute_to_dict(dict_obj, attribute, occured):
    if attribute in dict_obj:
        dates_arr = dict_obj[attribute]
    else:
        dates_arr = []
    dates_arr.append(occured)
    dict_obj[attribute] = dates_arr


def generate_histogram(all_events):
    for event_json_outer in all_events:
        event_json = event_json_outer["ground_truth"]

        event_type = event_json["event_type"]
        event_types[event_type] += 1

        occured = event_json["occurred"]

        if "target_entity" in event_json:
            recipients = event_json["target_entity"]
            if not isinstance(recipients, list):
                recipients = [recipients]

            for r in recipients:
                target = r
                if isinstance(r, dict):
                    if "ip" in r:
                        target = r["ip"]
                add_attribute_to_dict(target_entity, target, occured)

        if "addresses" in event_json:
            addresses_array = event_json["addresses"]
            for addr in addresses_array:
                if "ip" in addr:
                    add_attribute_to_dict(addresses_ip, addr["ip"], occured)
                if "url" in addr:
                    add_attribute_to_dict(addresses_url, addr["url"], occured)

        if "email_subject" in event_json:
            subject = event_json["email_subject"]
            add_attribute_to_dict(email_subject, subject, occured)

        if "email_sender" in event_json:
            senders = event_json["email_sender"]
            if not isinstance(senders, list):
                senders = [senders]
            for sender in senders:
                add_attribute_to_dict(email_sender, sender, occured)

        if "files" in event_json:
            files_array = event_json["files"]
            for file in files_array:
                if "filename" in file:
                    add_attribute_to_dict(files_filename, file["filename"], occured)

        if "threat_designation_family" in event_json:
            for t in event_json["threat_designation_family"]:
                add_attribute_to_dict(threat_designation_family, t, occured)

        if "threat_designation_type"  in event_json:
            for t in event_json["threat_designation_type"]:
                add_attribute_to_dict(threat_designation_type, t, occured)

        if "threat_actor" in event_json:
            actors = event_json["threat_actor"]
            for actor in actors:
                if "ip" in actor:
                    add_attribute_to_dict(threat_actor, actor["ip"], occured)

        if "attack_method" in event_json:
            for method in event_json["attack_method"]:
                add_attribute_to_dict(attack_method, method, occured)

        if "detector_classification" in event_json:
            for d in event_json["detector_classification"]:
                add_attribute_to_dict(detector_classification, d, occured)

files = ["/lfs1/effect/gt/armstrong/armstrong-mar2018-atoifs.json", "/lfs1/effect/gt/knox/knox-mar2018-atoifs.json"]
out_folder = "/lfs1/effect/gt-analysis/details-histogram/"

for filename in files:
    json_doc = json.load(open(filename))
    print "Generate Histogram for:", filename
    generate_histogram(json_doc)

for detail in event_details:
    print "Output details for:", detail
    detail_histogram = event_details[detail]
    out_file = open(out_folder + detail + ".csv", "w")
    out_file_x = open(out_folder + detail + "-numTimesValueSeen.csv", "w")
    out_file_y = open(out_folder + detail + "-numValuesSeen.csv", "w")

    attr_no = 1
    num_map = {}
    for attribute in detail_histogram:
        if attribute is not None:
            arr = detail_histogram[attribute]
            arr.sort()
            occured = ",".join(arr)
            print(attribute.encode('utf-8', 'ignore'))
            print(occured)
            out_file.write(attribute.encode('utf-8', 'ignore') + "," + occured + "\n")
            num_times = len(arr)
            str_num_times = str(num_times)
            attribute_anonymous = "attribute_" + str(attr_no)
            attr_no += 1
            out_file_x.write(attribute_anonymous + "," + str_num_times + "\n")
            if str_num_times in num_map:
                arr = num_map[str_num_times]
            else:
                arr = []
            arr.append(attribute_anonymous)
            num_map[str_num_times] = arr

    for num_times in sorted(num_map.iterkeys(), key=int):
        out_file_y.write(num_times + "," + str(len(num_map[num_times])) + "\n")

    out_file.close()
    out_file_x.close()
    out_file_y.close()

