__author__ = 'dipsy'

from argparse import ArgumentParser
from os import listdir
from os.path import isfile, join
import json
import collections

if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument("-i", "--input", help="Input Folder", required=True)
    args = parser.parse_args()

    folder = args.input
    filenames = [join(folder, f) for f in listdir(folder) if isfile(join(folder, f))]
    object_map = {}
    all_properties = []

    def build_object(json_obj):
        type = json_obj["@type"]
        if type in object_map:
            object = object_map[type]
        else:
            object = []

        for attribute in json_obj:
            if attribute != "@explicit" and attribute != "@type" and attribute != "a":
                if not attribute in object:
                    object.append(attribute)
                if not attribute in all_properties:
                    all_properties.append(attribute)
                value = json_obj[attribute]
                if len(value.keys()) != 0:
                    build_object(value)
        object_map[type] = object

    for filepath in filenames:
        with open(filepath, 'r') as fh:
            js = json.loads(fh.read())
            build_object(js)



    num_properties = {}
    for type in object_map:
        print "--------------------------------------------------"
        print type, ":", json.dumps(object_map[type])
        num_properties[type] = len(object_map[type])

    print "--------------------------------------------------"
    print "--------------------------------------------------"
    ordered_num_properties = collections.OrderedDict(sorted(num_properties.items()))
    i = 1


    for type in ordered_num_properties:
        print i, "\t", type, "\t", num_properties[type]
        i += 1

    print "Total number of unique properties:", len(all_properties), ":", json.dumps(all_properties)