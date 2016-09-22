__author__ = 'dipsy'

import sys
from argparse import ArgumentParser
import json
import time
from csvToJson import CSVToJson
import hashlib
from cdrLoader import CDRLoader

if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument("-i", "--input", help="Input filename", required=True)
    parser.add_argument("-o", "--output", type=str, help="Output filename", required=True)
    parser.add_argument("-f", "--format", help="Input Data Format - csv/json/xml/cdr", required=True)
    parser.add_argument("-n", "--source", type=str, help="Source Name", required=True)
    parser.add_argument("-s", "--separator", type=str, help="Input Separator fro CSV Files", default=",", required=False)


    args = parser.parse_args()
    print ("Got arguments:", args)

    out_file = open(args.output, 'w')
    cdrLoader = CDRLoader()

    def write_output(line):
        line = json.dumps(line)
        out_file.write(line + "\n")


    def generate_cdr_from_raw_json(raw, type):
        json_res = {}
        timestamp = str(int(time.time() * 1000))
        hash = hashlib.md5(raw).hexdigest()
        json_res["timestamp"] = timestamp
        json_res["content_type"] = type
        json_res["raw_content"] = raw
        json_res["_id"] = args.source + "_" + hash
        json_res["url"] = "http://effect.isi.edu/input/" + args.source + "/" + hash
        json_res["version"] = "2.0"
        json_res["team"] = "ISI"
        json_res["source_name"] = args.source
        return json_res


    if args.format == "csv":
        toJson = CSVToJson(config=args)
        row = toJson.next()
        while row is not None:
            cdr_row = generate_cdr_from_raw_json(json.dumps(row), "application/json")
            write_output(cdrLoader.load_from_json_object(cdr_row)[1])
            row = toJson.next()
        toJson.close()
    elif args.format == "json" or args.format == "cdr":
        input = open(args.input, 'r')
        for line in input:
            line = line.strip()
            if args.format == "json":
                cdr_row = generate_cdr_from_raw_json(line, "application/json")
            else:
                cdr_row = json.loads(line)
            write_output(cdrLoader.load_from_json_object(cdr_row)[1])
        input.close()
    elif args.format == "xml":
        input = open(args.input, 'r')
        xml = input.read()
        cdr_row = generate_cdr_from_raw_json(xml, "application/xml")
        write_output(cdrLoader.load_from_json_object(cdr_row)[1])
    out_file.close()