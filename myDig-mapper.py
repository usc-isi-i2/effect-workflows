import sys
import json
import hashlib

if __name__ == '__main__':
    input_filename = sys.argv[1]
    output_filename = sys.argv[2]

    out = open(output_filename, 'w')
    for line in open(input_filename, 'r'):
        line_json = json.loads(line)

        out_line = {}
        type = "_".join(line_json["a"])
        out_line[type] = line_json
        out_line['doc_id'] = hashlib.sha256(line_json['uri']).hexdigest()

        out.write(json.dumps(out_line))
        out.write("\n")
