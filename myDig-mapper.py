import sys
import json
import hashlib

if __name__ == '__main__':
    input_filename = sys.argv[1]
    output_filename = sys.argv[2]

    out = open(output_filename, 'w')
    for line in open(input_filename, 'r'):
        line_json = json.loads(line)
        line_json['doc_id'] = hashlib.sha256(line_json['uri']).hexdigest()
        out.write(json.dumps(line_json))
        out.write("\n")
