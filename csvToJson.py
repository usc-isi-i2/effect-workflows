__author__ = 'dipsy'

import sys
from argparse import ArgumentParser
import json
import time

class CSVToJson:
    def __init__(self, config):
        self.config = config
        self.input = open(config.input, 'r')
        header = self.input.readline().strip()
        self.separator = config.separator
        self.header = header.split(self.separator)
        print self.header

    def next(self):
        line = self.input.readline()
        if line is not None:
            row = line.strip().split(self.separator)
            if len(row) > 0:
                if len(row) == 1 and row[0] == "":
                    return None

                json_rep = {}
                col_index = 0
                # print row
                for column_name in self.header:
                    json_rep[column_name] = row[col_index]
                    col_index += 1
                    if col_index == len(row):
                        break
                return json_rep

        return None

    def close(self):
        self.input.close()


if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument("-i", "--input", help="Input filename", required=True)
    parser.add_argument("-o", "--output", type=str, help="Output filename", required=True)
    parser.add_argument("-s", "--separator", type=str, help="Input Seperator", default=",", required=False)

    args = parser.parse_args()
    print ("Got arguments:", args)

    out_file = open(args.output, 'w')

    def write_output(line):
        line = json.dumps(line)
        out_file.write(line + "\n")

    toJson = CSVToJson(config=args)
    row = toJson.next()
    while row is not None:
        write_output(row)
        row = toJson.next()

    toJson.close()

    out_file.close()