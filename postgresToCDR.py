__author__ = 'dipsy'


import psycopg2
import psycopg2.extras
import sys
from argparse import ArgumentParser
import json
import time

class PostgresToCDR:
    def __init__(self, config):
        self.config = config
        self.connection = psycopg2.connect(self.__get_connection_string())
        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        self.cursor.execute("SELECT * FROM " + config.table)

    def __get_connection_string(self):
        return "host='%s' dbname='%s' user='%s' password='%s'" % (self.config.host,
                                                                  self.config.database,
                                                                  self.config.user,
                                                                  self.config.password)

    def next(self):
        row = self.cursor.fetchone()
        if row is not None:
            content = dict(row)
            json_res = {}
            json_res["_url"] = "http://effect.isi.edu/input/" + self.config.database + "/" + self.config.table
            json_res["_timestamp"] = str(int(time.time() * 1000))
            json_res["_content_type"] = "application/json"
            json_res["_jsonRep"] = content
            json_res["_rawContent"] = json.dumps(content)
            return json_res

        return None

    def close(self):
        self.connection.close()


if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument("-n", "--host", help="PostgreSQL hostname", default="localhost", required=False)
    parser.add_argument("-u", "--user", type=str, help="PostgreSQL username", default="postgres", required=False)
    parser.add_argument("-p", "--password", type=str, help="PostgreSQL password", default="postgres", required=False)
    parser.add_argument("-d", "--database", type=str, help="Database name", required=True)
    parser.add_argument("-t", "--table", type=str, help="Table name", required=True)
    parser.add_argument("-o", "--output", type=str, help="Output filename", default="stdout", required=False)

    args = parser.parse_args()
    print ("Got arguments:", args)

    if args.output != 'stdout':
        out_file = open(args.output, 'w')

    def write_output(line):
        line = json.dumps(line)

        if args.output == 'stdout':
            print (line)
        else:
            out_file.write(line + "\n")

    toCDR = PostgresToCDR(config=args)
    row = toCDR.next()
    while row is not None:
        write_output(row)
        row = toCDR.next()

    toCDR.close()

    if args.output != 'stdout':
        out_file.close()