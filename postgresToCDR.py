__author__ = 'dipsy'


import psycopg2
import psycopg2.extras
import sys
from argparse import ArgumentParser
import json
import time

class DateTimeEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return int(mktime(obj.timetuple()))

        return json.JSONEncoder.default(self, obj)

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
            timestamp = str(int(time.time() * 1000))
            json_res["timestamp"] = timestamp
            json_res["content_type"] = "application/json"
            #json_res["json_rep"] = content
            json_res["raw_content"] = json.dumps(content, ,cls = DateTimeEncoder)
            json_res["_id"] = self.config.database + "_" + self.config.table + "_" + timestamp
            json_res["url"] = "http://effect.isi.edu/input/" + self.config.database + "/" + self.config.table + "/" + timestamp
            json_res["version"] = self.config.version
            json_res["team"] = "ISI"
            json_res["source_name"] = self.config.database
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
    parser.add_argument("-v", "--version", type=str, help="CDR Version", default="2.0", required=False)

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
