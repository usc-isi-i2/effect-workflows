import json
import requests
from requests.auth import HTTPBasicAuth
from datetime import date
import os

class APIDownloader:
    def __init__(self, spark_context, sql_context):
        self.sc = spark_context
        self.sqlContext = sql_context

    def byteify(self,input):
        if isinstance(input, dict):
            return {self.byteify(key): self.byteify(value)
                    for key, value in input.iteritems()}
        elif isinstance(input, list):
            return [self.byteify(element) for element in input]
        elif isinstance(input, unicode):
            return input.encode('utf-8')
        else:
            return input

    def download_api(self, url, username=None, password=None, headers=None):
        auth = None
        if username is not None:
            auth = HTTPBasicAuth(username, password)
        print "Download:", url
        response = requests.get(url, verify=False,	auth=auth, headers=headers)
        return self.byteify(json.loads(response.text))

    def write_as_json_lines(self, array, file_handler):
        for line in array:
            line = json.dumps(line, ensure_ascii=False)
            file_handler.write(line + "\n")


    def load_into_cdr(self, data, tablename, teamname, sourcename):
        tablename = tablename.replace("-", "_")
        self.sqlContext.sql("DROP TABLE " + tablename)
        self.sqlContext.sql("CREATE TABLE " + tablename + "(raw_content STRING) STORED AS TEXTFILE")
        out_file = open(tablename + ".jl", "w")
        self.write_as_json_lines(data, out_file)
        out_file.close()
        self.sqlContext.sql("LOAD DATA LOCAL INPATH '" + tablename + ".jl' INTO TABLE " + tablename)
        today = date.today()
        self.sqlContext.sql("FROM " + tablename + " h "
                            "INSERT INTO TABLE cdr PARTITION(year='" + str(today.year) + "', "
                                    "month='" + str(today.month) + "', day='" + str(today.day) + "') "
                            "SELECT concat('" + sourcename + "/', hex(hash(h.raw_content))), "
                                    "unix_timestamp(),"
                                    "h.raw_content, "
                                    "'application/json', "
                                    "concat('http://effect.isi.edu/input/" + sourcename + "',hex(hash(h.raw_content))),"
                                    "'2.0', "
                                    "'" + teamname + "', "
                                    "'" + sourcename + "'")

        #Cleanup, delete the temporary file and intermediate table
        os.remove(tablename + ".jl")
        #self.sqlContext("DROP TABLE " + tablename)
