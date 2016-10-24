import json
import requests
from requests.auth import HTTPBasicAuth
from datetime import date

class APIDownloader:
    def __init__(self, spark_context, sql_context):
        self.sc = spark_context
        self.sqlContext = sql_context

    def download_api(self, url, username=None, password=None):
        auth = None
        if username is not None:
            auth = HTTPBasicAuth(username, password)
        response = requests.get(url, verify=False,	auth=auth)
        return json.loads(response.text)

    def write_as_json_lines(self, array, file_handler):
        for line in array:
            json.dump(line, file_handler)
            file_handler.write("\n")


    def load_into_cdr(self, data, tablename, teamname, sourcename):
        self.sqlContext.sql("DROP TABLE " + tablename)
        self.sqlContext.sql("CREATE TABLE " + tablename + "(raw_content STRING) STORED AS TEXTFILE")
        out_file = open(tablename + ".jl", "w")
        self.write_as_json_lines(data, out_file)
        out_file.close()
        self.sqlContext.sql("LOAD DATA LOCAL INPATH '" + tablename + ".jl' INTO TABLE " + tablename)
        today = date.today()
        self.sqlContext.sql("FROM " + tablename + " h "
                            "INSERT INTO TABLE cdr_auto_test PARTITION(year='" + str(today.year) + "', "
                                    "month='" + str(today.month) + "', day='" + str(today.day) + "') "
                            "SELECT concat('" + sourcename + "/', hex(hash(h.raw_content))), "
                                    "unix_timestamp(),"
                                    "h.raw_content, "
                                    "'application/json', "
                                    "concat('http://effect.isi.edu/input/" + sourcename + "',hex(hash(h.raw_content))),"
                                    "'2.0', "
                                    "'" + teamname + "', "
                                    "'" + sourcename + "'")
