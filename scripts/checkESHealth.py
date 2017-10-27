from elasticsearch import Elasticsearch
import smtplib
import subprocess
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

if __name__ == "__main__":
    from_addr = 'oozie@isi.edu'
    to_addr = ['dipsykapoor@gmail.com']

    es = Elasticsearch(['http://cloudweb01.isi.edu/es/'], http_auth=("effect", "c@use!23"), port=80)
    required_nodes = ["cloudsrch01", "cloudsrch02", "cloudsrch03", "cloudsrch04"]
    err = ""

    # Check if all nodes are present
    node_stats = es.nodes.stats(metric='name')
    nodes = node_stats["nodes"]
    for node_id in nodes:
        node = nodes[node_id]
        name = node["name"]
        required_nodes.remove(name)

    if len(required_nodes) > 0:
        err += "Required nodes:" + str(required_nodes) + " are down"

    # Check if all indexes are green - are healthy
    indices = es.cat.indices()
    for index in indices:
        index_name = index["index"]
        if index_name.startswith("effect"):
            #print index_name, index["status"], index["health"]
            if index["status"] == "open":
                if index["health"] != 'green':
                    err += "<BR>Index: " + index_name + " is unhealthy. Status: " + index["health"]

    if len(err) > 0:
        print "Email error:", err
        s = smtplib.SMTP('smtp.isi.edu')
        html = "<html><body>" + err + "</body></html>"
        msg = MIMEMultipart(
            "alternative", None, [MIMEText(html), MIMEText(html, 'html')])

        msg['Subject'] = 'URGENT: ISSUE WITH ISI CLUSTER ELASTIC SEARCH'
        msg['From'] = from_addr
        msg['To'] = ", ".join(to_addr)

        s.sendmail(from_addr, to_addr, msg.as_string())
        s.quit()

