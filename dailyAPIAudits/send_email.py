import smtplib
import subprocess
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime as dt

date_of_pull=dt.datetime.today().strftime("%Y-%m-%d")
cat = subprocess.Popen(["hadoop", "fs", "-cat", "/user/hive/warehouse/daily_audit_report/date_of_pull="+date_of_pull+"/*"], stdout=subprocess.PIPE)

column_list = ["Source Name", "Number Downloaded", "Average Downloaded Last Week", "Last Date Data Received", "Average Downloaded", "Median of Data Downloaded"]
col_header=""
for col in column_list:
    col_header+="<th>"+col+"</th>"
rowData = ""
rowData+=col_header
for line in cat.stdout:
    rowDict=""
    row=line.split(",")
    rowDict+="<tr><td align='center'>"+row[0]+"</td>"
    rowDict+="<td align='center'>"+row[1]+"</td>"
    rowDict+="<td align='center'>"+row[2]+"</td>"
    rowDict+="<td align='center'>"+row[3]+"</td>"
    rowDict+="<td align='center'>"+row[4]+"</td>"
    rowDict+="<td align='center'>"+row[5]+"</td></tr>"
    rowData+=rowDict
text = """
Hi,

Summary of Effect Daily Audit Report:

{table}

Regards,

ISI"""

html = """
<html><body><p>Hi,</p>
<p><b>Summary of Effect Daily Audit Report:<b></p>
<table border="1" cellpadding="5">
{table}
</table>
<p>Please note that the Averages and Median Computations do not include days that we receive no results</p>
<p>Regards,</p>
<p>ISI</p>
</body></html>
"""
text=text.format(table=rowData)
html=html.format(table=rowData)


msg = MIMEMultipart(
    "alternative", None, [MIMEText(text), MIMEText(html,'html')])

from_addr = 'oozie@isi.edu'
to_addr = ['osuba@isi.edu','dipsy@isi.edu','bmackintosh@hyperiongray.com','knoblock@usc.edu','aagraw25@asu.edu','shak@asu.edu']

msg['Subject'] = 'Effect Daily API Audit'
msg['From'] = from_addr
msg['To'] = ", ".join(to_addr)

# Send the message via our own SMTP server, but don't include the
# envelope header.
s = smtplib.SMTP('smtp.isi.edu')
s.sendmail(from_addr, to_addr, msg.as_string())
s.quit()

