import smtplib
import subprocess
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime as dt

exclude_sources = ["hackmageddon", "hg-msbulletin", "hg-taxii", "isi-twitter","isi-company-cpe-linkedin"]
highlighted_sources = ["hg-abusech", "asu-twitter", "hg-blogs", "asu-dark-mention-rules", "asu-dark-mentions"]

date_of_pull = dt.datetime.today().strftime("%Y-%m-%d")
day_minus_1 = (dt.date.today() - dt.timedelta(1)).strftime("%Y-%m-%d")
day_minus_2 = (dt.date.today() - dt.timedelta(2)).strftime("%Y-%m-%d")
day_minus_3 = (dt.date.today() - dt.timedelta(3)).strftime("%Y-%m-%d")
day_minus_4 = (dt.date.today() - dt.timedelta(4)).strftime("%Y-%m-%d")
day_minus_5 = (dt.date.today() - dt.timedelta(5)).strftime("%Y-%m-%d")
day_minus_6 = (dt.date.today() - dt.timedelta(6)).strftime("%Y-%m-%d")

cat = subprocess.Popen(
    ["hadoop", "fs", "-cat", "/user/hive/warehouse/daily_audit_report/date_of_pull=" + date_of_pull + "/*"],
    stdout=subprocess.PIPE)

column_list = ["Source Name", "Last Day of Pull", date_of_pull,
               day_minus_1, day_minus_2, day_minus_3, day_minus_4, day_minus_5, day_minus_6, "Average Downloaded"]

col_header = ""
for col in column_list:
    col_header += "<th align='center'>" + col + "</th>"


lines = []
highlighted_style = " style='font-weight:bold;color:blue'"
for line in cat.stdout:
    line = line.strip()
    row = line.split(",")
    if row[0] not in exclude_sources:
        rowDict = ''
        if row[0] in highlighted_sources:
            rowDict += "<tr" + highlighted_style + ">"
        else:
            rowDict += "<tr>"
        rowDict += "<td align='center'>" + row[0] + "</td>"
        rowDict += "<td align='center'>" + row[1] + "</td>"
        rowDict += "<td align='center'>" + row[2] + "</td>"
        rowDict += "<td align='center'>" + row[3] + "</td>"
        rowDict += "<td align='center'>" + row[4] + "</td>"
        rowDict += "<td align='center'>" + row[5] + "</td>"
        rowDict += "<td align='center'>" + row[6] + "</td>"
        rowDict += "<td align='center'>" + row[7] + "</td>"
        rowDict += "<td align='center'>" + row[8] + "</td>"
        rowDict += "<td align='center'>" + row[9] + "</td></tr>"
        lines.append(rowDict)

html = "<html><body><p>Hi,</p><p>Summary of Effect Daily Audit Report:</p>"
html += "<table border='1' cellpadding='2'>"
html += "<tr>" + col_header + "</tr>"
html += "\n".join(lines)
html += "</table>"
html += "<p>Regards,</p><p>ISI</p></body></html>"

msg = MIMEMultipart(
    "alternative", None, [MIMEText(html), MIMEText(html, 'html')])

from_addr = 'oozie@isi.edu'
to_addr = ['osuba@isi.edu','dipsy@isi.edu','annas@isi.edu','dipsykapoor@gmail.com']

msg['Subject'] = 'Effect Daily API Audit'
msg['From'] = from_addr
msg['To'] = ", ".join(to_addr)
# Send the message via our own SMTP server, but don't include the
# envelope header.
s = smtplib.SMTP('smtp.isi.edu')
s.sendmail(from_addr, to_addr, msg.as_string())
s.quit()

