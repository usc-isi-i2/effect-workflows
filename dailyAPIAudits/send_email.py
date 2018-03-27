import smtplib
import subprocess
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime as dt

exclude_sources = ["hackmageddon", "hg-msbulletin", "hg-taxii", "isi-twitter","isi-company-cpe-linkedin", "hg-zdi"]
highlighted_sources = ["hg-abusech", "asu-twitter", "hg-blogs", "asu-dark-mention-rules", "asu-dark-mentions",
                       "asu-hacking-items", "asu-hacking-posts", "asu-dark-mentions"]

zero_count_sources = ["asu-dark-mention-rules", "hg-abusech"]

today = dt.datetime.today()
date_of_pull = today.strftime("%Y-%m-%d")
day_minus_1 = (dt.date.today() - dt.timedelta(1)).strftime("%Y-%m-%d")
day_minus_2 = (dt.date.today() - dt.timedelta(2)).strftime("%Y-%m-%d")
day_minus_3 = (dt.date.today() - dt.timedelta(3)).strftime("%Y-%m-%d")
day_minus_4 = (dt.date.today() - dt.timedelta(4)).strftime("%Y-%m-%d")
day_minus_5 = (dt.date.today() - dt.timedelta(5)).strftime("%Y-%m-%d")
day_minus_6 = (dt.date.today() - dt.timedelta(6)).strftime("%Y-%m-%d")

cat = subprocess.Popen(
    ["hadoop", "fs", "-cat", "/user/hive/warehouse/daily_audit_report/date_of_pull=" + date_of_pull + "/*"],
    stdout=subprocess.PIPE)

other_file_date = (dt.date.today() - dt.timedelta(7)).strftime("%Y-%m-%d")
cat2 = subprocess.Popen(
    ["hadoop", "fs", "-cat", "/user/hive/warehouse/daily_audit_report/date_of_pull=" + other_file_date + "/*"],
    stdout=subprocess.PIPE)

column_list = ["Source Name", "Last Day of Pull", date_of_pull,
               day_minus_1, day_minus_2, day_minus_3, day_minus_4, day_minus_5, day_minus_6, "Average Downloaded"]

col_header = ""
for col in column_list:
    col_header += "<th align='center'>" + col + "</th>"


lines = []
highlighted_style = " style='font-weight:bold;color:blue'"

errors_per_source = {}
errors_per_source["isi"] = []
errors_per_source["asu"] = []
errors_per_source["ruhr"] = []
errors_per_source["hg"] = []

for (line, line2) in zip(cat.stdout, cat2.stdout):
    line = line.strip()
    row = line.split(",")

    line2 = line2.strip()
    row2 = line2.split(",")
    if row[0] not in exclude_sources:
        rowDict = ''
        if row[0] in highlighted_sources:
            rowDict += "<tr" + highlighted_style + ">"
        else:
            rowDict += "<tr>"
        rowDict += "<td align='center'>" + row[0] + "</td>" #Source Name
        rowDict += "<td align='center'>" + row[1] + "</td>" #last Day of pull
        rowDict += "<td align='center'>" + row[2] + "</td>" #day1
        rowDict += "<td align='center'>" + row[3] + "</td>" #day2
        rowDict += "<td align='center'>" + row[4] + "</td>" #day3
        rowDict += "<td align='center'>" + row[5] + "</td>" #day4
        rowDict += "<td align='center'>" + row[6] + "</td>" #day5
        rowDict += "<td align='center'>" + row[7] + "</td>" #day6
        rowDict += "<td align='center'>" + row[8] + "</td>" #day7
        rowDict += "<td align='center'>" + row[9] + "</td></tr>" #Average downloaded

        today_count = int(row[2])
        averge_downloaded = int(row[9])
        if today_count == 0 and averge_downloaded != 0 and row[0] not in zero_count_sources:
            #Check if this is normal, count number of times it goes zero consectutively
            counts = []
            for i in range(3, 9):
                counts.append(int(row[i]))

            for i in range(2, 9):
                counts.append(int(row2[i]))

            cur_zero_len = 0
            max_zero_len = 0

            for count in counts:
                if count == 0:
                    cur_zero_len += 1
                else:
                    cur_zero_len = 0
                if cur_zero_len > max_zero_len:
                    max_zero_len = cur_zero_len

            todays_zero_len = 1
            for count in counts:
                if count == 0:
                    todays_zero_len += 1
                else:
                    break

            print(row)
            print(todays_zero_len)
            print(max_zero_len)

            if todays_zero_len > max_zero_len:
                #Flag this source
                print(str(rowDict) + " is not getting data as expected")
                print(row)
                print(row2)
                if row[0].startswith("hg-"):
                    errors_per_source['hg'].append(rowDict)
                elif row[0].startswith("asu-"):
                    errors_per_source['asu'].append(rowDict)
                elif row[0].startswith("ruhr-"):
                    errors_per_source['ruhr'].append(rowDict)
                else:
                    errors_per_source['isi'].append(rowDict)

        lines.append(rowDict)

html = "<html><body><p>Hi,</p><p>Summary of Data Received by APIs last week:</p>"
html += "<table border='1' cellpadding='2'>"
html += "<tr>" + col_header + "</tr>"
html += "\n".join(lines)
html += "</table>"
html += "<p>Regards,</p><p>ISI</p></body></html>"

msg = MIMEMultipart(
    "alternative", None, [MIMEText(html), MIMEText(html, 'html')])

from_addr = 'dipsy@isi.edu'
to_addr = ['dipsy@isi.edu','annas@isi.edu','dipsykapoor@gmail.com']
cc_addr = []
subject = 'Effect Daily API Audit'

if today.weekday() == 6:
    #It is a Sunday, also send email to Craig and Kristina
    to_addr.append('knoblock@isi.edu')
    to_addr.append('lerman@isi.edu')
    subject = "Effect: Data Received by APIs last week"


emails = {}
emails['asu'] = ['jshak@asu.edu']
emails['hg'] = ['bmackintosh@hyperiongray.com']
emails['isi'] = ['dipsykapoor@gmail.com']
emails['ruhr'] = ['florian.quinkert@ruhr-uni-bochum.de']

emails_cc = {}
emails_cc['asu'] = ['dipsykapoor@gmail.com', 'knoblock@isi.edu', 'lerman@isi.edu','shak@asu.edu']
emails_cc['hg'] = ['dipsykapoor@gmail.com', 'knoblock@isi.edu', 'lerman@isi.edu', 'atowler@hyperiongray.com']
emails_cc['isi'] = ['knoblock@isi.edu', 'lerman@isi.edu']
emails_cc['ruhr'] = ['dipsykapoor@gmail.com', 'knoblock@isi.edu', 'lerman@isi.edu', 'thorsten.holz@rub.de']


#For testing
#to_addr = ['dipsykapoor@gmail.com']
#emails['asu'] = ['dipsykapoor@gmail.com']
#emails['hg'] = ['dipsykapoor@gmail.com']
#emails['isi'] = ['dipsykapoor@gmail.com']
#emails['ruhr'] = ['dipsykapoor@gmail.com']


msg['Subject'] = subject
msg['From'] = from_addr
msg['To'] = ", ".join(to_addr)
msg['CC'] = ", ".join(cc_addr)

# Send the message via our own SMTP server, but don't include the
# envelope header.
s = smtplib.SMTP('smtp.isi.edu')
s.sendmail(from_addr, to_addr + cc_addr, msg.as_string())
print ("Send email:", msg.as_string())

for source in errors_per_source:
    errors = errors_per_source[source]
    if len(errors) > 0:
        html = "<html><body><p>Hi,</p><p>Some of the " + source.upper() + " APIs are not returning data as expected</p>"
        html += "<table border='1' cellpadding='2'>"
        html += "<tr>" + col_header + "</tr>"
        html += "\n".join(errors)
        html += "</table>"
        html += "<p>Regards,</p><p>ISI</p></body></html>"
        msg_err = MIMEMultipart(
            "alternative", None, [MIMEText(html), MIMEText(html, 'html')])
        msg_err['Subject'] = source.upper() + ' API not returning back data as expected'
        msg_err['From'] = from_addr
        msg_err['To'] = ", ".join(emails[source])
        msg_err['CC'] = ", ".join(emails_cc[source])
        s.sendmail(from_addr, emails[source] + emails_cc[source], msg_err.as_string())
        print ("Send email:", msg_err.as_string())

s.quit()

