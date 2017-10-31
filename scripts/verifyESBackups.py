import smtplib
import subprocess
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re
import os

def convert_bytes(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

if __name__ == "__main__":
    from_addr = 'oozie@isi.edu'
    to_addr = ['dipsykapoor@gmail.com']

    files = [f for f in os.listdir("/data/lockheed/upload") if re.match(r'data-2017[0-9]*.json.gz', f)]
    files.sort()
    num_files = len(files)
    size = 0
    err = ""
    prev_file = ""
    for f in files[num_files - 10:]:
        statinfo = os.stat("/data/lockheed/upload/" + f)
        print f, statinfo.st_size
        if statinfo.st_size < size:
            err += "File " + f + " did not get generated correctly. Its size is " \
                    + convert_bytes(statinfo.st_size) + "(" \
                    + str(statinfo.st_size)\
                    + ") whereas the previous file " + prev_file + " is "\
                    + convert_bytes(size) + "("\
                    + str(size) + ")<BR>"
        size = statinfo.st_size
        prev_file = f

    if len(err) > 0:
        print "Email error:", err
        s = smtplib.SMTP('smtp.isi.edu')
        html = "<html><body>" + err + "</body></html>"
        msg = MIMEMultipart(
            "alternative", None, [MIMEText(html), MIMEText(html, 'html')])

        msg['Subject'] = 'EFFECT ERROR: Error generating the ES Backup'
        msg['From'] = from_addr
        msg['To'] = ", ".join(to_addr)

        s.sendmail(from_addr, to_addr, msg.as_string())
        s.quit()

