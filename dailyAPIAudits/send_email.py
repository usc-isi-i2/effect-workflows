import smtplib
import subprocess
from email.mime.text import MIMEText

# Open a plain text file for reading.  For this example, assume that
# the text file contains only ASCII characters.
# Create a text/plain message

cat = subprocess.Popen(["hadoop", "fs", "-cat", "/user/hive/warehouse/effect_daily_irregularitites/*"], stdout=subprocess.PIPE)
file_contents = ""
for line in cat.stdout:
    file_contents = file_contents + line + "\n"

if len(file_contents) > 0:

        msg_text = "Please find below the sources with probable inconsistencies in the number of records:" + file_contents

        from_addr = 'dipsy@isi.edu'
        to_addr = ['osuba@isi.edu', 'dipsykapoor@gmail.com']

        msg = MIMEText(msg_text)
        msg['Subject'] = 'Effect Daily API Audit'
        msg['From'] = from_addr
        msg['To'] = str(to_addr)

        # Send the message via our own SMTP server, but don't include the
        # envelope header.
        s = smtplib.SMTP('smtp.isi.edu')
        s.sendmail(from_addr, to_addr, msg.as_string())
        s.quit()

