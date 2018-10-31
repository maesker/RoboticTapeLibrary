__author__ = 'maesker'

import smtplib
import getpass
import socket
import datetime
from email.mime.text import MIMEText


class SMTP_client:

    def __init__(self, pw=None):
        self._gmx_smtp_server = "mail.gmx.net"
        self._gmx_smtp_tls_port = 587
        self.account = "maesker@gmx.net"
        self.recipient = ["maesker@uni-mainz.de"]
        if not pw:
            self.pw = getpass.getpass()
        else:
            self.pw = pw

    def send(self, subject, content):
        dt = datetime.datetime.now()
        email = MIMEText(content)
        email['Subject'] = "[%s %s] %s" % (
            socket.gethostname(), dt.strftime('%Y-%m-%d#%H:%M:%S'), subject)
        email['From'] = self.account
        email['To'] = self.recipient[0]

        server = smtplib.SMTP(self._gmx_smtp_server, self._gmx_smtp_tls_port)
#        print server.set_debuglevel(1)
        server.ehlo()
        server.starttls()
        server.login(self.account, self.pw)
        server.sendmail(self.account, self.recipient, email.as_string())


if __name__ == "__main__":
    c = SMTP_client()
    c.send("Test subject", "Test Content")
