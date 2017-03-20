#!/usr/bin/env python3.4

# @author Chauncey Neyman
# @author Javon Negahban

# Sends Overlord Status emails to recipients in Recipients table in OverlordDB.

import smtplib
from email.mime.text import MIMEText

from OverlordDAO import getRecipients

def email(subject, body):
  sender = "scriptstatus@dw.digitaldemocracy.org"
  s = smtplib.SMTP('localhost')
  msg = MIMEText(body)
  msg['Subject'] = subject
  msg['From'] = sender
  for r in getRecipients():
    msg['To'] = r['recipient']
    s.sendmail(sender, r['recipient'], msg.as_string())
  s.quit()

