"""" Test gmail tools """

# standard
from datetime import datetime

# third party
import pandas as pd

# local
from googleapi.gmail import Gmail
from tools.html import htmlTableBody

# Create email service
gmail = Gmail('config/mail-93851bb46b8d.json', 'ben@yellow.africa')

### Test hybrid html
# Read test table
df = pd.read_csv('data/test_table.csv')
df = df[df['trn_date'] > '2019-08-16']
df = df.sort_values('trn_date',ascending=False)
df = df.fillna('')

# Create HTML body
html = htmlTableBody("This is a table", df, 'blueTable')

with open('html_table.html', 'w') as htmlfile:
    htmlfile.write(html)

# Create multimessage to send
msg = gmail.create_message(
    sender = "ben@yellow.africa",
    to = 'ben@yellow.africa',
    subject = 'test message',
    message_text = 'please open as HTML email',
    html=html,
)

# Send test
sent = gmail.send_message(msg)
# print(sent) 