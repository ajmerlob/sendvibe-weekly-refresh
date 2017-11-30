from utilities.util import Util
u = Util()
import math
import sys
import boto3
import time
import json
import logging
import operator

class WeeklyRefresh:
  def wait_until_avail():
    pass

  def __init__(self):
    athena = boto3.client('athena')
    s3 = boto3.resource('s3')
    query = athena.get_named_query(NamedQueryId="1ea396df-fd3a-45f3-8a23-169131d4cbd7")
    QueryString = query['NamedQuery']['QueryString']
    
    response = athena.start_query_execution(
        QueryString=QueryString,
        QueryExecutionContext={
            'Database': 'emaildata'
        },
        ResultConfiguration={
            'OutputLocation': 's3://email-data-full/weekly-report/',
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        })
    
    query_id = response['QueryExecutionId']
    logging.error("QueryExecutionId: {}".format(query_id))
    s3_key = 's3://email-data-full/weekly-report/{}.csv'.format(query_id)
    
    wait_until_avail()
    obj = s3.Object('email-data-full',s3_key).get()
    weekly_data = obj['Body'].read().split("\n")[1:]
    logging.error(len(weekly_data))
    mail = {}
    for email in weekly_data:
      email_address = email[:email.find(",")].strip('"')
      if email_address not in mail:
        mail[email_address] = {"To":set([]),"From":set([])}
      payload = json.loads(email[1+email.find(","):].strip('"').replace('""','"'))
      if 'headers' not in payload:
        continue
      tos = [header['value'] for header in payload['headers'] if header['name'] in ['To','Cc','Bcc']]
      froms = [header['value'] for header in payload['headers'] if header['name'] in ['From']]
      for header in tos:
        for recipient in [x.strip() for x in header.split(",")]:
          r = u.scrub(recipient)
          if r not in mail[email_address]['To']:
            mail[email_address]['To'][r] = 0
          mail[email_address]['To'][r] += 1
      for header in froms:
        for recipient in [x.strip() for x in header.split(",")]:
          r = u.scrub(recipient)
          if r not in mail[email_address]['From']:
            mail[email_address]['From'][r] = 0
          mail[email_address]['From'][r] += 1
        
    sender_address=os.environ['SENDING_ADDRESS']
    password = os.environ['PASSWORD']
    for email_address in mail:
      sorted_to = sorted(mail[email_address]['To'].items(), key=operator.itemgetter(1))[-5:]
      sorted_from = sorted(mail[email_address]['From'].items(), key=operator.itemgetter(1))[-5:]
      message_to = "\n".join(["{} emails were addressed to {}".format(v,k) for (k,v) in sorted_to])
      message_from = "\n".join(["{} emails were sent from {}".format(v,k) for (k,v) in sorted_from])
      
      message = "From: %s\r\nSubject: %s\r\nTo: %s\r\n\r\n" % (sender_address,"SendVibe Weekly Report",email_address) + \
    """Hello from SendVibe!

This week's report is looking great!  Here is some of your activity for the week:

From:
{}

To:
{}

Best,
SendVibe

If you would like to stop using SendVibe, you can revoke SendVibe's access to your inbox at any time by visiting <https://myaccount.google.com/permissions> and clicking "Remove Access" in the SendVibe Alpha section.
""".format(message_from,message_to)
      u.mail(sender_address,email_address,message,password)

if __name__== "__main__":
  wr = WeeklyRefresh()
