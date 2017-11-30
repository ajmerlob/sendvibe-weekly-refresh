from utilities.util import Util
u = Util()
import math
import sys
import boto3
import time
import json
import logging

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
        for recipient in [x.strip() for x in header.split(",")]: mail[email_address]['To'].add(u.scrub(recipient))
      for header in froms:
        for recipient in [x.strip() for x in header.split(",")]: mail[email_address]['From'].add(u.scrub(recipient))
        
    

if __name__== "__main__":
  wr = WeeklyRefresh()
