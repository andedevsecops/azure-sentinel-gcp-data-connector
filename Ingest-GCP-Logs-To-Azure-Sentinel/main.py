import base64
import argparse
import os
import pprint
import time
import json
import re
import hashlib
import hmac
import logging

from datetime import datetime
from datetime import date
from google.cloud import secretmanager

import requests
from requests.adapters import HTTPAdapter

import urllib3
##turns off the warning that is generated below because using self signed ssl cert
urllib3.disable_warnings()


def hello_pubsub(event, context):
    
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    now_time = round(time.time(),3)
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    pattern='"timestamp":\s*"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d*Z)'
    pos=re.search(pattern,pubsub_message)

    timestamp=pubsub_message[pos.start()+13:pos.end()]

    #get epoch time for timestamp from the event timestamp
    brokentime=timestamp[0:len(timestamp)-1].split(".")
    date_time_obj = datetime.strptime(brokentime[0], '%Y-%m-%dT%H:%M:%S')
    if len(brokentime)>1:
      nanos=brokentime[1]
      epoch=date_time_obj.strftime('%s')+'.'+nanos
    else:
      epoch=date_time_obj.strftime('%s')

    try:
      host=os.environ['HOST']
    except:
      host='GCPFunction'
    try:
      sourcetype=os.environ['SOURCE_TYPE']
    except:
      sourcetype='google:gcp:pubsub:message'
    try:
      source=os.environ['SOURCE_NAME']
    except:
      source=context.resource.get("name")
    try:
        indexing=os.environ['INDEX']
    except:
        indexing='False'
    
    indexname=''

    if indexing!='False':
      if indexing=='LOGNAME':
        #find the position of the logname
        st=pubsub_message.find('"logName":"') 
        #make sure logname is in the event
        if st>0:
            #find end of the logname
            end=pubsub_message.find('",',st)
            #find the tail end of the standard lognames
            st_log=pubsub_message.find('%2F',st,end)
            if st_log==-1:
                #wasn't a standard logname, use all logname
                st_log=pubsub_message.find('/logs/',st,end)
                #final check if logname exists
                if st_log>0:
                    #a logname is found, get the logname
                    logname=pubsub_message[st_log+6:end]
                else:
                    logname='NULL'
            else:
                #get the logname after %2F
                logname=pubsub_message[st_log+3:end]
            print(logname)
            
            if logname!='NULL':
                try:
                    indexname=os.environ[logname]   #get the index name from the environment variable
                except:
                    indexname=''                    #variable not set, so default to empty string or index set in another env variable       
            else:
                indexname=indexing                  #if env variable INDEX is any value other than LOGNAME, then the value here is the index name
 
    if indexname!='':
        indexname='"index":"'+indexname+'",'

    
    source=context.resource['name']
    event_message='{"time":'+ epoch +',"host":"'+host+'","source":"'+source+'","sourcetype":"'+sourcetype+'",'+indexname
    

    try:
        COMPATIBLE=os.environ['COMPATIBLE']
    except:
        COMPATIBLE='TRUE'

    if COMPATIBLE=='TRUE':
        payload='{"publish_time":'+str(now_time)+', "data":'+pubsub_message+', "attributes": {"logging.googleapis.com/timestamp":"'+timestamp+'"}}'
    else:
        #over-ride to allow raw payload through without original Splunk GCP Add-on wrapper
        payload=pubsub_message

    event_message=event_message+'"event":'+payload+'}'
    prepare_post(event_message, source)
    
def prepare_post(logdata, source):
    print("Preparing LogData to send to Log Analytics Workspace")
    try:
        workspace_id = os.environ['WORKSPACE_ID']
    except:        
        print("Unknown Error in retreiving environment variable WORKSPACE_ID")

    try:
        workspace_key = os.environ['WORKSPACE_KEY']
    except:
        print("Unknown Error in retreiving environment variable WORKSPACE_KEY")

    try:
        custom_log_table = os.environ['LAW_TABLE_NAME']
    except:
        print("Unknown Error in retreiving environment variable LAW_TABLE_NAME")

    post_data(get_secret_value(workspace_id), get_secret_value(workspace_key), logdata, custom_log_table, source)

def build_signature(workspace_id, workspace_key, date, content_length, method, content_type, resource):
    """Returns authorization header which will be used when sending data into Azure Log Analytics"""
    
    x_headers = 'x-ms-date:' + date
    string_to_hash = method + "\n" + str(content_length) + "\n" + content_type + "\n" + x_headers + "\n" + resource
    bytes_to_hash = bytes(string_to_hash, 'UTF-8')
    decoded_key = base64.b64decode(workspace_key)
    encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()).decode('utf-8')
    authorization = "SharedKey {}:{}".format(workspace_id,encoded_hash)
    return authorization

def post_data(workspace_id, workspace_key, logdata, custom_log_table, source):
    """Sends payload to Azure Log Analytics Workspace
    
    Keyword arguments:
    workspace_id -- Workspace ID obtained from Advanced Settings
    workspace_key -- Authorization header, created using build_signature
    logdata -- payload to send to Azure Log Analytics
    custom_log_table -- Azure Log Analytics table name
    """
    
    method = 'POST'
    content_type = 'application/json'
    resource = '/api/logs'
    rfc1123date = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    content_length = len(logdata)
    signature = build_signature(workspace_id, workspace_key, rfc1123date, content_length, method, content_type, resource)

    uri = 'https://' + workspace_id + '.ods.opinsights.azure.com' + resource + '?api-version=2016-04-01'

    headers = {
        'content-type': content_type,
        'Authorization': signature,
        'Log-Type': custom_log_table,
        'x-ms-date': rfc1123date
    }

    try:
      r = requests.post(uri,data=logdata, headers=headers)
    except requests.exceptions.HTTPError as errh:
      print ("Http Error:",errh)
      if errh.response.status_code<500:
        print(r.json())
      errorHandler(body,source)
    except requests.exceptions.ConnectionError as errc:
      print ("Error Connecting:",errc)
      errorHandler(body,source)
    except requests.exceptions.Timeout as errt:
      print ("Timeout Error:",errt)
      errorHandler(body,source)
    except requests.exceptions.RequestException as err:
      print ("Error: ",err)
      errorHandler(body,source)
    except:
      print("unknown Error in http post >> message content:")
      print(body.replace('\n',''))
      errorHandler(body,source)
    
def get_secret_value(secret_name):
    
    # Setup the Secret manager Client
    client = secretmanager.SecretManagerServiceClient()
    # Get the sites environment credentials
    project_id = os.environ["PROJECTID"]

    # Get the secret value  
    print(f"Retrieving Secret values for {secret_name} from Secrets Manager")
    resource_name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    try:
        response = client.access_secret_version(resource_name)
    except:
        print(f"Unknown Error in retreiving secret value: {secret_name}")
    secret_value = response.payload.data.decode('UTF-8')
    return secret_value


def errorHandler(logdata,source):
    """Publishes failed messages to Pub/Sub topic to Retry later."""

    from google.cloud import pubsub_v1
    
    try:
        project_id = os.environ['PROJECTID']
    except:
        print("Unknown Error in retreiving environment variable PROJECTID")
    
    try:
        topic_name = os.environ['RETRY_TOPIC']
    except:
        print("Unknown Error in retreiving environment variable RETRY_TOPIC")
    
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)    
    data = logdata.encode('utf-8')
    # Add url, token and source attributes to the message
    future = publisher.publish(topic_path, data, origin=source, source='gcplogIngestionPubSubFunction')