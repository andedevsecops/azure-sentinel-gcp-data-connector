import base64
import argparse
import pprint
import json
import re
import hashlib
import hmac
import os
import requests
from requests.adapters import HTTPAdapter
import urllib3
import time
import threading
from threading import Thread
from queue import Queue
from datetime import datetime
from datetime import date
##turns off the warning that is generated below because using self signed ssl cert
urllib3.disable_warnings()


def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    try:
        TIMEOUT=int(os.environ['TIMEOUT'])-20
    except:
        TIMEOUT=220 #default timeout for pulling from pub-sub. 
        
    startTime = time.time()
    messageCount=1
    spawned=0
    while messageCount!=0:
        try:
            messageCount=synchronous_pull(os.environ['PROJECTID'],os.environ['SUBSCRIPTION'])
        except:
            messageCount=0
        if (time.time()-startTime)>TIMEOUT:
            messageCount=0
        if (messageCount>0) and (spawned==0):
            retrypushHandler()
            spawned=1 #only fire another retry once
            
            
def synchronous_pull(project_id, subscription_name):
    """Pulling messages synchronously."""
    # [START pubsub_subscriber_sync_pull]
    from google.cloud import pubsub_v1

    try:
        NUM_MESSAGES=int(os.environ['BATCH'])
    except:
        NUM_MESSAGES=100 #default pull from pub-sub

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_name)
        
    # The subscriber pulls a specific number of messages.
    response = subscriber.pull(subscription_path, max_messages=NUM_MESSAGES)
    
    ack_ids=AckMessages()
    incount=0
    outcount=0
    
    queue = Queue()
    threadcount=10
    
    if len(response.received_messages)<threadcount:
        threadcount = len(response.received_messages)

    # Create (max) 10 worker threads (no need to thread more than number of messages)
    for x in range(threadcount):
        worker = ThreadWorker(queue)
        # Set as daemon thread 
        worker.daemon = True
        worker.start()

    # Pop the messages into the thread queue 
    for received_message in response.received_messages:
        incount=incount+1         
        queue.put((received_message, ack_ids))
    # wait for the queue to finish processing all the tasks
    queue.join()    
    
    # Acknowledges the messages that were succesfully written so they will not be sent again.
    if len(ack_ids.ack_ids)>0:
        subscriber.acknowledge(subscription_path, ack_ids.ack_ids)
    outcount=len(ack_ids.ack_ids)

    print('in:'+str(incount)+' success:'+str(outcount))
    return outcount    

def prepare_post(logdata):
    print("Preparing LogData to send to Log Analytics Workspace")
    try:
        workspace_id = os.environ['WORKSPACE_ID']
    except:
        workspace_id = ""
        print("Unknown Error in retreiving environment variable WORKSPACE_ID")

    try:
        workspace_key = os.environ['WORKSPACE_KEY']
    except:
        print("Unknown Error in retreiving environment variable WORKSPACE_KEY")

    try:
        custom_log_table = os.environ['TABLE_NAME']
    except:
        print("Unknown Error in retreiving environment variable TABLE_NAME")

    post_data(workspace_id, workspace_key, logdata, custom_log_table)

def build_signature(workspace_id, workspace_key, date, content_length, method, content_type, resource):
    """Returns authorization header which will be used when sending data into Azure Log Analytics"""
    
    x_headers = 'x-ms-date:' + date
    string_to_hash = method + "\n" + str(content_length) + "\n" + content_type + "\n" + x_headers + "\n" + resource
    bytes_to_hash = bytes(string_to_hash, 'UTF-8')
    decoded_key = base64.b64decode(workspace_key)
    encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()).decode('utf-8')
    authorization = "SharedKey {}:{}".format(workspace_id,encoded_hash)
    return authorization

def post_data(workspace_id, workspace_key, logdata, custom_log_table):
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
    content_length = len(body)
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
        return False
    except requests.exceptions.ConnectionError as errc:
        print ("Error Connecting:",errc)
        return False
    except requests.exceptions.Timeout as errt:
        print ("Timeout Error:",errt)
        return False
    except requests.exceptions.RequestException as err:
        print ("Error: ",err)
        return False
    return True    

def retrypushHandler():
    """Publishes a message to Pub/Sub topic to fire another Retry"""

    from google.cloud import pubsub_v1
    
    print('spawning another handler')
    project_id = os.environ['PROJECTID']
    topic_name = os.environ['RETRY_TRIGGER_TOPIC']
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    future = publisher.publish(topic_path, 'SelfSpawn'.encode("utf-8"))

#threadsafe ack list
class AckMessages:
    def __init__(self):
        self.ack_ids = []
        self._lock = threading.Lock()

    def locked_update(self, ack_id):     
        with self._lock:
            self.ack_ids.append(ack_id)

#thread worker - calls prepare_post function
class ThreadWorker(Thread):
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # Get the payloads from the queue and expand the queue
            received_message, ack_ids = self.queue.get()
            try:
                if prepare_post(received_message.message.data):
                    ack_ids.locked_update(received_message.ack_id)
            finally:
                self.queue.task_done()