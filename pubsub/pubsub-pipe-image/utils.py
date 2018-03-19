#!/usr/bin/env python
# Copyright 2015 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This file contains some utilities used for processing tweet data and writing
data to BigQuery
"""

import collections
import datetime
import time


import dateutil.parser
from google.cloud.pubsub import types


from google.oauth2 import service_account
SCOPES = ['https://www.googleapis.com/auth/bigquery',
          'https://www.googleapis.com/auth/pubsub']
NUM_RETRIES = 3



def get_credentials():
    """Get the Google credentials needed to access our services."""

    credentials = service_account.Credentials.from_service_account_file(
        './gcp_tw_bq_ps_creds.json')
    if credentials.requires_scopes:
        credentials = credentials.with_scopes(SCOPES)
    # if credentials.create_scoped_required():
    #         credentials = credentials.create_scoped(SCOPES)
    return credentials


def create_bigquery_client(creds):
    """Build the bigquery client."""
    from google.cloud import bigquery
    cli = bigquery.Client(credentials=creds)
    return cli

def create_pub_client(creds):
    """ Build a publishing client for pubsub"""
    from google.cloud import pubsub
    return pubsub.PublisherClient(credentials=creds, batch_settings=types.BatchSettings(max_messages=50))

def create_sub_client(creds):
    """ Build a publishing client for pubsub"""
    from google.cloud import pubsub
    return pubsub.SubscriberClient(credentials=creds)

# def create_pubsub_client(credentials):
#     """Build the pubsub client."""
#     http = httplib2.Http()
#     credentials.authorize(http)
#     from google.cloud import pubsub
#
#     return discovery.build('pubsub', 'v1beta2', http=http)


def flatten(lst):
    """Helper function used to massage the raw tweet data."""
    for el in lst:
        if (isinstance(el, collections.Iterable) and
                not isinstance(el, str)):
            for sub in flatten(el):
                yield sub
        else:
            yield el

def tweet_clean(data: dict):
    """clean and prepare data"""
    res = {}
    for k,v in data.items():
        if(k=="id"):
            res[k] = int(v)
        elif k=="text" :
            res[k]=str(v)
        elif k=="user":
            res["user_id"]=v["id"]
        elif k=="created_at":
            res[k]=str(dateutil.parser.parse(v))
        elif k=="in_reply_to_status_id":
            if v!=None:
                res[k]=int(v)
        elif k=="in_reply_to_user_id":
            if v != None:
                res[k]=int(v)
        elif k=="has_coordinates":
            res[k]=v
        elif k=="coordinates":
            if(v!=None and "coordinates" in v and len(v["coordinates"])==2):
                res["longitude"]=v["coordinates"][0]
                res["latitude"]=v["coordinates"][1]
        elif k=="place":
            if v!=None and "id" in v:
                res["place_id"]=v["id"]
        elif k=="quoted_status_id":
            if v != None:
                res[k]=int(v)
        elif k=="is_quoted_status":
            res[k]=v
        elif k=="quote_count":
            res[k]=int(v)
        elif k=="reply_count":
            res[k]=int(v)
        elif k=="retweet_count":
            res[k]=int(v)
        elif k=="favorite_count":
            res[k]=int(v)
        elif k=="entities":
            if "hashtags" in v and v["hashtags"]!=[]:
                res["hashtags"]=list(ht["text"] for ht in v["hashtags"] if "text" in ht)
            if "urls" in v and v["urls"]!=[]:
                res["expanded_urls"] = list(u["expanded_url"] for u in v["urls"] if "expanded_url" in u)
            if "user_mentions" in v and v["user_mentions"]!=[]:
                res["user_mentions"] = list(u["id"] for u in v["user_mentions"] if "id" in u)
        elif k=="filter_level":
            res[k]=v
        elif k=="user_id":
            res[k]=int(v)
    return res

def cleanup(data):
    """Do some data massaging."""
    if isinstance(data, dict):
        newdict = {}
        for k, v in data.items():
            if (k == 'coordinates') and isinstance(v, list):
                # flatten list
                newdict[k] = list(flatten(v))
            elif k == 'created_at' and v:
                newdict[k] = str(dateutil.parser.parse(v))
            # temporarily, ignore some fields not supported by the
            # current BQ schema.
            # TODO: update BigQuery schema
            elif (k == 'video_info' or k == 'scopes' or k == 'withheld_in_countries'
                  or k == 'is_quote_status' or 'source_user_id' in k
                  or k == ''
                  or 'quoted_status' in k or 'display_text_range' in k or 'extended_tweet' in k
                  or 'media' in k):
                pass
            elif v is False:
                newdict[k] = v
            else:
                if k and v:
                    newdict[k] = cleanup(v)
        return newdict
    elif isinstance(data, list):
        newlist = []
        for item in data:
            newdata = cleanup(item)
            if newdata:
                newlist.append(newdata)
        return newlist
    else:
        return data


def bq_data_insert(bigquery, project_id, dataset, table, tweets):
    """Insert a list of tweets into the given BigQuery table."""
    try:
        rowlist = []
        # Generate the data that will be sent to BigQuery
        for item in tweets:
            item_row = {"json": item}
            rowlist.append(item_row)
        body = {"rows": rowlist}
        # Try the insertion.
        response = bigquery.tabledata().insertAll(
                projectId=project_id, datasetId=dataset,
                tableId=table, body=body).execute(num_retries=NUM_RETRIES)
        # print "streaming response: %s %s" % (datetime.datetime.now(), response)
        return response
        # TODO: 'invalid field' errors can be detected here.
    except Exception as e1:
        print("Giving up: %s" % e1)
