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
"""This script grabs tweets from a PubSub topic, and stores them in BiqQuery
using the BigQuery Streaming API.
"""

import base64
import datetime
import json
import os
import time
from typing import Any

import utils
import pandas
import nltk
from google.cloud import pubsub
from google.cloud import pubsub_v1
# Get the project ID and pubsub topic from the environment variables set in
# the 'bigquery-controller.yaml' manifest.
PROJECT_ID = os.environ['PROJECT_ID']
PUBSUB_TOPIC = os.environ['PUBSUB_TOPIC']
NUM_RETRIES = 3


def fqrn(resource_type, project, resource):
    """Returns a fully qualified resource name for Cloud Pub/Sub."""
    return "projects/{}/{}/{}".format(project, resource_type, resource)


def create_subscription(client: pubsub_v1.SubscriberClient, project_name: str, sub_name: str):
    """Creates a new subscription to a given topic."""
    print("using pubsub topic: %s \n" % PUBSUB_TOPIC)
    name = client.subscription_path(project_name, sub_name)
    topic = client.topic_path(project_name, "new_tweets")
    print(name)
    print(topic)
    try:
        subscription: sub_client.types.module.Subscription = client.create_subscription(name, topic)
    except:
        subscription = "subscription already exists"
    #if subscription already exists, should return ALREADY_EXISTS
    print( 'Subscription {} was created.'.format(subscription))


def get_full_subscription_name(project, subscription):
    """Returns a fully qualified subscription name."""
    return fqrn('subscriptions', project, subscription)

def call_back(msg: pubsub_v1.subscriber.message.Message, res):
    msgData = base64.b64decode(msg.data)
    """b'{"created_at":"Tue Mar 06 02:33:10 +0000 2018","id":970849741985677312,"id_str":"970849741985677312","text":"I\\u2019m 164 pounds rn and my goal is to hit 150 by the end of the month \\ud83d\\udd25 big booty and lean waist season let\\u2019s get it https:\\/\\/t.co\\/UsYAez2tYl","display_text_range":[0,114],"source":"\\u003ca href=\\"http:\\/\\/twitter.com\\/download\\/iphone\\" rel=\\"nofollow\\"\\u003eTwitter for iPhone\\u003c\\/a\\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":4264593013,"id_str":"4264593013","name":"Deanogambino23","screen_name":"deanogambino","location":"San Marcos, TX","url":null,"description":"\\u201c I\\u2019m not superstitious but I am a little stitious\\u201d","translator_type":"none","protected":false,"verified":false,"followers_count":117,"friends_count":111,"listed_count":0,"favourites_count":378,"statuses_count":608,"created_at":"Tue Nov 17 05:45:38 +0000 2015","utc_offset":null,"time_zone":null,"geo_enabled":false,"lang":"en","contributors_enabled":false,"is_translator":false,"profile_background_color":"000000","profile_background_image_url":"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png","profile_background_image_url_https":"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png","profile_background_tile":false,"profile_link_color":"DD2E44","profile_sidebar_border_color":"000000","profile_sidebar_fill_color":"000000","profile_text_color":"000000","profile_use_background_image":false,"profile_image_url":"http:\\/\\/pbs.twimg.com\\/profile_images\\/965706225546678272\\/zCgHWy51_normal.jpg","profile_image_url_https":"https:\\/\\/pbs.twimg.com\\/profile_images\\/965706225546678272\\/zCgHWy51_normal.jpg","profile_banner_url":"https:\\/\\/pbs.twimg.com\\/profile_banners\\/4264593013\\/1518900029","default_profile":false,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":null,"contributors":null,"is_quote_status":false,"quote_count":0,"reply_count":0,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[],"urls":[],"user_mentions":[],"symbols":[],"media":[{"id":970849725862719489,"id_str":"970849725862719489","indices":[115,138],"media_url":"http:\\/\\/pbs.twimg.com\\/tweet_video_thumb\\/DXkmrpBVMAEYWoi.jpg","media_url_https":"https:\\/\\/pbs.twimg.com\\/tweet_video_thumb\\/DXkmrpBVMAEYWoi.jpg","url":"https:\\/\\/t.co\\/UsYAez2tYl","display_url":"pic.twitter.com\\/UsYAez2tYl","expanded_url":"https:\\/\\/twitter.com\\/deanogambino\\/status\\/970849741985677312\\/photo\\/1","type":"photo","sizes":{"thumb":{"w":150,"h":150,"resize":"crop"},"large":{"w":480,"h":360,"resize":"fit"},"medium":{"w":480,"h":360,"resize":"fit"},"small":{"w":480,"h":360,"resize":"fit"}}}]},"extended_entities":{"media":[{"id":970849725862719489,"id_str":"970849725862719489","indices":[115,138],"media_url":"http:\\/\\/pbs.twimg.com\\/tweet_video_thumb\\/DXkmrpBVMAEYWoi.jpg","media_url_https":"https:\\/\\/pbs.twimg.com\\/tweet_video_thumb\\/DXkmrpBVMAEYWoi.jpg","url":"https:\\/\\/t.co\\/UsYAez2tYl","display_url":"pic.twitter.com\\/UsYAez2tYl","expanded_url":"https:\\/\\/twitter.com\\/deanogambino\\/status\\/970849741985677312\\/photo\\/1","type":"animated_gif","video_info":{"aspect_ratio":[4,3],"variants":[{"bitrate":0,"content_type":"video\\/mp4","url":"https:\\/\\/video.twimg.com\\/tweet_video\\/DXkmrpBVMAEYWoi.mp4"}]},"sizes":{"thumb":{"w":150,"h":150,"resize":"crop"},"large":{"w":480,"h":360,"resize":"fit"},"medium":{"w":480,"h":360,"resize":"fit"},"small":{"w":480,"h":360,"resize":"fit"}}}]},"favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"en","timestamp_ms":"1520303590662"}\r\n'
    """
    decoded = json.loads(msgData)
    res.append(decoded)
    print(res)
    return res


class MsgCollector:
    def __init__(self, sub_client: pubsub_v1.subscriber.client.Client, project_name, sub_name, bigquery_client):
        self.subscription_path = sub_client.subscription_path(project_name, sub_name)
        self.tweets = []
        self.tokenized_tweets = []
        self.dataset_id = os.environ["DATASET_ID"]
        self.table_id = os.environ["TABLE_ID"]
        self.project_name=project_name
        self.bigquery_cli=bigquery_client
        self.dataset_ref = self.bigquery_cli.dataset(self.dataset_id)
        self.table_ref = self.dataset_ref.table(self.table_id)
        from google.cloud import bigquery as bq

        self.id_schema_field = bq.SchemaField('id','INTEGER',mode="NULLABLE")
        self.batch_size = 100
        self.sub_client = sub_client
        self.msg_pull()

    def loop(self, max_num_msg):
        while(True):
            time.sleep(3)
            print("#")
    def write_to_bq(self):
        try:
            print("write to bq happening")
            from nltk.tokenize import TweetTokenizer
            tt = TweetTokenizer()
            df = pandas.read_json(json.dumps(self.tweets))
            df["tokenized"]=df["text"].apply(tt.tokenize)
            df=df[["id","tokenized","user_id"]]
            df.to_gbq(self.dataset_id+"."+"tokenized_tweets",
                      self.project_name
                      ,if_exists="append"
                      ,private_key="./gcp_tw_bq_ps_creds.json")
        except Exception as e:
            print("Exception %s"%e)
        try:
            #errors = self.bigquery_cli.insert_rows(self.table_ref, self.tweets, [self.id_schema_field])
            errors = self.bigquery_cli.insert_rows_json(self.table_ref, self.tweets)
        except Exception as e:
            print("Exception in write bq %s" %e)
        print(errors)
        if(len(errors)!=0):
            print("errors:", errors)
        else:
            print("SUCCESS: write to db")
            self.tweets=[]
        # response = utils.bq_data_insert(self.bigquery, self.project_name, os.environ['BQ_DATASET'],
        #                      os.environ['BQ_TABLE'], self.tweets)

    def call_back(self, msg: pubsub_v1.subscriber.message.Message):
        msgData = base64.b64decode(msg.data)
        tweet_messy = json.loads(msgData)
        tweet_clean = utils.tweet_clean(tweet_messy)#utils.cleanup(tweet_messy)

        self.tweets.append(tweet_clean)
        print("current len(self.tweets):", len(self.tweets))
        if(len(self.tweets)>self.batch_size):
            print("Writing to BQ")
            self.write_to_bq()
    def msg_pull(self):
        try:
            print("subscribing to ", self.subscription_path)
            flow_control = pubsub_v1.types.FlowControl(max_messages=700)
            cons = self.sub_client.subscribe(self.subscription_path, flow_control=flow_control)
            future = cons.open(self.call_back)
            return
            # resp = sub_client.projects().subscriptions().pull(
            #         subscription=subscription, body=body).execute(
            #                 num_retries=NUM_RETRIES)
        except Exception as e:
            print("Exception: %s" % e)
            time.sleep(0.5)
            return



def pull_messages(sub_client: pubsub_v1.subscriber.client.Client, project_name, sub_name):
    """Pulls messages from a given subscription."""
    BATCH_SIZE = 50
    tweets = []

    subscription = sub_client.subscription_path(project_name,sub_name)
    #subscription = get_full_subscription_name(project_name, sub_name)

    body = {
            'returnImmediately': False,
            'maxMessages': BATCH_SIZE
    }
    print("subscribing to ", subscription)
    flow_control = pubsub_v1.types.FlowControl(max_messages=10)
    try:
        cons = sub_client.subscribe(subscription,flow_control=flow_control)
        f = lambda x: call_back(x,tweets)
        future = cons.open(f)
        print(len(tweets))
        return tweets
        # resp = sub_client.projects().subscriptions().pull(
        #         subscription=subscription, body=body).execute(
        #                 num_retries=NUM_RETRIES)
    except Exception as e:
        print( "Exception: %s" % e)
        time.sleep(0.5)
        return

    # receivedMessages = resp.get('receivedMessages')
    # if receivedMessages is not None:
    #     ack_ids = []
    #     for receivedMessage in receivedMessages:
    #             message = receivedMessage.get('message')
    #             if message:
    #                     tweets.append(
    #                         base64.urlsafe_b64decode(str(message.get('data'))))
    #                     ack_ids.append(receivedMessage.get('ackId'))
    #     ack_body = {'ackIds': ack_ids}
    #     sub_client.projects().subscriptions().acknowledge(
    #             subscription=subscription, body=ack_body).execute(
    #                     num_retries=NUM_RETRIES)
    #return tweets


def write_to_bq(pubsub, sub_name, bigquery):
    """Write the data to BigQuery in small chunks."""
    tweets = []
    CHUNK = 50  # The size of the BigQuery insertion batch.
    # If no data on the subscription, the time to sleep in seconds
    # before checking again.
    WAIT = 2
    tweet = None
    mtweet = None
    count = 0
    count_max = 50000
    while count < count_max:
        while len(tweets) < CHUNK:
            twmessages = pull_messages(pubsub, PROJECT_ID, sub_name)
            if twmessages:
                for tweet in twmessages:
                    # First do some massaging of the raw data
                    mtweet = utils.cleanup(tweet)
                    # We only want to write tweets to BigQuery; we'll skip
                    # 'delete' and 'limit' information.
                    if 'delete' in mtweet:
                        continue
                    if 'limit' in mtweet:
                        continue
                    tweets.append(mtweet)
            else:
                # pause before checking again
                print( 'sleeping...')
                time.sleep(WAIT)
        response = utils.bq_data_insert(bigquery, PROJECT_ID, os.environ['BQ_DATASET'],
                             os.environ['BQ_TABLE'], tweets)
        tweets = []
        count += 1
        if count % 25 == 0:
            print("processing count: %s of %s at %s: %s" %
                   (count, count_max, datetime.datetime.now(), response))


if __name__ == '__main__':
    topic_info = PUBSUB_TOPIC.split('/')
    topic_name = topic_info[-1]
    sub_name = "tweets-%s" % topic_name
    print( "starting write to BigQuery....")
    credentials = utils.get_credentials()

    bigquery = utils.create_bigquery_client(credentials)
    sub_client = utils.create_sub_client(credentials)
    try:
        # TODO: check if subscription exists first
        create_subscription(sub_client, PROJECT_ID, sub_name)
    except Exception as e:
        print( e)
    msg_worker = MsgCollector(sub_client,PROJECT_ID,sub_name,bigquery)
    msg_worker.loop(30)
    print( 'exited write loop')
