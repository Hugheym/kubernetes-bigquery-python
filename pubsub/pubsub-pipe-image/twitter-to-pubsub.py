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

"""This script uses the Twitter Streaming API, via the tweepy library,
to pull in tweets and publish them to a PubSub topic.
"""

import base64
import datetime
import os
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

import utils

# Get your twitter credentials from the environment variables.
# These are set in the 'twitter-stream.json' manifest file.
consumer_key = os.environ['CONSUMERKEY']
consumer_secret = os.environ['CONSUMERSECRET']
access_token = os.environ['ACCESSTOKEN']
access_token_secret = os.environ['ACCESSTOKENSEC']

PUBSUB_TOPIC = os.environ['PUBSUB_TOPIC']
PROJECT_ID = os.environ['PROJECT_ID']
NUM_RETRIES = 3

import json
def publish(client, pubsub_topic, data_lines):
    """Publish to the given pubsub topic."""
    messages = bytes()

    for line in data_lines:

        if isinstance(line, str):
            line = line.encode("UTF8")
        elif isinstance(line, dict):
            line = json.dumps(line).encode("UTF8")
        #line = base64.urlsafe_b64encode(line)
        # if isinstance(line, str):
        #     line = base64.urlsafe_b64decode(line.)
        pub = base64.urlsafe_b64encode(line)
        messages+=pub
    topic_name = client.topic_path(PROJECT_ID, pubsub_topic)
    future=client.publish(topic_name, messages)
    print(future)
    # resp = client.projects().topics().publish(
    #         topic=pubsub_topic, body=body).execute(num_retries=NUM_RETRIES)
    return future #if future.result, will hang and return msg id

# file_handle = open("twitter_data_post_clean.json", "w")

class StdOutListener(StreamListener):
    """A listener handles tweets that are received from the stream.
    This listener dumps the tweets into a PubSub topic
    """

    count = 0
    twstring = ''
    tweets = []
    batch_size = 100
    total_tweets = 10000

    client = utils.create_pub_client(utils.get_credentials())

    def write_to_pubsub(self, tw):
        future=publish(self.client, PUBSUB_TOPIC, tw)
        print(future.result())

    def on_data(self, data):
        """What to do when tweet data is received."""
        import json
        #Doing some preprocessinig and filtering here
        data = json.loads(data)
        if "text" not in data: #if this isn't a status post then do nothing with data.
            return True
        else:
            data=utils.tweet_clean(data)
        print(data)
        self.tweets.append(data)
        if len(self.tweets) >= self.batch_size:
            #json.dump(self.tweets, file_handle)
            #res = list(json.loads(s) for s in self.tweets)
            # json.dump(self.tweets, file_handle)
            # file_handle.flush()
            # file_handle.close()
            # quit()
            self.write_to_pubsub(self.tweets)
            self.tweets = []
        self.count += 1
        # if we've grabbed more than total_tweets tweets, exit the script.
        # If this script is being run in the context of a kubernetes
        # replicationController, the pod will be restarted fresh when
        # that happens.
        if self.count > self.total_tweets:
            return False
        if (self.count % 1000) == 0:
            print('count is: %s at %s' % (self.count, datetime.datetime.now()))
        return True

    def on_error(self, status):
        print( status)


if __name__ == '__main__':
    print('....')
    listener = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    print('stream mode is: %s' % os.environ['TWSTREAMMODE'])

    stream = Stream(auth, listener)
    # set up the streaming depending upon whether our mode is 'sample', which
    # will sample the twitter public stream. If not 'sample', instead track
    # the given set of keywords.
    # This environment var is set in the 'twitter-stream.yaml' file.
    if False:#os.environ['TWSTREAMMODE'] == 'sample':
        stream.sample()
    else:
        stream.filter(
                track=['music', 'song', 'gig', 'concert',
                       'hiphop', 'song', 'listening',
                       'band', 'tunes',"rap","indie"],
                languages=["en"]
                )
