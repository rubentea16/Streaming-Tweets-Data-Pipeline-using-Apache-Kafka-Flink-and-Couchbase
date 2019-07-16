from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
from kafka.errors import KafkaError
import tweepy
import json
import twitter_credentials

consumer_key = twitter_credentials.ckey
consumer_secret = twitter_credentials.csecret
access_token = twitter_credentials.atoken 
access_token_secret = twitter_credentials.asecret

# Setup access API
def connect_to_twitter_OAuth():
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    return api
# Create API object
api = connect_to_twitter_OAuth() 

# Prepare for send message to KAFKA
bootstrap_servers = ['localhost:9092']
topicName = 'twitter'

producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))

class StdOutListener(StreamListener):
    """ A listener handles tweets are the received from the stream.
    This is a basic listener that just prints received tweets to stdout.
    """
    def on_data(self, data):
        json_data = json.loads(data)
        output = json_data
        ack = producer.send(topicName,output)
        metadata = ack.get()
        print(ack)
        return True

    def on_error(self, status):
        print(status)

if __name__ == '__main__':
    l = StdOutListener()

    stream = Stream(auth = api.auth, listener=l)
    stream.filter(track=['basketball'])