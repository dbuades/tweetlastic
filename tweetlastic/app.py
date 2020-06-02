import logging
import tweepy
import yaml

from tweetlastic.utils.twitter import CustomStream, start_stream, set_twitter_auth
from tweetlastic.utils.aux import set_logging_level
from tweetlastic.utils.rabbitmq_producer import connect_rabbitmq

### Load .yaml file with general settings
with open("tweetlastic/config/settings.yaml", "r") as file:
  settings = yaml.safe_load(file)

### Load .yaml file with terms to follow in the twitter stream
with open(settings["terms_file_path"], "r") as file:
  terms_to_follow = yaml.safe_load(file)

### Start logging
logging_level = set_logging_level(settings["logging_level"])
logging.basicConfig(level = logging_level, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logging.info('Executing script...')

### Define RabbitMQ conection
channel = connect_rabbitmq(settings["rabbitmq_queue_name"])

### Initiate the stream
auth = set_twitter_auth()
myStreamListener = CustomStream(channel, settings["rabbitmq_queue_name"], settings["logging_level"], api=None)
myStream = tweepy.Stream(auth = auth, listener = myStreamListener)

### Execute the stream
start_stream(myStream,
            max_reconnects=int(settings["reconnect_stream"]["max_reconnects"]),
            hours_to_reset_counter=int(settings["reconnect_stream"]["hours_to_reset_counter"]),
            track=terms_to_follow,
            is_async=False,
            stall_warnings=True)
