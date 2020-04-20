import elasticsearch
import logging
import tweepy
import yaml

from tweetlastic.utils.elastic import IndexOperations, set_elastic_path
from tweetlastic.utils.twitter import CustomStream, start_stream, set_twitter_auth
from tweetlastic.utils.aux import set_logging_level

### Load .yaml file with general settings
with open("tweetlastic/config/settings.yaml", "r") as file:
  settings = yaml.safe_load(file)

### Load .yaml file with terms to follow in the twitter stream
with open(settings["terms_file_path"], "r") as file:
  terms_to_follow = yaml.safe_load(file)

### Start logging
logging_level = set_logging_level(settings["logging_level"])
logging.basicConfig(level = logging_level, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
# If logging_level != DEBUG, reduce elastic logging level to Warning (otherwise, in INFO, it logs every time a tweet is saved)
if settings["logging_level"] != "DEBUG":
  es_logger = logging.getLogger('elasticsearch')
  es_logger.setLevel(logging.WARNING)
logging.info('Executing script...')

### Define ElasticSearch connection
elastic_path = set_elastic_path()
es = elasticsearch.Elasticsearch(elastic_path)
# Create ElasticSearch index if it doesn't exist (or force overwrite)
IndexOperations().create_index(es, index_name = settings["elastic_index_name"], overwrite = settings["overwrite_index"])

### Initiate the stream
auth = set_twitter_auth()
myStreamListener = CustomStream(es, settings["elastic_index_name"], settings["logging_level"], api=None)
myStream = tweepy.Stream(auth = auth, listener = myStreamListener)

### Execute the stream
start_stream(myStream,
            max_reconnects=int(settings["reconnect_stream"]["max_reconnects"]),
            hours_to_reset_counter=int(settings["reconnect_stream"]["hours_to_reset_counter"]),
            track=terms_to_follow,
            is_async=False,
            stall_warnings=True)
