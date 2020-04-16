import elasticsearch
import logging
import tweepy
import json
from aux_functions import CustomStream, start_stream, create_index, define_twitter_credentials, define_elastic_path, set_logging_level, load_json, load_terms_to_follow

### Load .json file with settings
settings = load_json('settings.json')

### Start logging
logging_level = set_logging_level(settings["logging_level"])
logging.basicConfig(level = logging_level, filename=settings["log_name"], format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
logging.info('Executing script...')

### Define ElasticSearch connection
elastic_path = define_elastic_path(**settings["elastic_credentials"])
# elastic_path = settings["elastic_path"]
es = elasticsearch.Elasticsearch(elastic_path)
# Reduce elastic logging level to Warning (otherwise, in INFO, it logs every time a tweet is saved)
es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.WARNING)

### Create ElasticSearch index if it doesn't exist
if not es.indices.exists(index=settings["elastic_index_name"]):
    create_index(es, settings["elastic_index_name"])

### Initiate the stream
auth = define_twitter_credentials(**settings["twitter_credentials"])
myStreamListener = CustomStream(es, settings["elastic_index_name"], settings["logging_level"], api=None)
myStream = tweepy.Stream(auth = auth, listener=myStreamListener)

### Execute the stream
terms_to_follow = load_terms_to_follow(settings["terms_file_path"])
start_stream(myStream,
            max_reconnects=int(settings["reconnect_stream"]["max_reconnects"]),
            hours_to_reset_counter=int(settings["reconnect_stream"]["hours_to_reset_counter"]),
            track=terms_to_follow,
            is_async=False,
            stall_warnings=True)
