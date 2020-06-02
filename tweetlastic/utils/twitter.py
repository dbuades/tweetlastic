# Standard
import sys
import os
import logging  
import datetime
import time
# Extra
import tweepy

# Custom
from tweetlastic.utils.rabbitmq_producer import publish_rabbitmq

class CustomStream(tweepy.StreamListener):

    '''
    Custom Class for saving tweets with error handling 
    
    Should I initialize the Class before??
    '''

    def __init__(self, channel, queue_name, logging_level, **kwargs):

        super().__init__(**kwargs)
        self.channel = channel
        self.queue_name = queue_name

        # Debug parameters
        if logging_level == "DEBUG":
            self.debug = True
            self.debug_json_list = []
            self.debug_save_list = []
        else:
            self.debug = False

        # Log the start of the script
        logging.info('Starting tweet collection')

    ############ Error handling ##########################

    ### Error class
    class ForceReconnect(Exception):
        # This will cause the _read_loop to finish and perform the cleanup as per https://github.com/tweepy/tweepy/blob/cc3c8e7cae73d28b9a75edb7342f89f04da99702/tweepy/streaming.py#L286
        # We can catch this exception later and reconnect the stream.
        pass        

    ## Error functions
    def on_timeout(self):
        logging.warning('Timeout, waiting')
        return
    
    def on_warning(self, notice):
        logging.warning('Warning: ' + str(notice['code']))
        return
    
    def on_error(self, error):
        # Don't stop the stream automatically, let tweepy handle the reconnection based on the recommended backoff strategy.
        logging.error(error)
        return
    
    def on_limit(self, track):        
        # Stop and reconnect the stream if we missed more than 3000 tweets to start fresh.
        if track > 5000:
            logging.error('Restarting stream, too many tweets missed since last established connection.')
            raise ForceReconnect
        else:
            logging.warning('Rate limit kicked in: ' + str(track) + ' tweets missed since last established connection')
            return
        
    def on_disconnect(self, notice):
        logging.error('Disconected from stream with code ' + str(notice['code']) + '. Reason: ' + notice['reason'])
        # Stop and reconnect the stream
        raise ForceReconnect
    
    #################################### Processing #########################
    
    def on_status(self, status):
        if not status.retweeted and not status.text.startswith('RT @') and not status.favorited: # Ignore RT and favorites, we just want original tweets

            # Parse tweet into .json object
            tweet_json = status._json

            # Send tweet to the RabbitMQ broker
            publish_rabbitmq(channel=self.channel,
                            queue_name=self.queue_name,
                            tweet=tweet_json)

            # Debug
            if self.debug:
                self.debug_json_list.append(tweet_json)
                logging.debug(tweet_json)


def start_stream(stream,
                max_reconnects,
                hours_to_reset_counter,
                reconnects=0,
                **kwargs):

    '''
    Resillient way to start saving tweets and restarting the service on exceptions.
    '''

    try:
        stream.filter(**kwargs)

    except CustomStream.ForceReconnect:
        logging.warning('Forcing reconnection')
        time.sleep(2)
        start_stream(stream, max_reconnects, hours_to_reset_counter, reconnects, **kwargs)

    except:
        # Catch the rest of exceptions.
        reconnects += 1

        # Check wether to reset number of reconnections based on elapsed time.
        if reconnects == 1:
            first_reconnection_time = datetime.datetime.now()
        else:
            elapsed_time_since_first_reconnection = datetime.datetime.now() - first_reconnection_time
            if elapsed_time_since_first_reconnection >= datetime.timedelta(0,int(hours_to_reset_counter*3600)):
                reconnects = 1
                logging.warning('Number of reconnections resetted')

        # Check the maximum number of reconnects in order not to fall into an infinite loop.       
        if reconnects < max_reconnects:
            # Wait a bit and then force the reconnection of the stream
            logging.exception('Reconnection number ' + str(reconnects) + '. Maximum of' + str(max_reconnects) + str(' reconnection attempts allowed.'))
            time.sleep(int(10 *(reconnects**1.25)))
            start_stream(stream, max_reconnects, hours_to_reset_counter, reconnects, **kwargs)

        else:
            # Wait for 60 minutes before exiting and then Docker will restart the container on its own
            time.sleep(3600)
            logging.exception('Maximum number of reconnection attempts ( ' + str(max_reconnects) + ' ) reached.')

def set_twitter_auth():
    '''
    Set the credentials for connecting to the Twitter API
    '''

    TWITTER_CONSUMER_KEY = os.getenv('TWITTER_CONSUMER_KEY')
    TWITTER_CONSUMER_SECRET = os.getenv('TWITTER_CONSUMER_SECRET')

    TWITTER_ACCESS_TOKEN = os.getenv('TWITTER_ACCESS_TOKEN')
    TWITTER_ACCESS_TOKEN_SECRET = os.getenv('TWITTER_ACCESS_TOKEN_SECRET')

    auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET)
    auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)

    return auth