# Standard
import sys
import logging
import re
import datetime
import time
# Extra
import elasticsearch
import tweepy
import numpy as np


class CustomStream(tweepy.StreamListener):

    '''
    Custom Class for saving tweets with error handling 
    
    Should I initialize the Class before??
    '''

    def __init__(self, es, index_name, logging_level, **kwargs):

        super().__init__(**kwargs)
        self.es = es
        self.index_name = index_name

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
            json_data = status._json

            # Save object to our elasticsearch DB
            tweet = self.elastic_parse(json_data)
            self.elastic_save(tweet)

            # Debug
            if self.debug:
                self.debug_json_list.append(json_data)
                self.debug_save_list.append(tweet)
                logging.debug(tweet['url'])
            
    def date_parser(self, date):
        return datetime.datetime.strptime(date, '%a %b %d %H:%M:%S %z %Y')
    
    def location_parser(self, location):
        if len(location) == 1: # This is a shape, we want the middle point
            return np.mean(location, axis=1).reshape(-1).tolist()
        else: # This is a point
            return location
            
    def elastic_parse(self, tweet):
        
        # Default values
        monetizable = False
        place_dict = None
        
        # Check if it is a long tweet or not
        if 'extended_tweet' in tweet: 
            text = tweet['extended_tweet']['full_text']
            entities = tweet['extended_tweet']['entities']
            # Check for monetizable media
            if 'extended_entities' in tweet['extended_tweet']:
                if 'additional_media_info' in tweet['extended_tweet']['extended_entities']['media']:
                    monetizable = any([media['additional_media_info']['monetizable'] for media in tweet['extended_tweet']['extended_entities']['media']]) # If any media in the tweet is monetizable
        else : 
            text = tweet['text']
            entities = tweet['entities']
            
        # Check if location exists
        if 'place' in tweet and tweet['place'] is not None:
            place = tweet['place']
            place_dict = {
                'id_str' : place['id'],
                'url' : place['url'],
                'place_type' : place['place_type'],
                'name' : place['full_name'],
                'country' : place['country'],
                'country_code' : place['country_code'],
                'coordinates' : self.location_parser(place['bounding_box']['coordinates'])
            }

        # Create long objects (For code clarity)
        mentions = [{'name' : element['name'], 'url': 'https://twitter.com/' + element['screen_name'], 'id_str' : element['id_str']} for element in entities['user_mentions']]
        hastags = [hashtag['text'] for hashtag in entities['hashtags']] # Keep only the text of the hashtag

        # User parser
        user = tweet['user']
        user_dict = {
            'name' : user['name'],
            'url' : 'https://twitter.com/' + user['screen_name'],
            'id_str' : user['id_str'],
            'created_at' : self.date_parser(user['created_at']),
            'description' : user['description'],
            'protected' : user['protected'],
            'verified' : user['verified'],
            'lang' : user['lang'],
            'listed_count' : user['listed_count'],
            'location' : user['location'],
            'geo_enabled' : user['geo_enabled'],

            'stats' : {
                'statuses_count' : user['statuses_count'],
                'favourites_count' : user['favourites_count'],
                'followers_count' : user['followers_count'],
                'friends_count' : user['friends_count'],
            },

            'profile' : {
                'default_profile' : user['default_profile'],
                'default_profile_image' : user['default_profile_image'],
                'profile_background_image_url': user['profile_background_image_url'],
                'profile_image_url': user['profile_image_url'],
                'profile_background_color': user['profile_background_color'],
                'profile_text_color': user['profile_text_color'],
            }
        }

        # Create the new .json
        new = {
            'url' : 'https://twitter.com/statuses/' + tweet['id_str'],
            'id_str' : tweet['id_str'],
            'date': self.date_parser(tweet['created_at']),
            'text' : text,
            'hastags': hastags, 
            'monetizable' : monetizable,
            'source' : re.search(">(.*?)<", tweet['source']).group()[1:-1],
            'lang' : tweet['lang'],
            'mentions': mentions,
            'place': place_dict,

            'reply' : {
                'id_str' : tweet['in_reply_to_status_id_str'],                
                'url' : None if tweet['in_reply_to_status_id_str'] is None else 'https://twitter.com/statuses/' + tweet['in_reply_to_status_id_str'],
                'user_id_str' : tweet['in_reply_to_user_id_str'],
                'user_url' : None if tweet['in_reply_to_user_id_str'] is None else 'https://twitter.com/' + tweet['in_reply_to_screen_name'],
            },

            'stats' : {
                'favorite_count' : tweet['favorite_count'],
                'quote_count' : tweet['quote_count'],
                'reply_count' : tweet['reply_count'],
                'retweet_count' : tweet['retweet_count'],
            },

            'user' : user_dict,
        }
        
        return new

    def elastic_save(self, tweet):

        # Save the tweet to ElasticSearch
        self.es.index(
            index=self.index_name,
            body=tweet,
            ignore=400,
        )
            

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


def define_index_template():

    '''
    Define ElasticSearch template for twitter data
    '''

    template = {
        "mappings": {
            "properties": {
                "date": { 
                    "type": "date"
                },
                
                "hastags": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                
                "id_str": {
                    "type": "keyword",
                    "ignore_above": 256
                },
                
                "lang": {
                    "type": "keyword",
                    "ignore_above": 256
                },
                
                "place": {
                    "properties": {
                        "id_str" : {
                            "type": "keyword",
                            "ignore_above": 256
                        },
                        "url" : {
                            "type": "keyword"
                        },
                        "place_type" : {
                            "type": "keyword",
                            "ignore_above": 256
                        },
                        "name" : {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "country" : {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "country_code" : {
                            "type": "keyword",
                            "ignore_above": 256
                        },
                        "coordinates" : {
                            "type": "geo_point"
                        }
                    }
                },
                
                "mentions": {
                    "properties": {
                        "id_str": {
                            "type": "keyword",
                            "ignore_above": 256
                        },
                        "name": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "url": {
                            "type": "keyword"
                        },
                    }
                },
                
                "monetizable": {
                    "type": "boolean"
                },
            
                "reply": {
                    "properties": {
                        "id_str": {
                            "type": "keyword",
                            "ignore_above": 256
                        },
                        "url": {
                            "type": "keyword"
                        },
                        "user_id_str": {
                            "type": "keyword",
                            "ignore_above": 256
                        },
                        "user_url": {
                            "type": "keyword"
                        },
                    }
                },
                
                "source": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword"
                        }
                    }
                },
            
                "stats": {
                    "properties": {
                        "favorite_count": {
                            "type": "integer"
                        },
                        "quote_count": {
                            "type": "integer"
                        },
                        "reply_count": {
                            "type": "integer"
                        },
                        "retweet_count": {
                            "type": "integer"
                        }
                    }
                },
            
                # This is the main propierty (the tweet content)
                "text": {
                    "type": "text",
                    "fields": {
                        "spanish": { 
                            "type": "text",
                            "analyzer": "spanish"
                        },
                        "catalan": { 
                            "type": "text",
                            "analyzer": "catalan"
                        }
                    }
                },
                
                "url": {
                    "type": "keyword"
                },
                
                "user": {
                    "properties": {
                        "created_at": {
                            "type": "date"
                        },
                        
                        "description": {
                            "type": "text"
                        },
                        "geo_enabled": {
                            "type": "boolean"
                        },
                        "id_str": {
                            "type": "keyword",
                            "ignore_above": 256
                        },
                        "lang": {
                            "type": "keyword",
                            "ignore_above": 256
                        },
                        "listed_count": {
                            "type": "integer"
                        },
                        "location": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "name": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "profile": {
                            "properties": {
                                "default_profile": {
                                    "type": "boolean"
                                },
                                "default_profile_image": {
                                    "type": "boolean"
                                },
                                "profile_background_color": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                },
                                "profile_background_image_url": {
                                    "type": "keyword"
                                },
                                "profile_image_url": {
                                    "type": "keyword"
                                },
                                "profile_text_color": {
                                    "type": "keyword",
                                    "ignore_above": 256
                                }
                            }
                        },
                        "protected": {
                            "type": "boolean"
                        },
                        "stats": {
                            "properties": {
                                "favourites_count": {
                                    "type": "integer"
                                },
                                "followers_count": {
                                    "type": "integer"
                                },
                                "friends_count": {
                                    "type": "integer"
                                },
                                "statuses_count": {
                                    "type": "integer"
                                }
                            }
                        },
                        "url": {
                            "type": "keyword"
                        },
                        "verified": {
                            "type": "boolean"
                        }
                    }
                }
            }
        }
    }

    return template


def define_twitter_credentials(consumer_key, consumer_secret, access_token, access_token_secret):
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    return auth


def create_index(es, index_name):
    '''
    Create ElasticSearch index for saving the tweets
    '''
    index_template = define_index_template()
    es.indices.create(index=index_name, body=index_template)


def set_logging_level(logging_level):
    '''
    We load the string from a Json, we parse it as a variable.
    sys.tracebacklimit = 0 is for not polluting the log file with tracebacks, except when debugging.
    '''

    if logging_level == "DEBUG":
        logging_level = logging.DEBUG

    elif logging_level == "INFO":
        logging_level = logging.INFO
        sys.tracebacklimit = 0
    
    elif logging_level == "WARNING":
        logging_level = logging.WARNING
        sys.tracebacklimit = 0

    elif logging_level == "ERROR":
        logging_level = logging.ERROR
        sys.tracebacklimit = 0

    return logging_level


def define_elastic_path(elastic_user, elastic_pass, elastic_protocol, elastic_port, elastic_adress):
    '''
    Set the path for connecting to the ElasticSearch database
    '''
    elastic_path = elastic_protocol + '://' + elastic_user + ':' + elastic_pass + '@' + elastic_adress + ':' + elastic_port
    return elastic_path