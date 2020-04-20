# Standard
import os
import re
import datetime
# Extra
import elasticsearch
import numpy as np


class CustomParser():

    def __init__(self):
        pass
    
    @staticmethod
    def date(date):
        return datetime.datetime.strptime(date, '%a %b %d %H:%M:%S %z %Y')
    
    @staticmethod
    def location(location):
        if len(location) == 1:
            # This is a shape, we want the middle point
            return np.mean(location, axis=1).reshape(-1).tolist()
        else:
            # This is a point
            return location

            
def elastic_parse(tweet):
    
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
            'coordinates' : CustomParser.location(place['bounding_box']['coordinates'])
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
        'created_at' : CustomParser.date(user['created_at']),
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
        'date': CustomParser.date(tweet['created_at']),
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

def elastic_save(es, index_name, tweet):

    # Save the tweet to ElasticSearch
    es.index(
        index=index_name,
        body=tweet,
        ignore=400,
    )


class IndexOperations():

  def __init__(self):

      self.index_template = self.define_index_template()

  def create_index(self, es, index_name, overwrite = False):
      '''
      Create ElasticSearch index for saving the tweets (if it does not already exist)
      '''
      if not es.indices.exists(index=index_name):
          es.indices.create(index=index_name, body=self.index_template)
      
      else:
        if overwrite:
          es.indices.delete(index=index_name)
          es.indices.create(index=index_name, body=self.index_template)


  def define_index_template(self):
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


def set_elastic_path():
    '''
    Set the path for connecting to the ElasticSearch DB
    '''

    ELASTIC_PROTOCOL = os.getenv('ELASTIC_PROTOCOL')
    ELASTIC_ADDRESS = os.getenv('ELASTIC_ADDRESS')
    ELASTIC_PORT = os.getenv('ELASTIC_PORT')

    ELASTIC_USER = os.getenv('ELASTIC_USER')
    ELASTIC_PASS = os.getenv('ELASTIC_PASS')

    return ELASTIC_PROTOCOL + '://' + ELASTIC_USER + ':' + ELASTIC_PASS + '@' + ELASTIC_ADDRESS + ':' + ELASTIC_PORT