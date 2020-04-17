## Credentials management
import os
import tweepy

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