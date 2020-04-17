import elasticsearch
from src.authentication import set_elastic_path, set_twitter_auth

def test_load_credentials_from_env():
  """
  Test that the credentials have been correctly exported to env variables
  and can be loaded correctly.
  """  
  auth = set_twitter_auth()
  elastic_path = set_elastic_path()

  assert auth and elastic_path, "Credentials not loaded correctly"

def test_elastic_credentials():
  """
  Test that we can communicate with the Elastic DB
  """
  elastic_path = set_elastic_path()
  es = elasticsearch.Elasticsearch(elastic_path)
  ping = es.ping()

  assert ping, "Elastic DB couldn't be reached"

def test_twitter_credentials():
  """
  Test that we can communicate with the Twitter API
  """
  auth = set_twitter_auth()
  url = auth.get_authorization_url()

  assert url, "Twitter API couldn't be reached"
