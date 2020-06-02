import os
import json
import pika

def connect_rabbitmq(queue_name):
  HOST_NAME = os.getenv('RABBITMQ_HOST')

  # Try to connect to the server and open connection
  try:
    connection = pika.BlockingConnection(pika.ConnectionParameters(HOST_NAME))
    channel = connection.channel()
    channel.queue_declare(queue=queue_name)

  except Exception as err:
    ## TODO: Handle exception
    raise err

  return channel


def publish_rabbitmq(channel, queue_name, tweet):
    channel.basic_publish(exchange='',
                          routing_key=queue_name,
                          body=json.dumps(tweet))