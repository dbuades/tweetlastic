FROM python:3.7.7-slim

COPY config /tweetlastic/config
COPY src /tweetlastic/src
COPY requirements.txt /tweetlastic

WORKDIR /tweetlastic

RUN pip install -r ./requirements.txt

RUN [ "python", "./src/start.py" ]