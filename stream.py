from config import *
import sys
import re
import socket
import json
import tweepy


auth = tweepy.OAuthHandler(TW_API_KEY, TW_API_KEY_S)
auth.set_access_token(TW_ACCESS_TOKEN, TW_ACCESS_TOKEN_S)

KEYWORD = '#covid19'
if len(sys.argv) != 2:
    KEYWORD = sys.argv[1] or 'covid19'


def clean_text(text):

    # Remove mentions
    text = re.sub(r'@[A-Za-z0-9]+', '', text)

    # Remove URLs
    text = re.sub('https?://[A-Za-z0-9./]*', '', text)

    # Remove newlines and #
    text = re.sub('[\n\#]', ' ', text)

    # Remove HTML entities such as &amp;
    text = re.sub('&(#?\\w+);', '', text)

    return text


def preprocess_tweet(status):
    tweet = {
        'keyword': KEYWORD
    }

    # Text
    text = None
    if hasattr(status, 'retweeted_status'):
        try:
            text = status.retweeted_status.extended_tweet['full_text']
        except AttributeError:
            text = status.retweeted_status.text
    else:
        try:
            text = status.extended_tweet['full_text']
        except AttributeError:
            text = status.text
    if text is not None:
        tweet['text'] = clean_text(text)

    # Location
    location = None
    if hasattr(status, 'coordinates') and status.coordinates is not None:
        print('coordinates: ' + status.coordinates)
        location = status.coordinates
    elif hasattr(status, 'place') and hasattr(status.place, 'coordinates'):
        location = [sum(x) / len(x) for x in zip(*status.place.coordinates)]
        print('place: ' + str(location))
    elif hasattr(status, 'user') and status.user.location is not None:
        location = clean_text(status.user.location)
    if location is not None:
        tweet['location'] = location

    return tweet


print('Starting socket')
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((STREAM_IP, STREAM_PORT))
s.listen(1)
conn, addr = s.accept()
print('Received socket connection')


class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        json_tweet = (json.dumps(preprocess_tweet(status)) + '\n').encode('utf-8')
        conn.send(json_tweet)
        return True

    def on_error(self, status_code):
        print(f'Tweepy StreamListener error {status_code}')
        return False


myStream = tweepy.Stream(auth=auth, listener=StreamListener())

myStream.filter(track=[KEYWORD], languages=['en'])
