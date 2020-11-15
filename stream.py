from config import *
import sys
import socket
import json
import tweepy
import preprocessor


auth = tweepy.OAuthHandler(TW_API_KEY, TW_API_KEY_S)
auth.set_access_token(TW_ACCESS_TOKEN, TW_ACCESS_TOKEN_S)

KEYWORD = '#covid19'


def preprocessTweet(status):
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
        tweet['text'] = preprocessor.clean(text)

    # Location
    location = None
    if hasattr(status, 'coordinates') and status.coordinates is not None:
        location = status.coordinates
    elif hasattr(status, 'place') and status.place is not None:
        location = preprocessor.clean(status.place.full_name)
    elif hasattr(status, 'user') and status.user.location is not None:
        location = preprocessor.clean(status.user.location)
    if location is not None:
        tweet['location'] = location
        print(tweet['location'] + '\n')

    return tweet


print('Starting socket')
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((STREAM_IP, STREAM_PORT))
s.listen(1)
conn, addr = s.accept()
print('Received socket connection')


class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        json_tweet = (json.dumps(preprocessTweet(status)) + '\n').encode('utf-8')
        conn.send(json_tweet)
        return True

    def on_error(self, status_code):
        print(f'Tweepy StreamListener error {status_code}')
        return False


myStream = tweepy.Stream(auth=auth, listener=StreamListener())

myStream.filter(track=[KEYWORD], languages=['en'])
