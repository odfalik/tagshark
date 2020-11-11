from config import *
import sys
import socket
import tweepy
import preprocessor


auth = tweepy.OAuthHandler(API_KEY, API_KEY_S)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_S)

KEYWORD = '#covid19'


def preprocessTweet(status):
    location = status.user.location or ''

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

    text = preprocessor.clean(text)
    return "{}::{}".format(text, location)


print('Starting socket')
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
conn, addr = s.accept()
print('Received socket connection')


class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        formatted_tweet = preprocessTweet(status).encode('utf-8')
        conn.send(formatted_tweet)
        print(f'{formatted_tweet}\n\n')
        return True

    def on_error(self, status_code):
        print(f'StreamListener Error: status {status_code}')
        return False


myStream = tweepy.Stream(auth=auth, listener=StreamListener())

myStream.filter(track=[KEYWORD], languages=['en'])
