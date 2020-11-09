import tweepy
import socket
import re
from config import *
# import preprocessor


auth = tweepy.OAuthHandler(API_KEY, API_KEY_S)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_S)

hashtag = '#covid19'

def preprocessing(tweet):

    print(tweet)
    
    # Add here your code to preprocess the tweets and  
    # remove Emoji patterns, emoticons, symbols & pictographs, transport & map symbols, flags (iOS), etc
       
    return tweet


def getTweet(status):
    
    # You can explore fields/data other than location and the tweet itself. 
    # Check what else you could explore in terms of data inside Status object

    tweet = ''
    location = status.user.location
    
    if hasattr(status, 'retweeted_status'):  # Check if Retweet
        try:
            tweet = status.retweeted_status.extended_tweet['full_text']
        except AttributeError:
            tweet = status.retweeted_status.text
    else:
        try:
            tweet = status.extended_tweet['full_text']
        except AttributeError:
            tweet = status.text

    return location, preprocessing(tweet)


# create sockets
# s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# s.bind((TCP_IP, TCP_PORT))
# s.listen(1)
# conn, addr = s.accept()

class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        location, tweet = getTweet(status)
        if (tweet != None and location != None):
            print(status.text)
            tweetLocation = location + '::' + tweet + '\n'
            # conn.send(tweetLocation.encode('utf-8'))
        return True

    def on_error(self, status_code):
        print(f'StreamListener Error: status {status_code}')
        return False

myStream = tweepy.Stream(auth=auth, listener=StreamListener())

myStream.filter(track=[hashtag], languages=["en"])


