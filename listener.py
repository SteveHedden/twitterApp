from tweepy import Stream, OAuthHandler, StreamListener
import tweepy as tw
from IPython.display import clear_output
import json
import yaml

# Load parameters
with open('parameters.yaml') as file:
    parameters = yaml.full_load(file)

consumer_key = parameters['consumer_key']
consumer_secret = parameters['consumer_secret']
access_token = parameters['access_token']
access_token_secret = parameters['access_token_secret']
tweet_batch_size = parameters['tweet_batch_size']
tweet_buffer_size = parameters['tweet_buffer_size']
tracker = parameters['tracker']
restart_listener = parameters['restart_listener']
restart_file = parameters['restart_file']

# Connect to API
# TODO: Streamer can still run into limiting.
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=False)
#api = tw.API(auth)

# Buffer some number of tweets in a lit
# Dump buffered tweets into an extended list
# Once extended list exceeds batch limit, drop it by size of buffer

tweet_buffer=[]      # Create a list that will hold tweets
if restart_listener == 1:
    with open(restart_file) as json_data:
        d = json.load(json_data)
        json_data.close()
    raw_tweets=list(d)
else:
    raw_tweets=[]
    
#N=1000       # Set a number of tweets to collect before dumping into a file
class listener(StreamListener):
    def on_data(self,data):      # triggered when data appears
        tweet=json.loads(data)   # add the twitter data to the tweet list (pase in a dictionary format)
        clear_output()
        #tweet_buffer.append(tweet)
        #print(len(tweet_buffer), '/', len(raw_tweets) ,tweet['user']['screen_name'],':',tweet['text'])
        try:
            print(len(tweet_buffer) + 1, '/', len(raw_tweets), tweet['user']['screen_name'],':',tweet['text'])
            tweet_buffer.append(tweet)
        except:
            #sys.exit()
            #tweet_buffer = tweet_buffer[0:len(tweet_buffer)-1]
            #print(tweet_buffer[-1])
            #del tweet_buffer[-1]
            pass
        if len(tweet_buffer) >= tweet_buffer_size:
            raw_tweets.extend(tweet_buffer)
            if len(raw_tweets) >= tweet_batch_size + tweet_buffer_size:
                del raw_tweets[:tweet_buffer_size]
            f=open('raw_tweets.json','w')     # MacOS path
            json.dump(raw_tweets,f)               # dump the tweets into a json file
            f.close()
            tweet_buffer[:]=[]                      # reset the tweet list
            
    '''def on_error(self,status):
        print(status)'''
    
    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False

        
#Track and filter
twitterStream=Stream(auth, listener())
track=tracker
twitterStream.filter(track=track, is_async=True)  # track can also take a location argument