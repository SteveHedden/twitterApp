import datetime
from tweepy import Stream, OAuthHandler, StreamListener
import tweepy as tw
from IPython.display import clear_output
import json
import yaml
import os
import functions as fn
import shutil

# Load parameters
with open('parameters.yaml') as file:
    parameters = yaml.full_load(file)
    # Create path if it doesn't exist'
    prj_path = 'data/' + str(parameters['project']) + '/'
    out_path = 'data/' + str(parameters['project']) + '/raw_out/'
    preproc_path = 'data/' + str(parameters['project']) + '/preprocessed/'
    if not os.path.exists(prj_path):
        os.makedirs(prj_path)
        os.makedirs(out_path)
        os.makedirs(preproc_path)
    #filename = os.path.basename(file)
    #copyfile(file, os.path.join('data/' + str(parameters['project']) + '/', 'parameters.yaml'))
    newPath = shutil.copy('parameters.yaml', 'data/' + str(parameters['project']) + '/parameters.yaml')

consumer_key = parameters['consumer_key']
consumer_secret = parameters['consumer_secret']
access_token = parameters['access_token']
access_token_secret = parameters['access_token_secret']
tweet_batch_size = parameters['tweet_batch_size']
tweet_buffer_size = parameters['tweet_buffer_size']
tracker = parameters['tracker']
#restart_listener = parameters['restart_listener']
#restart_file = parameters['restart_file']
project = parameters['project']
refresh_interval = parameters['refresh_interval']

# Connect to API
# TODO: Streamer can still run into limiting.
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=False)
#api = tw.API(auth)

# Create path if it doesn't exist'
out_path = 'data/' + str(project) + '/raw_out/'
preproc_path = 'data/' + str(project) + '/preprocessed/'
if not os.path.exists(out_path):
    os.makedirs(out_path)

raw_tweets = []
class listener(StreamListener):
    def on_data(self,data):      # triggered when data appears
        tweet=json.loads(data)   # add the twitter data to the tweet list (paste in a dictionary format)
        clear_output()
        #print(len(raw_tweets),tweet['user']['screen_name'],':',tweet['text'])
        raw_tweets.append(tweet)
        if len(raw_tweets) > tweet_buffer_size:      # if the length of the list 'tweets' is greater than N, then:
            timestamp=datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            #f=open(r'data\raw_out\dataset_%s.json'%timestamp,'w')     # Windows path
            f=open(str(out_path) + 'dataset_%s.json'%timestamp,'w')     # MacOS path
            json.dump(raw_tweets,f)  # dump the tweets into a json file
            f.close()
            raw_tweets[:]=[]         # reset the tweet list
            if len(os.listdir(out_path)) > tweet_batch_size / tweet_buffer_size:
                oldest_timestamp = sorted(os.listdir(out_path))[0]
                os.remove(str(out_path) + str(oldest_timestamp)) # Remove earliest raw_data
            else:
                pass
            try:
                t0 = int(sorted(os.listdir(preproc_path))[-1].split('_')[-1].split('.')[0])
            except:
                t0 = int(sorted(os.listdir(out_path))[0].split('_')[-1].split('.')[0])
            #t0 = int(sorted(os.listdir(out_path))[0].split('_')[-1].split('.')[0])
            t1 = int(timestamp)
            dt = t1 - t0
            if dt > refresh_interval:
                #os.system('preprocessor.py')
                tweet_list = fn.concat_raw_files()
                fn.write_graph_dict(tweet_list)

    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False
        
#Track and filter
twitterStream=Stream(auth, listener())
track=tracker
twitterStream.filter(track=track, is_async=True)  # track can also take a location argument