import requests
import os
import os.path
import json
import csv
import time
import pandas as pd
from csv import writer
from minet import multithreaded_resolve
from time import sleep
from ural import get_domain_name

from dotenv import load_dotenv
from utils import import_data

def create_url(ids):

    tweet_fields = 'tweet.fields=attachments,author_id,context_annotations,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,public_metrics,possibly_sensitive,referenced_tweets,source,text,withheld'
    expansions = 'referenced_tweets.id.author_id'
    media_fields = 'public_metrics'
    user = 'user.fields=created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld'
    url = 'https://api.twitter.com/2/tweets?{}&{}&expansions={}&{}'.format(ids, tweet_fields,  expansions, user)
    return url

def create_headers(bearer_token):

    headers = {'Authorization': 'Bearer {}'.format(bearer_token)}
    return headers

def connect_to_endpoint(url, headers):
    response = requests.request('GET', url, headers=headers)
    if response.status_code != 200:
        raise Exception(
            'Request returned an error: {} {}'.format(
                response.status_code, response.text
            )
        )
    return response.json()

def write_results(json_response, filename):

    with open(filename, 'a+') as tweet_file:

        writer = csv.DictWriter(tweet_file,
                                ['type_of_tweet',
                                'referenced_tweet_id',
                                'in_reply_to_username',
                                'retweeted_username',
                                'quoted_username',
                                'id',
                                'text',
                                'created_at',
                                'lang',
                                'possibly_sensitive',
                                'retweet_count',
                                'reply_count',
                                'like_count',
                                'quote_count',
                                'conversation_id',
                                'source',
                                'expanded_urls',
                                'domain_name',
                                'hashtags',
                                'author_id',
                                'author_name',
                                'username',
                                'account_created_at',
                                'account_description',
                                'followers_count',
                                'following_count',
                                'tweet_count',
                                'listed_count',
                                'error'], extrasaction='ignore')

        data_index = {}

        if 'data' and 'includes' in json_response:

            for tweet in json_response['data']:

                tweet['retweet_count'] = tweet['public_metrics']['retweet_count']
                tweet['reply_count'] = tweet['public_metrics']['reply_count']
                tweet['like_count'] = tweet['public_metrics']['like_count']
                tweet['quote_count'] = tweet['public_metrics']['quote_count']
                tweet['text'] = tweet['text'].lower()

                if 'entities' in tweet.keys():

                    if "hashtags" in tweet["entities"]:
                        l=len(tweet["entities"]["hashtags"])
                        tweet["hashtags"] = []

                        for i in range(0,l):
                            a = tweet["entities"]["hashtags"][i]["tag"]
                            tweet["hashtags"].append(a)
                    else:
                        tweet["hashtags"] = []
                else:
                    tweet['expanded_urls'] = []
                    tweet['domain_name'] = []
                    tweet["hashtags"] = []

                if 'tweets' in json_response['includes']:

                    for tw in json_response['includes']['tweets']:

                        if 'referenced_tweets' in tweet.keys():

                            tweet['type_of_tweet'] = tweet['referenced_tweets'][0]['type']
                            tweet['referenced_tweet_id'] = tweet['referenced_tweets'][0]['id']

                            if tweet['referenced_tweets'][0]['id']==tw['id']:

                                if 'public_metrics' in tw:

                                    tweet['retweet_count'] = tw['public_metrics']['retweet_count']
                                    tweet['reply_count'] = tw['public_metrics']['reply_count']
                                    tweet['like_count'] = tw['public_metrics']['like_count']
                                    tweet['quote_count'] = tw['public_metrics']['quote_count']
                                    tweet['text'] = tw['text'].lower()

                                if 'entities' in  tw.keys():

                                    if "urls" in tw["entities"]:

                                        lu=len(tw["entities"]['urls'])

                                        tweet['expanded_urls']=[]
                                        tweet['domain_name']=[]

                                        for i in range(0,lu):

                                            link = tw["entities"]['urls'][i]['expanded_url']

                                            if len(link) < 35:

                                                if 'unwound_url' in tw["entities"]['urls'][i].keys():
                                                    b = tw["entities"]['urls'][i]['unwound_url']
                                                    c = get_domain_name(tw["entities"]['urls'][i]['unwound_url'])

                                                elif 'unwound_url' not in tw["entities"]['urls'][i].keys():
                                                    for result in multithreaded_resolve([link]):
                                                        b = result.stack[-1].url
                                                        c = get_domain_name(result.stack[-1].url)

                                                else:
                                                    b = tw["entities"]['urls'][i]['expanded_url']
                                                    c = get_domain_name(tw["entities"]['urls'][i]['expanded_url'])

                                                tweet['expanded_urls'].append(b)
                                                tweet['domain_name'].append(c)

                                            else:
                                                d = tw["entities"]['urls'][i]['expanded_url']
                                                e = get_domain_name(tw["entities"]['urls'][i]['expanded_url'])

                                                tweet['expanded_urls'].append(d)
                                                tweet['domain_name'].append(e)

                                    else:
                                        tweet['expanded_urls'] = []
                                        tweet['domain_name'] = []

                                    if "hashtags" in tw["entities"]:
                                        l=len(tw["entities"]["hashtags"])
                                        tweet["hashtags"] = []

                                        for i in range(0,l):
                                            a = tw["entities"]["hashtags"][i]["tag"]
                                            tweet["hashtags"].append(a)
                                    else:
                                        tweet["hashtags"] = []

                                else:
                                    tweet['expanded_urls'] = []
                                    tweet['domain_name'] = []
                                    tweet["hashtags"] = []

                            if tweet['referenced_tweets'][0]['type'] == 'retweeted':

                                if 'entities' in tweet :

                                    if 'mentions' in tweet['entities'].keys():

                                        if tweet['entities']['mentions'][0]['id'] == tw['author_id'] :

                                            a = tweet['entities']['mentions'][0]['username']
                                            b = a.lower()

                                            tweet['retweeted_username'] = b

                            if tweet['referenced_tweets'][0]['type'] == 'quoted':

                                if 'entities' in tweet.keys():

                                    if 'urls' in tweet['entities']:

                                        l = len(tweet['entities']['urls'])

                                        for i in range(0,l):

                                            if 'expanded_url' in tweet['entities']['urls'][i].keys():

                                                url = tweet['entities']['urls'][i]['expanded_url']

                                                if tweet['referenced_tweets'][0]['id'] in url:

                                                    if 'https://twitter.com/' in url:

                                                        a = url.split('https://twitter.com/')[1]
                                                        b = a.split('/status')[0].lower()

                                                        tweet['quoted_username'] = b


                if 'users' in json_response['includes']:

                    for user in json_response['includes']['users']:

                        if tweet['author_id'] == user['id']:

                            tweet['author_name'] = user['name'].lower()
                            tweet['username'] = user['username'].lower()
                            tweet['account_created_at'] = user['created_at']
                            tweet['account_description'] = user['description'].lower()
                            tweet['followers_count'] = user['public_metrics']['followers_count']
                            tweet['following_count'] = user['public_metrics']['following_count']
                            tweet['tweet_count'] = user['public_metrics']['tweet_count']
                            tweet['listed_count'] = user['public_metrics']['listed_count']
                            tweet['protected_account'] = user['protected']

                        if 'in_reply_to_user_id' in tweet.keys():

                            if tweet['in_reply_to_user_id'] == user['id']:

                                a = user['username'].lower()
                                tweet['in_reply_to_username'] = a

                if 'withheld' in tweet.keys():

                    tweet['withheld']=tweet['withheld']['copyright']

                if 'entities' in tweet.keys():

                    if "urls" in tweet["entities"]:

                        lu=len(tweet["entities"]['urls'])

                        tweet['expanded_urls']=[]
                        tweet['domain_name']=[]

                        for i in range(0,lu):

                            link = tweet["entities"]['urls'][i]['expanded_url']
                            #print (i,tweet["entities"]['urls'][i].keys())

                            # if tweet['id'] == "1455491668728328192":
                            #     print(json_response['data'])

                            if len(link) < 35:

                                if 'unwound_url' in tweet["entities"]['urls'][i].keys():
                                    b = tweet["entities"]['urls'][i]['unwound_url']
                                    c = get_domain_name(tweet["entities"]['urls'][i]['unwound_url'])

                                elif 'unwound_url' not in tweet["entities"]['urls'][i].keys():
                                    #print('unwound not there!')
                                    #print(tweet['id'])
                                    for result in multithreaded_resolve([link]):
                                        b = result.stack[-1].url
                                        c = get_domain_name(result.stack[-1].url)

                                # tweet['expanded_urls'].append(b)
                                # tweet['domain_name'].append(c)
                                    # tweet['expanded_urls'].append(b)
                                    # tweet['domain_name'].append(c)

                                else:
                                    b = tweet["entities"]['urls'][i]['expanded_url']
                                    c = get_domain_name(tweet["entities"]['urls'][i]['expanded_url'])

                                tweet['expanded_urls'].append(b)
                                tweet['domain_name'].append(c)


                            else:
                                d = tweet["entities"]['urls'][i]['expanded_url']
                                e = get_domain_name(tweet["entities"]['urls'][i]['expanded_url'])

                                tweet['expanded_urls'].append(d)
                                tweet['domain_name'].append(e)
                    else:
                        tweet['expanded_urls'] = []
                        tweet['domain_name'] = []
                writer.writerow(tweet)

        if 'errors' in json_response:
            tweet = {}
            tweet['error'] = json_response['errors'][0]['title']
            tweet['id'] = json_response['errors'][0]['resource_id']
            writer.writerow(tweet)

def get_tweet_ids (input):

    data_path = os.path.join('.', 'data', input)
    df = pd.read_csv(data_path, dtype='str')
    list_id = df['tweetId'].tolist()
    print('there are', len(list_id), 'tweets')

    list_id = list(set(list_id))
    print('there are', len(list_id), 'unique tweets')
    for item in list_id:
        if 'E' in item:
            print(item)
            list_id.remove(item)

    return list_id

def collect_tweets_by_id(bearer_token, file_name, ids):

    filename = os.path.join('.', 'data', file_name)
    file_exists = os.path.isfile(filename)

    with open(filename, 'a+') as tweet_file:

        writer = csv.DictWriter(tweet_file,
                                ['type_of_tweet',
                                'referenced_tweet_id',
                                'in_reply_to_username',
                                'retweeted_username',
                                'quoted_username',
                                'id',
                                'text',
                                'created_at',
                                'lang',
                                'possibly_sensitive',
                                'retweet_count',
                                'reply_count',
                                'like_count',
                                'quote_count',
                                'conversation_id',
                                'source',
                                'expanded_urls',
                                'domain_name',
                                'hashtags',
                                'author_id',
                                'author_name',
                                'username',
                                'account_created_at',
                                'account_description',
                                'followers_count',
                                'following_count',
                                'tweet_count',
                                'listed_count',
                                'error'],
                                extrasaction='ignore')
        if not file_exists:
            writer.writeheader()

    bearer_token = os.getenv('TWITTER_TOKEN')
    url = create_url(ids)
    headers = create_headers(bearer_token)
    json_response = connect_to_endpoint(url, headers)
    write_results(json_response, filename)

def get_100_tweets(list_id, file_name, bearer_token):

    l=len(list_id)

    for i in range(0,l,100):
        print(i)
        a=str(list_id[i:i+99])
        b='ids=' + a[1:-1]
        ids=b.replace('\'' ,'')
        ids=ids.strip()
        ids=ids.replace(' ' ,'')
        collect_tweets_by_id(bearer_token, file_name, ids)
        sleep(6)

    return ids

def main():

    load_dotenv()
    input = 'notes-00000.csv'
    list_id = get_tweet_ids (input)

    file_name = 'tweet_metrics_notes_2022_05_02_with_links.csv'
    bearer_token = os.getenv('TWITTER_TOKEN')

    get_100_tweets(list_id, file_name, bearer_token)

if __name__ == '__main__':

    main()
