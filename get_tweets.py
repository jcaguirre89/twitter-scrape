import twitter
from collections import namedtuple
import pandas as pd
from secrets import api_key
import time


Tweet = namedtuple('Tweet', ['date', 'id', 'text',
                             'user_handle', 'place',
                             'user_id', 'followers_count',
                             'favorite_count', 'retweet_count'])


def build_tweet_entry(tweet):
    tweet_instance = Tweet(tweet.created_at, tweet.id,
                           tweet.full_text, tweet.user.screen_name,
                           tweet.place, tweet.user.id,
                           tweet.user.followers_count,
                           tweet.favorite_count, tweet.retweet_count
                           )

    return tweet_instance


def get_tweets(start_id, parameters):
    """
    Returns a generator that returns tweets one by one
    :param start_id:
    :param parameters:
    :return:
    """
    latest_tweets = api.GetSearch(**parameters)
    if not len(latest_tweets):
        return []
    last_id = latest_tweets[-1].id
    for tweet in latest_tweets:
        yield tweet

    print(start_id, last_id, start_id - last_id)
    while last_id >= start_id:
        parameters['max_id'] = last_id - 1
        results = api.GetSearch(**parameters)
        if len(results):
            for tweet in results:
                yield tweet
            last_id = results[-1].id
            print(f'last seen: {last_id} @ {results[-1].created_at}')
        else:
            break


if __name__ == '__main__':
    api = twitter.Api(*api_key, sleep_on_rate_limit=True, tweet_mode='extended')


    start_id = 1132073789481787392  # Around May 25
    end_id   = 1134881073773633536  # Around June 1

    parameters = {
        'term': 'piñera OR bachelet OR sur OR tornado OR chile OR talcahuano OR conception OR angeles OR tromba OR armada',
        'count': 100,
        'include_entities': False,
        'lang': 'es',
    }


    tweets = []
    for idx, tweet in enumerate(get_tweets(start_id, parameters)):
        tweets.append(build_tweet_entry(tweet))
        # Store intermediate results every 50k entries
        if idx % 50000 == 0:
            df = pd.DataFrame(tweets)
            df.to_pickle(f"{round(time.time())}_output.pkl")

    df = pd.DataFrame(tweets)
    df.to_pickle(f"{round(time.time())}_output.pkl")
