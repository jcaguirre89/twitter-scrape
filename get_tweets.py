"""
A generator that searches for tweets from the Twitter Search API for a given set of parameters (like
term, geocode, etc.) and an additional function to put the results into a pandas dataframe and save
the results to a pickle file, for later exploration with jupyter.

Sleeps automatically if the rate limit set by the twitter api is reached.
"""

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
    Returns a generator that fetches all tweets with the given parameters, starting
    from the latest one and going back until the `start_id` is reached.


    :param start_id: `int`. A Twitter ID that marks the oldest tweet to fetch. The open Twitter
    search API only returns up to around a week of history
    :param parameters: a dictionary with parameters that will be given to the `GetSearch` method of
    the `python-twitter` library. Must include at least one of the following keys: `term`, `geocode`,
    or `raw_query`.
    :return: yields a namedtuple corresponding to a tweet.
    """
    latest_tweets = api.GetSearch(**parameters)
    if not len(latest_tweets):
        return []
    last_id = latest_tweets[-1].id
    for tweet in latest_tweets:
        yield tweet

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


def build_tweet_db(start_id, parameters, intermediate_save=50000):
    """
    Run the get_tweets generator and return a pandas dataframe with results, while storing
    intermediate and final results as a pickle file in the current working directory.

    :param start_id: `int`. A Twitter ID that marks the oldest tweet to fetch. The open Twitter
    search API only returns up to around a week of history
    :param parameters: a dictionary with parameters that will be given to the `GetSearch` method of
    the `python-twitter` library. Must include at least one of the following keys: `term`, `geocode`,
    or `raw_query`.
    :return: Pandas DataFrame
    """

    tweets = []
    for idx, tweet in enumerate(get_tweets(start_id, parameters)):
        tweets.append(build_tweet_entry(tweet))
        # Store intermediate results
        if idx % intermediate_save == 0:
            df = pd.DataFrame(tweets)
            df.to_pickle(f"{round(time.time())}_output.pkl")

    df = pd.DataFrame(tweets)
    df.to_pickle(f"{round(time.time())}_output.pkl")

    return df


if __name__ == '__main__':
    api = twitter.Api(*api_key, sleep_on_rate_limit=True, tweet_mode='extended')

    #start_id = 1132073789481787392  # Around May 25
    start_id = 1135634520554835969  # Around May 25

    search_term = ('piñera OR bachelet OR sur OR tornado OR'
                   ' chile OR talcahuano OR conception OR '
                   'angeles OR tromba OR armada')

    parameters = {
        'term': search_term,
        'count': 100,
        'include_entities': False,
        'lang': 'es',
    }

    df = build_tweet_db(start_id, parameters, intermediate_save=50000)
