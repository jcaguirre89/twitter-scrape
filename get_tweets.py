"""
A generator that searches for tweets from the Twitter Search API for a given set of parameters (like
term, geocode, etc.) and an additional function to put the results into a pandas dataframe and save
the results to a pickle file, for later exploration with jupyter.

Sleeps automatically if the rate limit set by the twitter api is reached.

author: https://github.com/jcaguirre89
June 3, 2019
"""

from argparse import ArgumentParser
import time
from collections import namedtuple

import pandas as pd
import twitter

try:
    from secrets import api_key
except ImportError:
    raise ImportError('Remember to create a secrets.py file with the twitter API keys')

DEFAULT_LANG = 'en'
# Just an old start id, will run until 7 days are reached
DEFAULT_START_ID = 1132073789481787392
INTERMEDIATE_SAVE = 50000

Tweet = namedtuple('Tweet', ['date', 'id', 'text',
                             'user_handle', 'place',
                             'user_id', 'followers_count',
                             'favorite_count', 'retweet_count'])



def _build_parser():
    parser = ArgumentParser()
    parser.add_argument('--terms', type=str, dest='terms',
                        help='comma-sepparated list of terms to search: term1,term2,term3',
                        required=True)
    parser.add_argument('--start_id', type=int, dest='start_id',
                        help='Tweet ID that marks the oldest tweet to search for, so once reached it stops',
                        required=False, default=DEFAULT_START_ID)
    parser.add_argument('--lang', type=str, dest='lang',
                        help='Search only tweets in this language. Default: en',
                        required=False, default=DEFAULT_LANG)
    parser.add_argument('--checkpoint', type=int, dest='intermediate_save',
                        help='Save a pickle dataframe every time a multiple of this number of tweets is reached',
                        required=False, default=INTERMEDIATE_SAVE)
    parser.add_argument('--csv', type=bool, dest='save_csv',
                        help='Save a CSV file as well',
                        required=False, default=False)
    return parser

def _build_tweet_entry(tweet):
    """ Builds a simplified version of a tweet, just with the basic, useful information """
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


def main():
    """
    Run the get_tweets generator and return a pandas dataframe with results, while storing
    intermediate and final results as a pickle file in the current working directory.

    """

    # Get args
    parser = _build_parser()
    options = parser.parse_args()
    start_id = options.start_id
    comma_sep_terms = options.terms
    lang = options.lang
    intermediate_save = options.intermediate_save
    save_csv = options.save_csv

    # Build parameters dict
    parameters = {
        'term': build_search_term(comma_sep_terms),
        'count': 100,
        'include_entities': False,
        'lang': lang,
    }

    tweets = []
    for idx, tweet in enumerate(get_tweets(start_id, parameters)):
        tweets.append(_build_tweet_entry(tweet))
        # Store intermediate results
        if (idx + 1) % intermediate_save == 0:
            df = pd.DataFrame(tweets)
            df.to_pickle(f"{round(time.time())}_output.pkl")

    if len(tweets):
        df = pd.DataFrame(tweets)
        df.to_pickle(f"{round(time.time())}_output.pkl")

        earliest = df.loc[0, 'date']
        latest = df.loc[df.shape[0] - 1, 'date']
        print(f'Got {df.shape[0]} tweets going from {earliest} to {latest}')

        # Not working great, doesnt handle \r and \n inside emojis
        if save_csv:
            df.to_csv(f"{round(time.time())}_output.csv", encoding='utf-16')
        return df
    else:
        print("Didn't find any tweets for the given parameters")

def build_search_term(comma_sep_terms):
    """
    Takes a string of comma-separated terms to be searched
    and returns it as one string as the API expects it
    """
    entries = comma_sep_terms.split(',')
    if len(entries) == 1:
        # Single term to search
        return entries[0]
    search_term = ''
    for term in entries:
        search_term += term + ' OR '
    # Remove last 4 digits (' OR ')
    search_term = search_term[:-4]
    return search_term


if __name__ == '__main__':
    api = twitter.Api(*api_key, sleep_on_rate_limit=True, tweet_mode='extended')
    main()
