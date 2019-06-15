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
from dateutil.parser import parse
import datetime
import csv

import twitter

Tweet = namedtuple('Tweet', ['date', 'timestamp', 'id', 'text',
                             'user_handle', 'place',
                             'user_id', 'followers_count',
                             'favorite_count', 'retweet_count',
                             'is_retweet', 'city'])

try:
    from secrets import api_key
except ImportError:
    raise ImportError('Remember to create a secrets.py file with the twitter API keys')

DEFAULT_LANG = 'en'
# Just an old start id, will run until 7 days are reached
DEFAULT_START_ID = 1132073789481787392


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
    return parser


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

    # Build parameters dict
    parameters = {
        'term': _build_search_term(comma_sep_terms),
        'count': 100,
        'include_entities': False,
        'lang': lang,
    }

    # Create file and write top row with column names
    file_name = f"{round(time.time())}_output.csv"
    with open(file_name, mode='w', encoding='utf-8-sig', newline='') as fp:
        writer = csv.writer(fp)
        writer.writerow(Tweet._fields)

    for tweet in get_tweets(start_id, parameters):
        tweet_instance = _process_tweet(tweet)
        with open(file_name, mode='a', encoding='utf-8-sig', newline='') as fp:
            writer = csv.writer(fp)
            writer.writerow(tweet_instance)


def _build_search_term(comma_sep_terms):
    """
    Takes a string of comma-separated terms to be searched
    and returns it as one string as the API expects it
    """
    entries = comma_sep_terms.split(',')
    if len(entries) == 1:
        # Single term to search
        return entries[0]
    return ' OR '.join(entries)


def _parse_date(date):
    """
    receives a date as a string and returns a timestamp
    Turn back to datetime: datetime.datetime.fromtimestamp(timestamp)
    Timezone is always UTC
    """
    # sample input: 'Thu Jun 13 21:21:39 +0000 2019'
    # Remove day of week (first 4 chars)
    date = date[4:]
    # Remove microseconds
    date, year = date.split('+')
    date = date.strip()
    date += ' 2019'
    date = parse(date, ignoretz=True)
    timestamp = date.replace(tzinfo=datetime.timezone.utc).timestamp()
    return timestamp


def _process_tweet(tweet):
    """ Receives a Tweet from the Search API and processes it """

    text = tweet.full_text.replace('\n', ' ')
    timestamp = _parse_date(tweet.created_at)
    is_retweet = True if text.startswith('RT') else False
    try:
        city = tweet.place['full_name']
    except TypeError:
        city = None

    tweet_instance = Tweet(tweet.created_at, timestamp, tweet.id,
                           tweet.full_text, tweet.user.screen_name,
                           tweet.place, tweet.user.id,
                           tweet.user.followers_count,
                           tweet.favorite_count, tweet.retweet_count,
                           is_retweet, city,
                           )

    return tweet_instance


if __name__ == '__main__':
    api = twitter.Api(*api_key, sleep_on_rate_limit=True, tweet_mode='extended')
    main()
