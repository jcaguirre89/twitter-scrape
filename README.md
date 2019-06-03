# Twitter search tool
Simple command line tool to interact with [Twitter's search API](https://developer.twitter.com/en/docs/tweets/search/api-reference/get-search-tweets.html). Built on top of the [`python-twitter`](https://github.com/bear/python-twitter) to provide a simpler interface just to the `GetSearch` method, built mostly because I needed a tool for historical searches. It accepts a list of terms, a language and a start ID and searches historical tweets (can't go back further than 7 days as that's the oldest the open Search API will go). 

Once done it saves a pickled pandas dataframe with the resulting tweets. Also saves intermediate checkpoints (every 50k by default) in case the program crashes for any reason. It can take a long time to run, as the API has a rate limit and `python-twitter` will sleep when it's reached, which happens about every 5k tweets downloaded. for a 1.5M download it took around 20 hours to run (probably 90% of this time was spent sleeping anyway).

## Sample usage:
```bash
# Search for all tweets that have the terms 'Chile' or 'Santiago', in english, going back as far as the tweet with id 1132073789481787392 (around may 25, 2019)
python get_tweets.py --terms Chile,Santiago --start_id 1132073789481787392 --lang es
```

## Requirements
Must create a `secrets.py` file in the working directory with the following form:
```python
from collections import namedtuple


ApiKey = namedtuple('ApiKey', [
    'CONSUMER_KEY',
    'CONSUMER_SECRET',
    'ACCESS_TOKEN',
    'ACCESS_TOKEN_SECRET'
])

# Replace these strings with the corresponding keys/tokens
api_key = ApiKey(
    'consumer-key',
    'consumer-secret',
    'access-token',
    'access-token-secret',
)
```
