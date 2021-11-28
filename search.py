from datetime import date, timedelta
import dateutil.parser
import time
import requests
import os
import json
import csv
# Client secrets
import secrets

# Collection of scripts to facilitate the compilation of datasets from Twitter v2 API
# Note: these scripts are optmized for the v2 API academic access tier

# Alex Norton · 2021 ~ MIT License

# Largely inspired/grateful for the excellent tutorial provided by Andrew Edward on Towards Data Science
# https://towardsdatascience.com/an-extensive-guide-to-collecting-tweets-from-twitter-api-v2-for-academic-research-using-python-3-518fcb71df2a


# Transform datetime into twitter time
def tweet_time(day):
    tweet_time_start = 'T00:00:00.000Z' 
    tweet_time_end = 'T23:59:59.000Z' 
    
    day_start = day.isoformat() + tweet_time_start
    day_end = day.isoformat() + tweet_time_end

    return [day_start, day_end]


# Transform datetime into twitter time
def tweet_time_hour(day, hour):
    
    if (hour < 10):
        tweet_time_start = 'T0' + str(hour) + ':00:00.000Z'
        tweet_time_end = 'T0' + str(hour) + ':59:59.000Z' 
    else:
        tweet_time_start = 'T' + str(hour) + ':00:00.000Z' 
        tweet_time_end = 'T' + str(hour) + ':59:59.000Z' 
    
    day_start = day.isoformat() + tweet_time_start
    day_end = day.isoformat() + tweet_time_end

    return [day_start, day_end]


# Fetch client secrets
def auth():
    return secrets.bearer_token


# Create headers for API requests
def create_headers(bearer_token):
    headers = { "Authorization": f"Bearer {bearer_token}" }
    return headers


# Create "Search" URL, define which fields the API request will return
def create_url(keyword, start_time, end_time, max_results = 10):
    
    # Change to the endpoint you want to collect data from
    search_url = "https://api.twitter.com/2/tweets/search/all"

    # Change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_time,
                    'end_time': end_time,
                    'max_results': max_results,
                    'expansions': 'author_id,in_reply_to_user_id,geo.place_id',
                    'tweet.fields': 'id,text,author_id,in_reply_to_user_id,geo,conversation_id,created_at,lang,public_metrics,referenced_tweets,reply_settings,source',
                    'user.fields': 'id,name,username,created_at,description,public_metrics,verified',
                    'place.fields': 'full_name,id,country,country_code,geo,name,place_type',
                    'next_token': {}}
    return (search_url, query_params)


# Create "Count" URL, define which fields the API request will return
def create_count_url(keyword, start_time, end_time):
    
    # Change to the endpoint you want to collect data from
    search_url = "https://api.twitter.com/2/tweets/counts/all"

    # Change params based on the endpoint you are using
    query_params = {'query': keyword,
                    'start_time': start_time,
                    'end_time': end_time,
                    'next_token': {}}
    return (search_url, query_params)


# Connect to the API endpoint · generalized for any Twitter API product
def connect_to_endpoint(url, headers, params, next_token=None):

    params['next_token'] = next_token 
    response = requests.request("GET", url, headers=headers, params=params)
    
    print(response.status_code)

    if response.status_code != 200:
        raise Exception(response.status_code, response.text)

    return response.json()


# Utility function to write output in JSON format
def write_json(data, name='test'): 
    out_file = 'data/' + name + '.json'
    with open(out_file, 'w') as outfile:
        json.dump(data, outfile, indent=2,)


# Check tweet volume for query within date/time range
def getTweetCounts(day, day_last=None):
    next_token = None
    total_results = 0
    flag = True
    
    # Format date/time with respect to input params
    if day_last:        
        start_time = day.isoformat() + 'T00:00:00.000Z' 
        end_time = day_last.isoformat() + 'T00:00:00.000Z' 
    else:
        start_time, end_time = tweet_time(day)
    
    url = create_count_url(keyword, start_time, end_time)

    while flag:

        # Carry out request
        json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
        result_count = json_response['meta']['total_tweet_count']

        if 'next_token' in json_response['meta']:
            # Save the token to use for next call
            next_token = json_response['meta']['next_token']
            if result_count is not None and result_count > 0 and next_token is not None:
                print("Start Date: ", start_time)
                total_results += result_count
                print(total_results)
                time.sleep(3)

        # if no token exists
        else:
            if result_count is not None and result_count > 0:
                print("Start Date: ", start_time)
                total_results += result_count
                print(total_results)
                time.sleep(3)

            next_token = None
            flag = False


# Return tweet collection for a given day/duration
def getTweetCollection(day, max_results, max_results_rx):

    # Sample hourly (24/day)
    for hour in range(1, 24):

        total_results = 0
        flag = True
        next_token = None
        
        start_time, end_time = tweet_time_hour(day, hour)
        url = create_url(keyword, start_time, end_time, max_results_rx)

        while flag:
            # Check if max_results_day/hour reached
            if total_results > max_results:
                break

            # Otherwise, carry out request
            json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
            result_count = json_response['meta']['result_count']

            if 'next_token' in json_response['meta']:
                # Save the token to use for next call
                next_token = json_response['meta']['next_token']

                if result_count is not None and result_count > 0 and next_token is not None:
                    print("Start Date: ", start_time)
                    append_to_csv(json_response, "data.csv")
                    total_results += result_count
                    time.sleep(3)
            
            # if no token exists
            else:
                if result_count is not None and result_count > 0:
                    append_to_csv(json_response, "data.csv")
                    total_results += result_count
                    time.sleep(3)

                next_token = None
                flag = False
        

# CSV from JSON parser
def append_to_csv(json_response, fileName):

    counter = 0

    # Open OR create the target CSV file
    csvFile = open(fileName, "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)

    # For each tweet in API response
    for tweet in json_response['data']:        

        # 1. Author ID
        author_id = tweet['author_id']

        # 2. Time created
        created_at = dateutil.parser.parse(tweet['created_at'])

        # 3. Geolocation
        if ('geo' in tweet):   
            geo = tweet['geo']['place_id']
        else:
            geo = " "

        # 4. Tweet ID
        tweet_id = tweet['id']

        # 5. Language
        lang = tweet['lang']

        # 6. Tweet metrics
        retweet_count = tweet['public_metrics']['retweet_count']
        reply_count = tweet['public_metrics']['reply_count']
        like_count = tweet['public_metrics']['like_count']
        quote_count = tweet['public_metrics']['quote_count']

        # 7. source
        source = tweet['source']

        # 8. Tweet text
        text = tweet['text']

        # Join 'data' with 'includes' results
        for tweet_meta in json_response['includes']['users']:
            if tweet['author_id'] == tweet_meta['id']:
                # 9. Username
                username = tweet_meta['username']

                # 10. Name
                name = tweet_meta['name']

                # 11. Description
                account_description = tweet_meta['description']

                # 12. Created at
                account_created_at = tweet_meta['created_at']

                # 13. Verified
                verified = tweet_meta['verified']

                # 14. Follwers count
                followers_count = tweet_meta['public_metrics']['followers_count']

                # 15. Following count
                following_count = tweet_meta['public_metrics']['following_count']

                # 16. Tweet count
                tweet_count = tweet_meta['public_metrics']['tweet_count']

                # 17. Listed count
                listed_count = tweet_meta['public_metrics']['listed_count']

                break

        # Assemble all data in a list
        res = [author_id, created_at, geo, tweet_id, lang, like_count, quote_count, reply_count, retweet_count, source, text, username, name, account_description, account_created_at, verified, followers_count, following_count, tweet_count, listed_count]
        
        # Append the result to the CSV file
        csvWriter.writerow(res)
        counter += 1

    # Close the CSV file
    csvFile.close()

    # Print the number of tweets for this iteration
    print("# of Tweets added from this response: ", counter)


# Primary function to run script
def main():

    # Create CSV file
    csvFile = open("data.csv", "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)

    # Create headers for the CSV data · note: this should correspond to content from 'append_to_csv'
    csvWriter.writerow(['author id', 'created_at', 'geo', 'tweet_id','lang', 'like_count', 'quote_count', 'reply_count','retweet_count','source','tweet', 'username', 'name', 'account_description', 'account_created_at', 'verified', 'followers_count', 'following_count', 'tweet_count', 'listed_count'])
    csvFile.close()

    # Make the Twitter API requests
    # Offset %2 to sparsely fill in the data set; starting with 1, then 2
    for delta in range(1, 5, 2): # duration_days
        day = first_date + timedelta(days=delta)
        getTweetCollection(day, max_results_hour, max_results_rx)


# ----------------------------------------------------------------------------
# Inputs for the request · 2132 days » 4500 tweets/day ~ 9 requests / day
# To avoid asking for duplicates, really we want to ask for 4500 tweets / day

first_date = date(2016, 1, 1)
last_date = date(2021, 11, 1)
duration_days = (first_date - last_date).days + 1 # Add 1 to be inclusive of final day

bearer_token = auth()
headers = create_headers(bearer_token)

keyword = '"global warming" OR "climate change" OR #globalwarming OR #climatechange lang:en'
start_time = "2016-01-01T00:00:00.000Z"
end_time = "2021-11-01T00:00:00.000Z"

max_results_rx = 200 # 500 max / 200 for hourly model
max_results_hour = 200
max_results_day = 5000 # ~4500 


# Let's go!
if __name__ == "__main__":
    # getTweetCounts(first_date, last_date)
    main()