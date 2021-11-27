from datetime import date, timedelta
import time
import requests
import os
import json
# Client secrets
import secrets

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
# bearer_token = os.environ.get("BEARER_TOKEN")
# bearer_token = secrets.bearer_token


# Transform datetime into twitter time
def tweet_time(day):
    tweet_time_start = 'T00:00:00.000Z' 
    tweet_time_end = 'T23:59:59.000Z' 
    
    day_start = day + tweet_time_start
    day_end = day + tweet_time_end

    return [day_start, day_end]


def auth():
    return secrets.bearer_token


def create_headers(bearer_token):
    headers = { "Authorization": f"Bearer {bearer_token}" }
    return headers

# search_url = "https://api.twitter.com/2/tweets/search/all"

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields

# query_params = {'query': '(from:twitterdev -is:retweet) OR #twitterdev','tweet.fields': 'author_id'}
# query_params = {'query': '"global warming" OR "climate change" OR #globalwarming OR #climatechange', 'start_time': '2016-01-01', 'end_time': '2021-11-01'}

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FullArchiveSearchPython"
    return r


def create_url(keyword, start_time, end_time, max_results = 10):
    
    search_url = "https://api.twitter.com/2/tweets/search/all" #Change to the endpoint you want to collect data from

    #change params based on the endpoint you are using
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


def connect_to_endpoint(url, headers, params, next_token=None):
    params['next_token'] = next_token 
    response = requests.request("GET", url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def write_json(data):
    with open('data/test-7.json', 'w') as outfile:
        json.dump(data, outfile, indent=2,)


def main():
    json_response = connect_to_endpoint(url[0], headers, url[1])

    # Will need to pre-process the response to join the 'data' with the 'includes' section
    # Probably worth parsing into a CSV here as well
    
    # For requests > 500 tweets, use the following
    # next_token = json_response['meta']['next_token']
    
    
    write_json(json_response)

    print(json.dumps(json_response, indent=4, sort_keys=True))


# Recursive function to return tweet collection for a given day
def getTweetCollection(day, max_results_day, max_results_rx):
    total_results = 0
    flag = True
    next_token = None

    start_time, end_time = tweet_time(day)
    url = create_url(keyword, start_time, end_time, max_results_day)

    while flag:
        # Check if max_results_day reached
        if total_results > max_results_day:
            break

        # Otherwise, carry out request
        json_response = connect_to_endpoint(url[0], headers, url[1], next_token)
        result_count = json_response['meta']['result_count']

        if 'next_token' in json_response['meta']:
            # Save the token to use for next call
            next_token = json_response['meta']['next_token']

            if result_count is not None and result_count > 0 and next_token is not None:
                print("Start Date: ", start_list[i])
                append_to_csv(json_response, "data.csv")
                total_results += result_count
                time.sleep(5)
        
        # if no token exists
        else:
            if result_count is not None and result_count > 0:
                append_to_csv(json_response, "data.csv")
                total_results += result_count
                time.sleep(5)

            next_token = None
            flag = False
        


# Create CSV file
csvFile = open("data.csv", "a", newline="", encoding='utf-8')
csvWriter = csv.writer(csvFile)

#Create headers for the data you want to save, in this example, we only want save these columns in our dataset
csvWriter.writerow(['author id', 'created_at', 'geo', 'tweet_id','lang', 'like_count', 'quote_count', 'reply_count','retweet_count','source','tweet', 'username', 'name', 'account_description', 'account_created_at', 'verified', 'followers_count', 'following_count', 'tweet_count', 'listed_count'])
csvFile.close()

# CSV from JSON parser
def append_to_csv(json_response, fileName):

    #A counter variable
    counter = 0

    #Open OR create the target CSV file
    csvFile = open(fileName, "a", newline="", encoding='utf-8')
    csvWriter = csv.writer(csvFile)

    #Loop through each tweet
    for tweet in json_response['data']:        
        
        # We will create a variable for each since some of the keys might not exist for some tweets
        # So we will account for that

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

        # Join 'data' with 'meta' results
        for tweet_meta in json_response['meta']:
            if tweet['author_id'] == tweet_meta['author_id']:
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

    # When done, close the CSV file
    csvFile.close()

    # Print the number of tweets for this iteration
    print("# of Tweets added from this response: ", counter)


# -------------------------------------------------------------------------------
# Inputs for the request · 2132 days » 4500 tweets/day ~ 9 requests / day
# To avoid asking for duplicates, really we want to ask for 4500 tweets / day and use the nextPage token
first_date = date(2016, 1, 1)
last_date = date(2021, 11, 1)
# Add 1 to be inclusive of final day
duration_days = first_date - last_date + 1 

day = first_date + timedelta(days=1)

bearer_token = auth()
headers = create_headers(bearer_token)

keyword = '"global warming" OR "climate change" OR #globalwarming OR #climatechange lang:en'
start_time = "2016-01-01T00:00:00.000Z"
end_time = "2021-11-01T00:00:00.000Z"
max_results_rx = 10
max_results_day = 4500

url = create_url(keyword, start_time, end_time, max_results)



if __name__ == "__main__":
    main()