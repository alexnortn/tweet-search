import requests
import os
import json
# Client secrets
import secrets

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
# bearer_token = os.environ.get("BEARER_TOKEN")
# bearer_token = secrets.bearer_token

def auth():
    return secrets.bearer_token


def create_headers(bearer_token):
    headers = {"Authorization": "Bearer {}".format(bearer_token)}
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
    url[1]['next_token'] = next_token 
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
    write_json(json_response)

    print(json.dumps(json_response, indent=4, sort_keys=True))


#Inputs for the request · 2132 days » 4500 tweets/day ~ 9 requests / day
# To avoid asking for duplicates, really we want to ask for 4500 tweets / day and use the nextPage token
bearer_token = auth()
headers = create_headers(bearer_token)

keyword = '"global warming" OR "climate change" OR #globalwarming OR #climatechange lang:en'
start_time = "2016-01-01T00:00:00.000Z"
end_time = "2021-11-01T00:00:00.000Z"
max_results = 10

url = create_url(keyword, start_time, end_time, max_results)



if __name__ == "__main__":
    main()