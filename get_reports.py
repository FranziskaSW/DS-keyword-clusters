import pandas as pd
import json
import requests
from bs4 import BeautifulSoup

our_key = ''
keyword = 'election'
page = 1

# URL
url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json?' + 'q=' + keyword + '&page=' + str(page) + '&api-key=' + our_key

print('-------------- load', url, ' --------------')
html = requests.get(url)  # load page
a = html.text
api_return = json.loads(a)
response = api_return['response']
meta = response['meta']
articles = response['docs']

article = articles[2]
article['_id']
article['pub_date']
article['headline']
article['byline']
article['keywords']
article['source']
article['document_type']
article['word_count']
article['uri']
article['type_of_material']
article['news_desk']
article['score']
article['snippet']
article['blog']
article['section_name']
article['web_url']
article['multimedia']