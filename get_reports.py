import pandas as pd
import json
import requests
import pickle

our_key = 'eiEHfvIAWEikGUQ2g7hrOeA0rrAMzUyI'
keyword = 'election'
page = 1

# URL
url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json?' + 'q=' + keyword + '&page=' + str(page) + '&api-key=' + our_key

print('-------------- load', url, ' --------------')
html = requests.get(url)  # load page
a = html.text
api_return = json.loads(a)
response = api_return['response']

#
# with open("response_1.pickle", "wb") as f:
#     pickle.dump(response, f)
#
# with open("response_1.pkl", "rb") as f:
#     response = pickle.load(f)

meta = response['meta']
articles = response['docs']

article = articles[2]

with open('result.json', 'w') as fp:
    json.dump(article, fp)

# article['_id']
# article['pub_date']
# article['headline']
# article['byline']
# article['keywords']
# article['source']
# article['document_type']
# article['word_count']
# article['uri']
# article['type_of_material']
# article['news_desk']
# article['score']
# article['snippet']
# article['blog']
# article['section_name']
# article['web_url']
# article['multimedia']

def extract_info(article_old, keys):
    article_new = dict()
    for key in keys:
        try:
            article_new.update({key: article_old[key]})
        except KeyError:
            article_new.update({key: None})
    return article_new

article = extract_info(article, ['_id', 'pub_date', 'headline', 'byline', 'section_name', 'source', 'keywords'])

def extr_byline(article):
    person = article['byline']['person'][0]
    try:
        author = person['firstname'] + ' ' + person['middlename'] + ' ' + person['lastname']
    except TypeError:
        author = person['firstname'] + ' ' + person['lastname']
    organization = article['byline']['organization']
    article.update({'author' : author})
    article.update({'organization' : organization})
    del article['byline']
    return article

def extr_keywords(article):

    keyword_list = list()
    for keyword in article['keywords']:
        keyword_tup = (keyword['name'], keyword['value'])
        keyword_list.append(keyword_tup)
    return(keyword_list)



article = extr_byline(article)
