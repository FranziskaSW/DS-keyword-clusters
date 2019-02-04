import json
import requests
import pickle
import pandas as pd

search_key = 'eiEHfvIAWEikGUQ2g7hrOeA0rrAMzUyI'
archive_key = 'Jctp3rj1ZdOaLQiMArs79ioGnwvfK1pC'
month = '2018/1'
month_path = '2018_01'

keyword = 'election'
page = 1


# URL
url = 'https://api.nytimes.com/svc/search/v2/articlesearch.json?' + 'q=' + keyword + '&page=' + str(page) + '&api-key=' + search_key
url = 'https://api.nytimes.com/svc/archive/v1/' + month + '.json?api-key=' + archive_key

print('-------------- load', url, ' --------------')
html = requests.get(url)  # load page
a = html.text
api_return = json.loads(a)
response = api_return['response']


with open(month_path + ".pickle", "wb") as f:
    pickle.dump(response, f)

# with open("response_1.pkl", "rb") as f:
#     response = pickle.load(f)

meta = response['meta']
articles = response['docs']

def extract_info(article_old, keys):
    """
    not needed
    :param article_old:
    :param keys:
    :return:
    """
    article_new = dict()
    for key in keys:
        try:
            article_new.update({key: article_old[key]})
        except KeyError:
            article_new.update({key: None})
    return article_new


def extr_author(field):
    try:
        person = field['person'][0]
        try:
            author = person['firstname'] + ' ' + person['middlename'] + ' ' + person['lastname']
        except KeyError:
            author = person['firstname'] + ' ' + person['lastname']
    except (TypeError, IndexError):
        author = None
    return author

def extr_author_fn(field):
    try:
        person = field['person'][0]
        try:
            name = person['firstname']
        except KeyError:
            name = ''
    except (TypeError, IndexError):
        name = ''
    return name

def extr_author_mn(field):
    try:
        person = field['person'][0]
        try:
            name = person['middlename']
        except KeyError:
            name = ''
    except (TypeError, IndexError):
        name = ''
    return name

def extr_author_ln(field):
    try:
        person = field['person'][0]
        try:
            name = person['lastname']
        except KeyError:
            name = ''
    except (TypeError, IndexError):
        name = ''
    return name


def extr_organization(field):
    try:
        organization = field['organization']
    except (KeyError, TypeError):
        organization = None
    return organization


def extr_keywords_step1(field):
    keyword_list = list()
    for keyword in field:
        keyword_tup = (keyword['name'], keyword['value'])
        keyword_list.append(keyword_tup)
    return(keyword_list)

def extr_headline_main(field):
    return field['main']

def extr_headline_print(field):
    return field['print_headline']


def create_keywords_table(keywords_list):
    # TODO: take first line outside and table_keywords as argument of function so that it can be extended
    df_keywords = pd.DataFrame([[0, '*name*', '*value*', 0]], columns=['id', 'name', 'value', 'counts'])

    for k_list in keywords_list:
        if len(k_list) > 0:
            for k_word in k_list:
                id = df_keywords.id[(df_keywords.value == k_word[1]) & (df_keywords.name == k_word[0])]
                if len(id) == 0:
                    next_id = max(df_keywords.id) + 1
                    new_row = pd.DataFrame([[next_id, k_word[0], k_word[1], 1]], columns=['id', 'name', 'value', 'counts'])
                    df_keywords = df_keywords.append(new_row, ignore_index=True)
                elif len(id) == 1:
                    id = id._get_values(0)
                    df_keywords.loc[df_keywords.id == id, 'counts'] += 1
                else:
                    print('something went wrong')  # TODO: delete
    return df_keywords


def create_keywords_table_2(df):

    keywords_list = df.keywords.apply(lambda x: extr_keywords_step1(x))
    sections = df.section  # TODO: or the other feature

    # TODO: take first line outside and table_keywords as argument of function so that it can be extended
    df_keywords = pd.DataFrame([[0, '*name*', '*value*', 0, dict()]], columns=['id', 'name', 'value', 'counts', 'section_count'])

    for k_list, section_id in zip(keywords_list, sections):
        print(k_list, section_id)

        if len(k_list) > 0:
            for k_word in k_list:
                id = df_keywords.id[(df_keywords.value == k_word[1]) & (df_keywords.name == k_word[0])]

                if len(id) == 0:
                    next_id = max(df_keywords.id) + 1
                    section_dict = {section_id: 1}
                    new_row = pd.DataFrame([[next_id, k_word[0], k_word[1], 1, section_dict]], columns=['id', 'name', 'value', 'counts', 'section_count'])
                    df_keywords = df_keywords.append(new_row, ignore_index=True)

                elif len(id) == 1:
                    id = id._get_values(0)
                    section_dict = df_keywords.section_count[df_keywords.id == id]._get_values(0)
                    print(section_dict)
                    try: # is there already a value to this section_id?
                        value = section_dict[section_id]
                    except KeyError: # could not find this section_id
                        value = 0
                    section_dict.update({section_id: value+1})
                    df_keywords.loc[df_keywords.id == id, 'counts'] += 1
                    try: # somehow did not work when the new section_dict was longer than the old one... it overwrites the column but still gives an error message
                        df_keywords.loc[df_keywords.id == id, 'section_count'] = section_dict
                    except ValueError:
                        pass
                else:
                    print('something went wrong')  # TODO: delete

    return df_keywords


def keywords2id(field, table_keywords):
    id_list = []
    try:
        for tup in field:
            id = table_keywords.id[(table_keywords.value == tup[1]) & (table_keywords.name == tup[0])]._get_values(0)
            id_list.append(id)
    except TypeError:
        id_list = None
    return id_list


def section2id(field, table_sections):

    try:
        id = table_sections.id[(table_sections.name == field)]
        id = id._get_values(0)
    except IndexError:
        id = 0

    return id



def create_section_table(df):
    sections = df.section_name

    # TODO: take first line outside and table_keywords as argument of function so that it can be extended
    table = pd.DataFrame([[0, '*name*', 0]], columns=['id', 'name', 'counts'])

    for section in sections:
        try:
            if len(section) > 0:
                id = table.id[table.name == section]

                if len(id) == 0:
                    next_id = max(table.id) + 1
                    new_row = pd.DataFrame([[next_id, section, 1]],
                                           columns=['id', 'name', 'counts'])
                    table = table.append(new_row, ignore_index=True)
                elif len(id) == 1:
                            id = id._get_values(0)
                            table.loc[table.id == id, 'counts'] += 1
                else:
                    print('something went wrong')  # TODO: delete
        except TypeError:
            pass
    return table


df = pd.DataFrame(articles[:100])
df['author_fn'] = df.byline.apply(lambda x: extr_author_fn(x))
# df['author_mn'] = df.byline.apply(lambda x: extr_author_mn(x))
df['author_ln'] = df.byline.apply(lambda x: extr_author_ln(x))
df['author'] = df.author_fn + ' ' + df.author_ln

df['organization'] = df.byline.apply(lambda x: extr_organization(x))
# df['keywords'] = df.keywords.apply(lambda field: extr_keywords(field))
df['headline_main'] = df.headline.apply(lambda field: extr_headline_main(field))
df['headline_print'] = df.headline.apply(lambda field: extr_headline_print(field))
df[:2]

# TODO: too much back and forth here... have only one function, not three

# step 1: translate keywords.json to ('name', 'value') pairs
df.keywords = df.keywords.apply(lambda x: extr_keywords_step1(x))

# step 2: use those pairs to create table of keywords with ids and counts
table_keywords = create_keywords_table(df.keywords)

# step 3: translate ('name', 'value') pairs to ids
df.keywords = df.keywords.apply(lambda x: keywords2id(x, table_keywords))

# TODO: combine with sections? - weightvector
# TODO: or create small matrix first, that ever row just once and then with join over keyword_id and article find weightvector

# with open(month_path + "_df.pickle", "wb") as f:
#     pickle.dump(df, f)
#
# with open("table_keywords.pickle", "wb") as f:
#      pickle.dump(table_kewwords, f)

with open("table_keywords.pickle", "rb") as f:
    table_keywords = pickle.load(f)

### section mapping
table_sections = create_section_table(df)
df['section'] = df.section_name.apply(lambda x: section2id(x, table_sections))


table_keywords['section_count'] = 0

def keyword_section_frequency(df, table_keywords):

    keyword_ids = df.keywords
    sections = df.section
    section_count = dict()

    for i in range(0, keyword_ids.shape[0]):
        keyword_id = keyword_ids[i]
        section = sections[i]
        for k_id in keyword_id:
            section_count = table_keywords.section_count[table_keywords.id == k_id]
            section_count
            table_keywords.section_count[table_keywords.id == k_id] =



######################################################################################
#            KEYWORDS NETWORK GRAPH
######################################################################################

# nodes: keywords
# id, keyword, type-section_name
# the section_name won't be unique, take most frequent?
# only use keywords that appeared >30(?) times
t = table_keywords[table_keywords.counts >= 30]


# edges: appeared together, id1 < id2
# id1, id2, weight-frequency

