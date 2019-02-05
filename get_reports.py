import json
import requests
import pickle
import pandas as pd
import numpy as np
from itertools import chain
from itertools import combinations
import os

global cwd
cwd = os.getcwd()

def extr_keywords_step1(field):
    keyword_list = list()
    for keyword in field:
        keyword_tup = (keyword['name'], keyword['value'])
        keyword_list.append(keyword_tup)
    return keyword_list


def extr_keywords(field, table_keywords):
    keyword_list = list()
    for keyword in field:
        id = table_keywords.id[
            (table_keywords.name == keyword['name']) & (table_keywords.value == keyword['value'])]._get_values(0)
        keyword_list.append(id)
    return keyword_list


def section_max(field):
    try:
        section = pd.Series(field).idxmax()
    except ValueError: # if dict was empty because keyword was never used in an article that was assigned to section
        section = 0
    return section


def section2id(field, table_sections):

    try:
        id = table_sections.id[(table_sections.name == field)]
        id = id._get_values(0)
    except IndexError:
        id = 0

    return id


def create_keywords_table(df, table):
    
    keywords_list = df.keywords.apply(lambda x: extr_keywords_step1(x))
    sections = df.section  # TODO: or the other feature
    table = table[table.counts >= 3]

    for k_list, section_id in zip(keywords_list, sections):
        if len(k_list) > 0:
            for k_word in k_list:
                keyword_id = table.id[(table.value == k_word[1]) & (table.name == k_word[0])]

                if len(keyword_id) == 0:
                    next_id = max(table.id) + 1
                    section_dict = {section_id: 1}
                    new_row = pd.DataFrame([[next_id, k_word[0], k_word[1], 1, section_dict]],
                                           columns=['id', 'name', 'value', 'counts', 'section_count'])
                    table = table.append(new_row, ignore_index=True)

                elif len(keyword_id) == 1:
                    # increase count of keyword by one
                    keyword_id = keyword_id._get_values(0)
                    table.loc[table.id == keyword_id, 'counts'] += 1

                    # increase count of section by, but only if it was not section == 0
                    if section_id == 0:
                        pass
                    else:
                        section_dict = table.section_count[table.id == keyword_id]._get_values(0)
                        try: # is there already a value to this section_id?
                            value = section_dict[section_id]
                        except KeyError: # could not find this section_id
                            value = 0
                        section_dict.update({section_id: value+1})
                        try: # somehow did not work when the new section_dict was longer than the old one... it overwrites the column but still gives an error message
                            table.loc[table.id == keyword_id, 'section_count'] = section_dict
                        except ValueError:
                            pass
                else:
                    print('something went wrong')  # TODO: delete
    table['section'] = table.section_count.apply(lambda x: section_max(x))
    return table


def create_section_table(df, table):
    sections = df.section_name

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


######################################################################################
#            KEYWORDS NETWORK GRAPH
######################################################################################

def keyword_edges(field):

    edges = []
    for subset in combinations(field, 2):
        edge = str(subset[0]) + ',' + str(subset[1])
        edges.append(edge)
    return edges


def edges_nodes(article_keywords, table_keywords, min_weight):

    edges_list = article_keywords.apply(lambda x: keyword_edges(x)).tolist()
    edges_df   = pd.Series(list(chain.from_iterable(edges_list)))
    edges_weights = edges_df.value_counts()

    edges = pd.DataFrame([x.split(',') for x in edges_weights.index], columns=['keyword_1', 'keyword_2'])
    edges['Source'] = edges.keyword_1.apply(lambda x: int(x))
    edges['Target'] = edges.keyword_2.apply(lambda x: int(x))
    edges['Weight'] = edges_weights.reset_index()[0]

    e = edges[edges.Weight >= min_weight][['Source', 'Target', 'Weight']]

    t = table_keywords[['id', 'section', 'value']]
    ids_1 = e.Source.value_counts().index.get_values().tolist()
    ids_2 = e.Target.value_counts().index.get_values().tolist()
    t_1 = t[t.id.isin(ids_1)]
    t_2 = t[t.id.isin(ids_2)]
    t_3 = t_1.merge(t_2, on=list(t_1), how='outer')
    return e, t_3


def get_data(year, month):
    archive_key = 'Jctp3rj1ZdOaLQiMArs79ioGnwvfK1pC'
    month_api = year + '/' + month
    if len(month) == 1:
        month = '0' + month
    data_suffix = year + '_' + month
    url = 'https://api.nytimes.com/svc/archive/v1/' + month_api + '.json?api-key=' + archive_key

    print('-------------- load', url, ' --------------')
    html = requests.get(url)  # load page
    a = html.text
    api_return = json.loads(a)
    response = api_return['response']

    with open(cwd + "/data/archive/" + data_suffix + ".pickle", "wb") as f:
        pickle.dump(response, f)

    articles = response['docs']
    df = pd.DataFrame(articles)

    return df, data_suffix


def update_tables(df):

    with open(cwd + "/data/table_sections.pickle", "rb") as f:
        table_sections = pickle.load(f)
    table_sections = create_section_table(df, table_sections)
    with open(cwd + "/data/table_sections.pickle", "wb") as f:
          pickle.dump(table_sections, f)
    df['section'] = df.section_name.apply(lambda x: section2id(x, table_sections))

    with open(cwd + "/data/table_keywords.pickle", "rb") as f:
        table_keywords = pickle.load(f)

    # table_keywords = pd.DataFrame([[0, '*name*', '*value*', 0]], columns=['id', 'name', 'value', 'counts'])

    table_keywords = create_keywords_table(df, table_keywords)
    with open(cwd + "/data/table_keywords.pickle", "wb") as f:
          pickle.dump(table_keywords, f)
    return table_keywords, table_sections


def main():
    year = '2018'
    for m in range(1,13):
        month = str(m)
        
        month = '3'
        df, data_suffix = get_data(year, month)
        table_keywords, table_sections = update_tables(df)

        df.keywords = df.keywords.apply(lambda x: extr_keywords(x, table_keywords))
    
        edges, nodes = edges_nodes(df.keywords, table_keywords, 3)
    
        nodes.to_csv(cwd + '/data/gephi/nodes_' + data_suffix + '.csv', sep=';', index=False)
        edges.to_csv(cwd + '/data/gephi/edges_' + data_suffix + '.csv', sep=';', index=False)


        # with open(cwd + "/data/gephi/nodes_" + data_suffix + ".pickle", "wb") as f:
        #     pickle.dump(nodes, f)
        #
        # with open(cwd + "/data/gephi/nodes_combined.pickle", "wb") as f:
        #     pickle.dump(nodes, f)
        #
        # with open(cwd + "/data/gephi/edges_" + data_suffix + ".pickle", "wb") as f:
        #     pickle.dump(edges, f)
        #
        # with open(cwd + "/data/gephi/edges_combined.pickle", "wb") as f:
        #     pickle.dump(edges, f)

        with open(cwd + "/data/gephi/nodes_" + data_suffix + ".pickle", "wb") as f:
            pickle.dump(nodes, f)
        with open(cwd + "/data/gephi/nodes_combined.pickle", "rb") as f:
            nodes_combined = pickle.load(f)
        nodes_combined = pd.merge(nodes_combined, nodes, on=['id', 'value'], how='outer')
        with open(cwd + "/data/gephi/nodes_combined.pickle", "wb") as f:
            pickle.dump(nodes_combined, f)
        nodes_combined.to_csv(cwd + '/data/gephi/nodes_combined.csv', sep=';', index=False)

        with open(cwd + "/data/gephi/edges_" + data_suffix + ".pickle", "wb") as f:
            pickle.dump(edges, f)
        with open(cwd + "/data/gephi/edges_combined.pickle", "rb") as f:
            edges_combined = pickle.load(f)
        edges_new = pd.merge(edges_combined, edges, on=['Source', 'Target'], how='outer').fillna(0)
        edges_new['Weight'] = edges_new.Weight_x + edges_new.Weight_y
        edges_combined = edges_new[['Source', 'Target', 'Weight']]
        with open(cwd + "/data/gephi/edges_combined.pickle", "wb") as f:
            pickle.dump(edges_combined, f)
        edges_combined.to_csv(cwd + '/data/gephi/edges_combined.csv', sep=';', index=False)

# table_keywords.shape 6337  , 7341  ,
# edges_combined.shape 12072 , 21020 ,
# nodes_cbomined.shape 2349  , 3659  ,