import json
import requests
import pickle
import pandas as pd
import numpy as np
from itertools import chain
from itertools import combinations
import os
import math

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
        try:
            id = table_keywords.id[
                (table_keywords.name == keyword['name']) & (table_keywords.value == keyword['value'])]._get_values(0)
            keyword_list.append(id)
        except IndexError:
            pass
    return keyword_list


def extr_newsdesk(field):
    try:
        newsdesk = field.split(' / ')[0]
    except AttributeError:
        newsdesk = '*DUMMY*'
    try:
        newsdesk = newsdesk.split(' | ')[0]
    except AttributeError:
        newsdesk = '*DUMMY2*'
    return newsdesk


def section_max(field):
    try:
        max_val = pd.Series(field).max()
        if max_val >= 0.5*sum(field.values()):
            section = pd.Series(field).idxmax()
        else:
            section = 0
    except ValueError:  # if dict was empty because keyword was never used in an article that was assigned to section
        section = 0
    return section

#
# def section_max(field):
#     try:
#         section = pd.Series(field).idxmax()
#     except ValueError:  # if dict was empty because keyword was never used in an article that was assigned to section
#         section = 0
#     return section

def section2id(field, table_sections):
    try:
        id = table_sections.id[(table_sections.name == field)]
        id = id._get_values(0)
    except IndexError:
        id = 0

    return id


def create_keywords_table_old(df, table):
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
                        try:  # is there already a value to this section_id?
                            value = section_dict[section_id]
                        except KeyError:  # could not find this section_id
                            value = 0
                        section_dict.update({section_id: value + 1})
                        try:  # somehow did not work when the new section_dict was longer than the old one...
                              # it overwrites the column but still gives an error message
                            table.loc[table.id == keyword_id, 'section_count'] = section_dict
                        except ValueError:
                            pass
                else:
                    print('something went wrong')  # TODO: delete
    table['section'] = table.section_count.apply(lambda x: section_max(x))
    return table


def create_keywords_table(df, table):
    keywords_list = df.keywords.apply(lambda x: extr_keywords_step1(x))
    sections = df.section  # TODO: or the other feature

    for k_list, section_id in zip(keywords_list, sections):
        if len(k_list) == 0:  # if this article has no keywords, do nothing
            pass
        else:
            for k_word in k_list:  # for keyword in keyword-list find id of table
                keyword_id = table.id[(table.value == k_word[1]) & (table.name == k_word[0])]

                if len(keyword_id) == 0:  # if id doesnt exist, create new entry
                    next_id = max(table.id) + 1
                    section_dict = {section_id: 1}
                    new_row = pd.DataFrame([[next_id, k_word[0], k_word[1], 1, section_dict]],
                                           columns=['id', 'name', 'value', 'counts', 'section_count'])
                    table = table.append(new_row, ignore_index=True)

                elif len(keyword_id) == 1:  # if id already exists, +1 count and +1 for section
                    # increase count of keyword by one
                    keyword_id = keyword_id._get_values(0)
                    table.loc[table.id == keyword_id, 'counts'] += 1

                    # increase count of section by, but only if it was not section == 0
                    if section_id == 0:
                        pass
                    else:
                        section_dict = table.section_count[table.id == keyword_id]._get_values(0)
                        try:  # is there already a value to this section_id?
                            value = section_dict[section_id]
                        except KeyError:  # could not find this section_id
                            value = 0
                        section_dict.update({section_id: value + 1})
                        try:  # somehow did not work when the new section_dict was longer than the old one...
                              # it overwrites the column but still gives an error message
                            table.loc[table.id == keyword_id, 'section_count'] = section_dict
                        except ValueError:
                            pass

    table['section'] = table.section_count.apply(lambda x: section_max(x))
    return table


def create_keywords_table_partial(df):
    keywords_list = df.keywords.apply(lambda x: extr_keywords_step1(x))
    sections = df.section  # TODO: or the other feature
    table = pd.DataFrame([[0, '*NAME*', '*VALUE*', 0]], columns=['id', 'name', 'value', 'counts'])

    for k_list, section_id in zip(keywords_list, sections):
        if len(k_list) == 0:  # if this article has no keywords, do nothing
            pass
        else:
            for k_word in k_list:  # for keyword in keyword-list find id of table
                keyword_id = table.id[(table.value == k_word[1]) & (table.name == k_word[0])]

                if len(keyword_id) == 0:  # if id doesnt exist, create new entry
                    next_id = max(table.id) + 1
                    section_dict = {section_id: 1}
                    new_row = pd.DataFrame([[next_id, k_word[0], k_word[1], 1, section_dict]],
                                           columns=['id', 'name', 'value', 'counts', 'section_count'])
                    table = table.append(new_row, ignore_index=True)

                elif len(keyword_id) == 1:  # if id already exists, +1 count and +1 for section
                    # increase count of keyword by one
                    keyword_id = keyword_id._get_values(0)
                    table.loc[table.id == keyword_id, 'counts'] += 1

                    # increase count of section by, but only if it was not section == 0
                    if section_id == 0:
                        pass
                    else:
                        section_dict = table.section_count[table.id == keyword_id]._get_values(0)
                        try:  # is there already a value to this section_id?
                            value = section_dict[section_id]
                        except KeyError:  # could not find this section_id
                            value = 0
                        section_dict.update({section_id: value + 1})
                        try:  # somehow did not work when the new section_dict was longer than the old one...
                              # it overwrites the column but still gives an error message
                            table.loc[table.id == keyword_id, 'section_count'] = section_dict
                        except ValueError:
                            pass
    return table


def merge_keyword_tables(table_big, table_small):

    for idx_small in range(1, table_small.shape[0]):
        row_big = table_big[(table_big.value == table_small.loc[idx_small, 'value']) &
                                  (table_big.name == table_small.loc[idx_small, 'name'])]

        if row_big.shape[0] == 0:
            # this idx_small does not exist yet
            new_row = table_small.loc[[idx_small]]
            new_row.id = table_big.id.max() + 1
            table_big = table_big.append(new_row, ignore_index=True)
        else:
            # update row of table_big
            idx_big = row_big.index.get_values()[0]

            section_dict_small = table_small.loc[idx_small, 'section_count']
            section_dict_big = row_big.section_count[idx_big]

            if len(section_dict_small) <= len(section_dict_big):
                dict_s = section_dict_small
                dict_b = section_dict_big
            else:
                dict_s = section_dict_big
                dict_b = section_dict_small

            for section in dict_s.keys():
                value_small = dict_s[section]
                try:
                    value_big = dict_b[section]
                except KeyError:
                    value_big = 0
                dict_b.update({section: value_small + value_big})
            table_big.section_count[idx_big] = dict_b
            table_big.counts[idx_big] += table_small.counts[idx_small]

    return table_big

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



def create_newsdesk_table(df, table):
    newsdesks = df.newsdesk

    for section in newsdesks:
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
    field.sort()
    edges = []
    for subset in combinations(field, 2):
        edge = str(subset[0]) + ',' + str(subset[1])
        edges.append(edge)
    return edges


def edges_nodes(article_keywords, table_keywords):
    edges_list = article_keywords.apply(lambda x: keyword_edges(x)).tolist()
    edges_df = pd.Series(list(chain.from_iterable(edges_list)))
    edges_counts = edges_df.value_counts()

    edges = pd.DataFrame([x.split(',') for x in edges_counts.index], columns=['keyword_1', 'keyword_2'])
    edges['Source'] = edges.keyword_1.apply(lambda x: int(x))
    edges['Target'] = edges.keyword_2.apply(lambda x: int(x))
    edges['Counts'] = edges_counts.reset_index()[0]
    edges['Weight'] = edges.apply(lambda x: edge_weight(x, table_keywords), axis=1)
    e = edges[['Source', 'Target', 'Weight']]

    t = table_keywords[['id', 'section', 'value']]
    ids_1 = e.Source.value_counts().index.get_values().tolist()
    ids_2 = e.Target.value_counts().index.get_values().tolist()
    t_1 = t[t.id.isin(ids_1)]
    t_2 = t[t.id.isin(ids_2)]
    t_3 = t_1.merge(t_2, on=list(t_1), how='outer')
    t_3.columns = ['id', 'Section', 'Label']
    return e, t_3


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


def edge_weight(edges_row, table_keywords):

    # # calculate Davids Weight "mutual weight" not just count
    # f = edges.loc[1]
    # table_keywords[table_keywords.id == f.Source]  # Trump, Donald J - 19382
    # table_keywords[table_keywords.id == f.Target]  # Inaugurations   - 194
    # f.Counts                                       # combi           - 104

    # P(Trump | Inaugurations) = P(Trump, Inaugurations) / P(Inaugurations)
    p1 = (edges_row.Counts / table_keywords[table_keywords.id == edges_row.Target].counts).get_values()[0]  # 0.536082
    # P(Inaugurations | Trump) = P(Trump, Inaugurations) / P(Trump)
    p2 = (edges_row.Counts / table_keywords[table_keywords.id == edges_row.Source].counts).get_values()[0]
    p = (p1 + p2)*100
    return p

def reduce_edges(nodes, edges, percentage, min_edges):
    edges_new = pd.DataFrame(columns=['Source', 'Target', 'Weight'])
    drop_ids = []
    for keyword_id in nodes.id:
        edges_of_id = edges[((edges.Source == keyword_id) | (edges.Target == keyword_id))]
        max_edges = math.ceil(percentage*edges_of_id.shape[0])
        if max_edges >= min_edges:
            edges_20 = edges_of_id.sort_values(by='Weight', ascending=False)[:max_edges]
            edges_new = pd.concat([edges_new, edges_20], ignore_index=True)
            edges_new = edges_new.drop_duplicates(['Source', 'Target'])
        else:
            drop_ids.append(keyword_id)

    mask1 = edges_new.Source.apply(lambda x: x not in drop_ids)
    mask2 = edges_new.Target.apply(lambda x: x not in drop_ids)

    edges_n = edges_new[(mask1 & mask2)]
    nodes_n = nodes[nodes.id.apply(lambda x: x not in drop_ids)]

    return edges_n, nodes_n


def main():

    year = '2018'
    with open(cwd + "/data/table_sections_16-18.pickle", "rb") as f:
        table_sections = pickle.load(f)

    # with open(cwd + "/data/table_keywords_16-18.pickle", "rb") as f:
    #     table_keywords = pickle.load(f)


    # with open(cwd + "/data/archive/" + year + "_01.pickle", "rb") as f:
    #     response = pickle.load(f)
    #     articles = response['docs']
    #     df = pd.DataFrame(articles)
    #
    for m in range(2, 4):
        month = str(m)
        if len(month) == 1:
            month = '0' + month
        suffix = year + "_" + month
        print(suffix)

        with open(cwd + "/data/archive/" + suffix + ".pickle", "rb") as f:
            response = pickle.load(f)
            articles = response['docs']
            df_new = pd.DataFrame(articles)
        df = df_new # = pd.concat([df, df_new], ignore_index=True)
        print(df.shape)

        df = df[~(df.word_count.isnull())]
        df['word_count'] = df.word_count.apply(lambda x: int(x))
        df = df[df.word_count > 20]
        df['section'] = df.section_name.apply(lambda x: extr_newsdesk(x))
        df['section'] = df.section.apply(lambda x: section2id(x, table_sections))

        table_keywords_partial = create_keywords_table_partial(df)
        print(table_keywords_partial.shape)
        with open(cwd + "/data/keywords/table_keywords_" + suffix + ".pickle", "wb") as f:
            pickle.dump(table_keywords_partial, f)

        with open(cwd + "/data/keywords/table_keywords_big_" + year + ".pickle", "rb") as f:
            table_big = pickle.load(f)

        print(table_big.shape, table_keywords_partial.shape)
        table_big = table_big[table_big.counts >= 2]
        table_keywords_partial = table_keywords_partial[table_keywords_partial.counts >= 2]

        table_big = merge_keyword_tables(table_big, table_keywords_partial)

        with open(cwd + "/data/keywords/table_keywords_big_" + year + ".pickle", "wb") as f:
            pickle.dump(table_big, f)

        # 17:42 - 17:59 - table_big is keywords 2018-01 - 2018-03

    table_keywords = table_big[1:]
    table_keywords['section'] = table_keywords.section_count.apply(lambda x: section_max(x))

    # t = table_keywords[table_keywords.counts >= 10]
    # t['section'] =  t.section_count.apply(lambda x: section_max(x)) # hat only assignes when more than 50%
    # table_keywords = t

    # with open(cwd + "/data/table_keywords_16-18_forgraph.pickle", "wb") as f:
    #     pickle.dump(table_keywords, f)

    ###################### continue another time ####################################
    # to analyse if section or news desk is the important label
    # t = df[['section', 'newsdesk']]
    # t['c'] = 1
    # t.groupby(by=['section', 'newsdesk']).count()
    # t.groupby(by=['newsdesk', 'section']).count()
    #
    # t.section.value_counts().shape
    # t.newsdesk.value_counts().shape
    #################################################################################

    with open(cwd + "/data/archive/" + year + "_01.pickle", "rb") as f:
        response = pickle.load(f)
        articles = response['docs']
        df = pd.DataFrame(articles)


    for m in range(2, 4):
        month = str(m)
        if len(month) == 1:
            month = '0' + month
        suffix = year + "_" + month
        print(suffix)

        with open(cwd + "/data/archive/" + suffix + ".pickle", "rb") as f:
            response = pickle.load(f)
            articles = response['docs']
            df_new = pd.DataFrame(articles)
        df = pd.concat([df, df_new], ignore_index=True)
        print(df.shape)

    df = df[~(df.word_count.isnull())]
    df['word_count'] = df.word_count.apply(lambda x: int(x))
    df = df[df.word_count > 20]
    df['section'] = df.section_name.apply(lambda x: extr_newsdesk(x))
    df['section'] = df.section.apply(lambda x: section2id(x, table_sections))

    df.keywords = df.keywords.apply(lambda x: extr_keywords(x, table_keywords))

    keywords = df.keywords

    # keywords_series = df.keywords
    #
    # # keywords_18 =
    # # keywords_17 = keywords_series
    # keywords_16 = keywords_series
    #
    # # combine keywords of the years
    # keywords = pd.concat([keywords_16, keywords_17, keywords_18], ignore_index=True)

    edges, nodes = edges_nodes(keywords, table_keywords)

    # TODO: overthink method
    #       especially the 20% thing. maybe it deletes nodes that were prior connected to something else already.
    #       draw sketch like who is in network with whom. if b is in 20% of b, but has only less than 1 connection
    #       -- want to delete b, has to be replaced with something else?
    #       especially not allowed to stay in edge, but be deleted in nodes
    # remove keywords/nodes that are tagged to a section that appears less than 10(?) times
    sec = nodes.Section.value_counts()
    sec = sec[sec >= 50].index.tolist()
    len(sec)
    mask = nodes.Section.apply(lambda x: x in sec)
    nodes_freq = nodes[mask]

    # reduce_edges
    # only keep 20% per node
    # remove nodes that would have less than 3 edges

    # now only the nodes that have section specified
    # nodes_wo = nodes[~(nodes.section == 0)]

    edges_20, nodes_20 = reduce_edges(nodes_freq, edges, 0.1, 5)

    s = nodes_20.Section.value_counts()
    s = s.reset_index()

    # TODO: why do I have so many nodes in edges, that dont appear in nodes?

    edges_20.shape
    nodes_20.shape
    nodes_20.to_csv(cwd + '/data/gephi/03_nodes_10-5.csv', sep=';', index=False)
    edges_20.to_csv(cwd + '/data/gephi/03_edges_10-5.csv', sep=';', index=False)

    with open(cwd + "/data/gephi/03_edges_16-18.pickle", "wb") as f:
        pickle.dump(edges_20, f)
    with open(cwd + "/data/gephi/03_nodes_16-18.pickle", "wb") as f:
        pickle.dump(nodes_20, f)
