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
        del field[0] # because uncategorized does not add more information. only choose among categorized tags
    except KeyError:
        pass

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

#
# def create_keywords_table_old(df, table):
#     keywords_list = df.keywords.apply(lambda x: extr_keywords_step1(x))
#     sections = df.section  # TODO: or the other feature
#     table = table[table.counts >= 3]
#
#     for k_list, section_id in zip(keywords_list, sections):
#         if len(k_list) > 0:
#             for k_word in k_list:
#                 keyword_id = table.id[(table.value == k_word[1]) & (table.name == k_word[0])]
#
#                 if len(keyword_id) == 0:
#                     next_id = max(table.id) + 1
#                     section_dict = {section_id: 1}
#                     new_row = pd.DataFrame([[next_id, k_word[0], k_word[1], 1, section_dict]],
#                                            columns=['id', 'name', 'value', 'counts', 'section_count'])
#                     table = table.append(new_row, ignore_index=True)
#
#                 elif len(keyword_id) == 1:
#                     # increase count of keyword by one
#                     keyword_id = keyword_id._get_values(0)
#                     table.loc[table.id == keyword_id, 'counts'] += 1
#
#                     # increase count of section by, but only if it was not section == 0
#                     if section_id == 0:
#                         pass
#                     else:
#                         section_dict = table.section_count[table.id == keyword_id]._get_values(0)
#                         try:  # is there already a value to this section_id?
#                             value = section_dict[section_id]
#                         except KeyError:  # could not find this section_id
#                             value = 0
#                         section_dict.update({section_id: value + 1})
#                         try:  # somehow did not work when the new section_dict was longer than the old one...
#                               # it overwrites the column but still gives an error message
#                             table.loc[table.id == keyword_id, 'section_count'] = section_dict
#                         except ValueError:
#                             pass
#                 else:
#                     print('something went wrong')  # TODO: delete
#     table['section'] = table.section_count.apply(lambda x: section_max(x))
#     return table
#
# #
# def create_keywords_table(df, table):
#     keywords_list = df.keywords.apply(lambda x: extr_keywords_step1(x))
#     sections = df.section  # TODO: or the other feature
#
#     for k_list, section_id in zip(keywords_list, sections):
#         if len(k_list) == 0:  # if this article has no keywords, do nothing
#             pass
#         else:
#             for k_word in k_list:  # for keyword in keyword-list find id of table
#                 keyword_id = table.id[(table.value == k_word[1]) & (table.name == k_word[0])]
#
#                 if len(keyword_id) == 0:  # if id doesnt exist, create new entry
#                     next_id = max(table.id) + 1
#                     section_dict = {section_id: 1}
#                     new_row = pd.DataFrame([[next_id, k_word[0], k_word[1], 1, section_dict]],
#                                            columns=['id', 'name', 'value', 'counts', 'section_count'])
#                     table = table.append(new_row, ignore_index=True)
#
#                 elif len(keyword_id) == 1:  # if id already exists, +1 count and +1 for section
#                     # increase count of keyword by one
#                     keyword_id = keyword_id._get_values(0)
#                     table.loc[table.id == keyword_id, 'counts'] += 1
#
#                     # increase count of section by, but only if it was not section == 0
#                     if section_id == 0:
#                         pass
#                     else:
#                         section_dict = table.section_count[table.id == keyword_id]._get_values(0)
#                         try:  # is there already a value to this section_id?
#                             value = section_dict[section_id]
#                         except KeyError:  # could not find this section_id
#                             value = 0
#                         section_dict.update({section_id: value + 1})
#                         try:  # somehow did not work when the new section_dict was longer than the old one...
#                               # it overwrites the column but still gives an error message
#                             table.loc[table.id == keyword_id, 'section_count'] = section_dict
#                         except ValueError:
#                             pass
#
#     table['section'] = table.section_count.apply(lambda x: section_max(x))
#     return table


def create_keywords_table_partial(df):
    keywords_list = df.keywords.apply(lambda x: extr_keywords_step1(x))
    sections = df.section  # TODO: or the other feature
    table = pd.DataFrame([[0, '*NAME*', '*VALUE*', 0]], columns=['id', 'name', 'value', 'counts'])

    for k_list, section_id, idx in zip(keywords_list, sections, range(0, sections.shape[0])):
        if idx%100 == 0:
            print(idx)

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
                    section_dict = table.section_count[table.id == keyword_id]._get_values(0)

                    try:  # is there already a value to this section_id?
                        value = section_dict[section_id]
                    except KeyError:  # could not find this section_id
                        value = 0

                    section_dict.update({section_id: value + 1})
                    table.section_count[id] = section_dict
    return table

#
# v = table.section_count.apply(lambda x: sum(x.values()))
#
# (v <= table.counts).value_counts
# table.section_count.apply(lambda x: x.drop_key)
# table_keywords[~(v == table_keywords.counts)][['counts', 'section_count']] # only some have section 0
# table_keywords[(v == table_keywords.counts)][['counts', 'section_count']] # all of them have section 0
# # TODO: table_keywords count and section_count does not fit! why??? because of if section == 0 count +1 but not section count +1
#



def merge_keyword_tables(table_big, table_small):

    for idx_small in table_small.index:  # we're only interested in getting through all the columns, it doesnt matter
        # to us which id the keyword had in the small table
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


def edge_weight(edges_row, table_keywords):
    # # calculate Davids Weight "mutual weight" not just count
    # f = edges.loc[1]
    # table_keywords[table_keywords.id == f.Source]  # Trump, Donald J - 19382
    # table_keywords[table_keywords.id == f.Target]  # Inaugurations   - 194
    # f.Counts                                       # combi           - 104

    # TODO: table_keywords.count is wrong! I don't want how often the keyword appears (in 2 articles), but how often it appears
    #       in a tuple of keywords (4, if both of the two articles had 3 keywords)

    # P(Trump | Inaugurations) = P(Trump, Inaugurations) / P(Inaugurations)
    p1 = (edges_row.prob - table_keywords[table_keywords.id == edges_row.Target].prob).get_values()[0]  # 0.536082
    # P(Inaugurations | Trump) = P(Trump, Inaugurations) / P(Trump)
    p2 = (edges_row.prob - table_keywords[table_keywords.id == edges_row.Source].prob).get_values()[0]
    p1, p2 = np.exp(p1), np.exp(p2)
    p = (p1 + p2)*100
    return p
#
# # how often does 24660 appear in articles?
# table_keywords[table_keywords.id == 49]  # 9
#
# #how often does 24660 appear in edges?
# n[n.id == 49].counts # 28
#
# table_keywords_bu = table_keywords
# table_keywords = n

def edges_nodes(article_keywords, table_keywords, no_articles):
    edges_list = article_keywords.apply(lambda x: keyword_edges(x)).tolist()  # each article has a list of keywords
    edges_df = pd.Series(list(chain.from_iterable(edges_list)))  # write everything in one list
    edges_counts = edges_df.value_counts()

    edges = pd.DataFrame([x.split(',') for x in edges_counts.index], columns=['keyword_1', 'keyword_2'])
    edges['Source'] = edges.keyword_1.apply(lambda x: int(x))
    edges['Target'] = edges.keyword_2.apply(lambda x: int(x))
    edges['Counts'] = edges_counts.reset_index()[0]

    e = edges[['Source', 'Target', 'Counts']]
    e['prob'] = np.log(e.Counts/no_articles)

    e['Weight'] = e.apply(lambda x: edge_weight(x, table_keywords), axis=1)

    t = table_keywords[['id', 'section', 'value']]
    ids_1 = e.Source.value_counts().index.get_values().tolist()  # unique ids in Source
    ids_2 = e.Target.value_counts().index.get_values().tolist()  # unique ids in Target
    mask = [any(y) for y in zip(t.id.isin(ids_1), t.id.isin(ids_2))]  # if id was either in Source or in Target or both
    n = t[mask]
    n.columns = ['id', 'Section', 'Label']

    return e, n

#
# def update_tables(df):
#     with open(cwd + "/data/table_sections.pickle", "rb") as f:
#         table_sections = pickle.load(f)
#     table_sections = create_section_table(df, table_sections)
#     with open(cwd + "/data/table_sections.pickle", "wb") as f:
#         pickle.dump(table_sections, f)
#     df['section'] = df.section_name.apply(lambda x: section2id(x, table_sections))
#
#     with open(cwd + "/data/table_keywords.pickle", "rb") as f:
#         table_keywords = pickle.load(f)
#
#     # table_keywords = pd.DataFrame([[0, '*name*', '*value*', 0]], columns=['id', 'name', 'value', 'counts'])
#
#     table_keywords = create_keywords_table(df, table_keywords)
#     with open(cwd + "/data/table_keywords.pickle", "wb") as f:
#         pickle.dump(table_keywords, f)
#     return table_keywords, table_sections



def reduce_edges(nodes, edges, percentage, min_edges):

    s = edges.Source.value_counts()
    t = edges.Target.value_counts()

    st = pd.merge(pd.DataFrame(s), pd.DataFrame(t), left_index=True, right_index=True, how='outer').fillna(0)
    st['counts'] = st.Source + st.Target

    # drop nodes that don't have enough edges
    mask = (st.counts > min_edges / percentage)
    idx = st[mask].index.get_values().tolist()
    drop_idx = st[~mask].index.get_values().tolist()
    nodes_reduced = nodes[nodes.id.isin(idx)]

    # drop edges where we had one of those nodes
    mask = [all(tup) for tup in zip(edges.Source.isin(idx), edges.Target.isin(idx))]

    e = edges[mask]

    edges_reduced = pd.DataFrame(columns=['Source', 'Target', 'Weight'])
    for keyword_id in nodes_reduced.id:
        edges_of_id = e[((e.Source == keyword_id) | (e.Target == keyword_id))]
        max_edges = math.ceil(percentage*edges_of_id.shape[0])
        edges_new = edges_of_id.sort_values(by='Weight', ascending=False)[:max_edges]
        edges_reduced = pd.concat([edges_reduced, edges_new], ignore_index=True)
        edges_reduced = edges_reduced.drop_duplicates(['Source', 'Target'])

    return edges_reduced, nodes_reduced


def reduce_edges_2(nodes, edges, percentage, min_edges):

    s = edges.Source.value_counts()
    t = edges.Target.value_counts()

    st = pd.merge(pd.DataFrame(s), pd.DataFrame(t), left_index=True, right_index=True, how='outer').fillna(0)
    st['counts'] = st.Source + st.Target

    # drop nodes that don't have enough edges
    mask = (st.counts > min_edges / percentage)
    idx = st[mask].index.get_values().tolist()
    nodes_reduced = nodes[nodes.id.isin(idx)]

    # drop edges where we had one of those nodes
    mask = [all(tup) for tup in zip(edges.Source.isin(idx), edges.Target.isin(idx))]

    e = edges[mask]

    edges_reduced = pd.DataFrame(columns=['Source', 'Target', 'Weight'])
    for keyword_id in nodes_reduced.id:
        edges_of_id = e[((e.Source == keyword_id) | (e.Target == keyword_id))]
        max_edges = math.ceil(percentage*edges_of_id.shape[0])
        edges_new = edges_of_id.sort_values(by='Counts', ascending=False)[:max_edges]
        edges_reduced = pd.concat([edges_reduced, edges_new], ignore_index=True)
        edges_reduced = edges_reduced.drop_duplicates(['Source', 'Target'])

    return edges_reduced, nodes_reduced


def main():

    year = '2017'
    with open(cwd + "/data/table_sections_16-18.pickle", "rb") as f:
        table_sections = pickle.load(f)
    with open(cwd + "/data/keywords/table_keywords_big_2018.pickle", "rb") as f:
        table_keywords = pickle.load(f)
    with open(cwd + "/data/df_2018.pickle", "rb") as f:
        df = pickle.load(f)

    # table_keywords = table_big[table_big.counts >= 2]
    # table_keywords = table_keywords[1:]
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

    # use df_month to create keywords_table
    # need to do first step manually. create table_keywords_year_01 and rename
    # to table_big_year
    # then can run algorithm from 02 onwards

    # with open(cwd + "/data/archive/" + year + "_01.pickle", "rb") as f:
    #     response = pickle.load(f)
    #     articles = response['docs']
    #     df = pd.DataFrame(articles)

    with open(cwd + "/data/keywords/table_keywords_big_" + year + ".pickle", "wb") as f:
        pickle.dump(table_keywords_partial, f)

    for m in range(2, 13):
        month = str(m)
        if len(month) == 1:
            month = '0' + month
        suffix = year + "_" + month
        print(suffix)

        with open(cwd + "/data/archive/" + suffix + ".pickle", "rb") as f:
            response = pickle.load(f)
            articles = response['docs']
            df_new = pd.DataFrame(articles)
        df = df_new  # = pd.concat([df, df_new], ignore_index=True)
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
        print(table_big.shape, table_keywords_partial.shape)

        table_big = merge_keyword_tables(table_big, table_keywords_partial)

        with open(cwd + "/data/keywords/table_keywords_big_" + year + ".pickle", "wb") as f:
            pickle.dump(table_big, f)

    # concat dfs to df_year and then clean and translate keywords
    with open(cwd + "/data/archive/" + year + "_01.pickle", "rb") as f:
        response = pickle.load(f)
        articles = response['docs']
        df = pd.DataFrame(articles)

    table_keywords = table_big
    table_keywords['section'] = table_keywords.section_count.apply(lambda x: section_max(x))

    for m in range(2, 13):
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

    df = df[~(df.word_count.isnull())]
    df['word_count'] = df.word_count.apply(lambda x: int(x))
    df = df[df.word_count > 20]
    df['section'] = df.section_name.apply(lambda x: extr_newsdesk(x))
    df['section'] = df.section.apply(lambda x: section2id(x, table_sections))

    # hier weiter:
    # load df
    # load table_keywords_big

    df.keywords = df.keywords.apply(lambda x: extr_keywords(x, table_keywords))

    no_articles = df.shape[0]  # TODO: global
    table_keywords['prob'] = np.log(table_keywords.counts / no_articles)

    keywords = df.keywords


    # with open(cwd + "/data/edges_2018.pickle", "rb") as f:
    #     edges = pickle.load(f)
    # with open(cwd + "/data/nodes_2018.pickle", "rb") as f:
    #     nodes = pickle.load(f)

    edges, nodes = edges_nodes(keywords, table_keywords, no_articles)
    edges.shape

    with open(cwd + "/data/edges_" + year + ".pickle", "wb") as f:
        pickle.dump(edges, f)
    with open(cwd + "/data/nodes_" + year + ".pickle", "wb") as f:
        pickle.dump(nodes, f)
    # with open(cwd + "/data/df_" + year + ".pickle", "wb") as f:
    #     pickle.dump(df, f)


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
    nodes_wo = nodes[~(nodes.Section == 0)]
    mask = [all(tup) for tup in zip(edges.Source.isin(nodes_wo.id), edges.Target.isin(nodes_wo.id))]
    edges_wo = edges[mask]

    edges_reduced, nodes_reduced = reduce_edges_2(nodes_wo, edges_wo, 0.2, 2)
    print(edges_reduced.shape, nodes_reduced.shape)

    s = edges_reduced.Source.value_counts()
    t = edges_reduced.Target.value_counts()

    st = pd.merge(pd.DataFrame(s), pd.DataFrame(t), left_index=True, right_index=True, how='outer').fillna(0)
    st['counts'] = st.Source + st.Target

    pd.Series(st.index.isin(nodes_reduced.index)).value_counts()



    edges_reduced.Source.value_counts()
    edges_reduced.Target.value_counts()


    name = '2018_20-2_wo'
    nodes_reduced.to_csv(cwd + '/data/gephi/05_nodes_' + name + '.csv', sep=';', index=False)
    edges_reduced.to_csv(cwd + '/data/gephi/05_edges_' + name + '.csv', sep=';', index=False)

    with open(cwd + "/data/gephi/05_edges_" + name + ".pickle", "wb") as f:
        pickle.dump(edges_reduced, f)
    with open(cwd + "/data/gephi/05_nodes_" + name + ".pickle", "wb") as f:
        pickle.dump(nodes_reduced, f)

edges_reduced.shape
nodes_reduced.shape