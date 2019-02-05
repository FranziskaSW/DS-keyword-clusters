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

def extr_headline_main(field):
    return field['main']

def extr_headline_print(field):
    return field['print_headline']


def extr_organization(field):
    try:
        organization = field['organization']
    except (KeyError, TypeError):
        organization = None
    return organization


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

