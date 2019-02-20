import pickle
import pandas as pd
import os

global cwd
cwd = os.getcwd()


def clean_section(name):
    world = ['World', 'Africa', 'Americas', 'Asia', 'Asia Pacific', 'Australia', 'Canada', 'Europe', 'Middle East',
             'What in the World', 'Opinion | The World', 'Foreign']
    if name in world: return 'World'
    us = ['U.S.', 'National']
    if name in us: return 'U.S.'
    politics = ['Elections', 'Politics', 'Tracking Trumps Agenda', 'The Upshot', 'Opinion | Politics', 'Upshot',
                'Washington ']
    if name in politics: return 'Politics'
    ny = ['N.Y. / Region', 'New York Today', 'Metro', 'Metropolitan']
    if name in ny: return 'New York'
    business_technology = ['Business Day', 'Economy', 'Media', 'Money', 'DealBook', 'Markets', 'Energy', 'Media',
                           'Technology', 'Personal Tech', 'Entrepreneurship', 'Your Money', 'Business', 'SundayBusiness']
    if name in business_technology: return 'Business & Technology'
    sports = ['Skiing', 'Rugby', 'Sailing', 'Cycling', 'Cricket', 'Auto Racing', 'Horse Racing', 'World Cup',
              'Olympics', 'Pro Football', 'Pro Basketball', 'Sports', 'Baseball', 'NFL', 'College Football', 'NBA',
              'College Basketball', 'Hockey', 'Soccer', 'Golf', 'Tennis']
    if name in sports: return 'Sports'
    arts = ['Opinion | Culture', 'Arts', 'Art & Design', 'Books', 'Dance', 'Movies', 'Music', 'Television', 'Theater',
            'Pop Culture', 'Watching', 'Culture', 'Arts&Leisure']
    if name in arts: return 'Arts'
    books = ['Book Review', 'BookReview', 'Best Sellers', 'By the Book', 'Crime', 'Children\'s Books', 'Book Review Podcast',
             'Now read this']
    if name in books: return 'Books'
    style = ['Men\'s Style', 'Style', 'Styles', 'TStyle', 'Fashion & Style', 'Fashion', 'Weddings', 'Self-Care']  # Weddings wo nd Society
    if name in style: return 'Style'
    science = ['Energy & Environment', 'Science', 'Climate', 'Opinion | Environment', 'Space & Cosmos', 'Trilobites',
               'Sciencetake', 'Out There']
    health = ['Mind', 'Health Guide', 'Health', 'Health Policy', 'Live', 'Global Health', 'The New Old Age', 'Science',
              'Well', 'Move']
    sci_hel = science + health + ['Family', 'Live']
    if name in sci_hel: return 'Health & Science'
    food = ['Eat', 'Wine, Beer & Cocktails', 'Restaurant Reviews', 'Dining', 'Food']
    travel = ['36 Hours', 'Frugal Traveler', '52 Places to go', 'Travel']
    magazine = ['Smarter Living', 'Wirecutter', 'Automobiles', 'T Magazine', 'Magazine', 'Design & Interiors', 'Food',
                'Travel', 'Fashion & Beauty', 'Entertainment', 'Video', 'Weekend']
    leisure = food + travel + magazine
    if name in leisure: return 'Leisure'
    opinion = ['Opinion', 'Letters', 'Contributors', 'Editorials', 'Columnists', 'OpEd', 'Sunday Review', 'Games',
               'Editorial']
    realestate = ['Real Estate', 'RealEstate', 'Commercial Real Estate', 'The High End', 'Commercial', 'IPhone App', 'Find a Home',
                  'Mortgage Calculator', 'Your Real Estate', 'List a Home']
    education = ['Education', 'Education Life', 'The Learning Network', 'Lesson Plans', 'Learning']
    delete = (['Blogs', 'Insider Events', 'Retirement', 'América', 'Multimedia/Photos', 'The Daily',
               'Briefing', 'Sunday Review', 'Crosswords & Games', 'Times Insider', 'Corrections', 'NYTNow',
               'Corrections', 'Podcasts', 'Insider', 'Obits', 'Summary']
              + opinion + education + realestate)
    if name in delete:
        return '*DELETE*'
    else: return '*UNKNOWN*'


def extr_headline_main(field):
    return field['main']


def clean_articles(df, word_count):
    df = df[~(df.word_count.isnull())]
    df['word_count'] = df.word_count.apply(lambda x: int(x))
    df = df[df.word_count > word_count]
    df['headline'] = df.headline.apply(lambda x: extr_headline_main(x))
    df = df.drop_duplicates(['headline', 'section_name'])
    df['section'] = df.section_name.apply(lambda x: clean_section(x))
    #TODO: add medium
    return df

def rename_sections(df):
    df['section'] = df.section_name.apply(lambda x: clean_section(x))
    without_section = df[df.section == '*UNKNOWN*']
    sections_from_newsdesk = without_section.news_desk.apply(lambda x: clean_section(x))
    idx = sections_from_newsdesk.index.get_values()
    df.loc[idx, 'section'] = sections_from_newsdesk
    return df

################################################################################# keyword stuff

def extr_keywords_step1(field):
    keyword = field
    keyword_tup = (keyword['name'], keyword['value'])
    return keyword_tup


def create_keyword_table_partial(df):
    dfs = df[['_id', 'section', 'pub_date', 'headline', 'keywords']]

    d1 = dfs.keywords.apply(pd.Series).merge(dfs, left_index=True, right_index=True).drop(["keywords"], axis = 1)
    d2 = d1.melt(id_vars = ['_id', 'section', 'pub_date', 'headline'], value_name = "keyword").drop("variable", axis = 1)

    mask = d2.keyword.isna()
    d3 = d2[~mask]

    d3 = d3.sort_values(by=['pub_date', '_id'])

    d3['keyword'] = d3.keyword.apply(lambda x: extr_keywords_step1(x))

    keyword_table = d3[['keyword', 'section', '_id']]
    table = keyword_table.groupby(by=['keyword', 'section']).count()
    table = table.reset_index()
    table.columns = ['keyword', 'section', 'counts']
    return table


def create_keyword_table(table, threshold):
    keyword_table = pd.DataFrame([['keyword', 'name', 'value', 0, 'section']],
                                 columns=['keyword', 'name', 'value', 'total_counts', 'section'])
    for kw in table.keyword.unique():
        entries = table[table.keyword == kw]
        max_count = entries['counts'].max()
        total_counts = entries['counts'].sum()
        if max_count >= threshold*total_counts:
            idx = entries['counts'].argmax()
            section = table.loc[idx, 'section']
        else:
            section = '*UNSPECIFIC*'
        new_row = pd.DataFrame([[kw, kw[0], kw[1], total_counts, section]], columns=['keyword', 'name', 'value', 'counts', 'section'])
        keyword_table = keyword_table.append(new_row)
    return keyword_table


######################
year = '2018'

# concat dfs to df_year and then clean and translate keywords
with open(cwd + "/data/archive/" + year + "_01.pickle", "rb") as f:
    response = pickle.load(f)
    articles = response['docs']
    df = pd.DataFrame(articles)

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

df.shape
df = clean_articles(df, 20)
df = rename_sections(df)
df.section.value_counts()
df = df[~(df['section'] == '*DELETE*')]
df.section.value_counts()



table = create_keyword_table_partial(df)
table_keywords = create_keyword_table(table, 0.5)

table_keywords.sort_values(by='counts')