from db_utils import query_analytics_store, query_hive_ssh
import pandas as pd

def get_month_pageviews_pre_hive():

    """
    Get monthly pageview counts per (project, country, device)
    from aggregate of sampled logs in pentaho table.
    Starting in April 2015, we have unsampled counts
    in Hive, which we use instead.
    """
    query = """
    SELECT
    SUM(pageviews) AS n,
    timestamp, 
    country_iso, 
    project, 
    access_method
    FROM staging.pentahoviews05
    WHERE is_spider = 0
    AND is_automata = 0
    AND project RLIKE 'wikipedia'
    group by timestamp, country_name, project, access_method
    """

    df = query_analytics_store(query, {})

    # only have partial data for April 2015
    df = df[df['timestamp'] != '2015-04-01']
    df.index = df['timestamp']

    return df

def get_month_pageviews_post_hive():
    """
    Get monthly pageview counts per (project, country, device)
    from hive.
    """

    query = """
    SET mapred.job.queue.name=priority;
    SELECT
    sum(view_count) as n,
    year,
    month,
    country_code as country_iso, 
    project, 
    access_method
    FROM wmf.projectview_hourly
    WHERE agent_type = 'user'
    AND project RLIKE 'wikipedia'
    AND YEAR >= 2015
    group by year, month, country_code, project, access_method;
    """

    df = query_hive_ssh(query, 'forecasting_refresh', priority=True, delete=True)
    df['month'] = df['month'].astype(str)
    df['year'] = df['year'].astype(str)
    df['month'] = df['month'].apply(lambda x: x if len(x) == 2 else '0' + x)
    df['month'].value_counts()
    df['timestamp'] = df['year'] + '-' + df['month'] + '-01'
    del df['month']
    del df['year']
    df.index = df['timestamp']

    return df

def replace_iso_with_country(df):
    """
    ISO codes are hard to read. Lets swap them out for the 
    country name.
    """
    df_codes = pd.read_csv('data/country_codes.csv')
    df_codes = df_codes[['ISO 3166-1 2 Letter Code', 'Common Name' ]]
    codes_dict = dict(tuple(x) for x in df_codes.values)
    codes_dict['Unknown'] = 'Unknown'
    codes_dict['--'] = 'Unknown'
    df['country'] = df['country_iso'].apply(lambda x: codes_dict.get(x, None))
    return df.dropna(subset = ['country'])



def untidy(df, min_views = 1000000):

    """
    Convert tidy data frame into one where
    each col represents a timeseries of counts
    for each (project, country, device) tuple.
    """
    group_dimensions = ['project', 'access_method', 'country',]
    groups = df.groupby(group_dimensions)
    cube = {}
    for group in groups:
        colname = '/'.join(group[0])
        dg = group[1]
        data = pd.Series(dg['n'])
        if data.sum() > min_views:
            cube[colname] = data
    df_cube = pd.DataFrame(cube)

    df_cube['timestamp'] = df_cube.index
    df_valid_cols = [c for c in df_cube.columns if len(c.split('/')) == 3]
    df_valid_cols.insert(0,'timestamp')
    df_cube = df_cube[df_valid_cols]

    # last month will not be complete
    return df_cube[:-1]




def main():
    d1 = get_month_pageviews_pre_hive()
    d2 = get_month_pageviews_post_hive()
    df = pd.concat([d1, d2], axis=0)
    df = replace_iso_with_country(df)
    df.to_csv('data/checkpoint.csv')
    df_cube = untidy(df)
    df_cube.to_csv('data/cube.csv', index = False)



if __name__ == '__main__':
    main()