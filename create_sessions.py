# -*- coding: utf-8 -*-
"""
Created on Thu Aug  8 14:02:20 2019

@author: moti.dabastani
"""

import argparse
import sys
import pandas as pd
from google.cloud import bigquery
import numpy as np
import datetime
import os
import json
import uuid
from utils.agg_config import agg_config
from utils.columns_names import col_change
from utils.logs import task, handle_errors
from utils.load_from_df import load_from_df
from utils.get_dates import get_date
import time

os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

def load_json(path=None):
    path = path if path is not None else 'config.json'
    with open(path) as json_data_file:
        config = json.load(json_data_file)
    return config

def query_l(name):
    config = load_json()
    name = config[name][0]
    with open(name)as f:
        query_ = f.read()
    return query_
    

def get_df(query):
    client = bigquery.Client()
    query_job = client.query(query)
    results = query_job.result()  
    df  = results.to_dataframe()
    return  df


def create_session(df):
    df['time_diff'] = df['event_time'].diff(1)
    df.loc[df.uid != df.uid.shift(1) , 'time_diff'] = np.nan
    df['session_duration'] = (df.time_diff.dt.total_seconds()/60)
    T = 30
    df['new_session'] = ((df['time_diff'].dt.total_seconds()/60>=T) | (df['device_name'] != df['device_name'].shift(1))).astype(int)
    df['session_dur'] = df.groupby('uid')['session_duration'].sum()
    df['s_num'] = df.groupby('uid')['new_session'].cumsum()
    df['session_id'] = df['uid'].astype(str) + '_' + df['s_num'].astype(str)
    df.loc[(df.session_id != df.session_id.shift(1)), 'time_diff'] = np.nan
    df['session_duration'] = (df.time_diff.dt.total_seconds()/60)
    return df

def unique_num_of_pages(df):
    m = df['page_type'].eq('home')
    #create consecutive groups and filter only by mask home groups
    s = m.ne(m.groupby(df['session_id']).shift()).cumsum()[m]
    df['home'] = s.map(s.value_counts()).eq(1).astype(int)
    #df['page_type']  = df['page_type'].mask(m | df['page_type'].eq('null'))
    df['home'] = df['home'].fillna(0).astype(int)
    temp = df.groupby('session_id').agg({'article_id':'nunique',
                                    'home':'sum'}).rename(columns={'article_id':'unique_articles',
                          'home':'non_consectutive_home'})
    return temp


def subsription(df):
    df['subscription'] = ((df.user_type != df['user_type'].shift(1)) & (df.user_type == 'Digital Paying')).astype(int)
    return df

    
def page_types_c(df):
    page_types = [ x for x in list(df.page_type.unique()) if isinstance(x,str)]
    temp = pd.DataFrame(columns=page_types,index=df.groupby('session_id').size().index)
    for t in page_types:
        temp[t] = df.groupby('session_id').agg({'page_type':lambda x:(x==t).sum()})
    temp.columns = [x for x in temp.columns.ravel()]
    return temp

def page_types_count(df):
    columns_names= ['article','other','quote','home','section','misc','author','article','promotions','tag','login']
    temp = df.groupby(['session_id','page_type']).size().unstack(fill_value=0).astype(np.int64)
    page_types = list(temp.columns)
    temp_list = [ x for x in columns_names if x not in page_types ]
    for col in temp_list:
        temp[col] = 0
    return  temp

def generate_uuid(df):
    df.reset_index(inplace=True)
    df['session_id'] = df.index.to_series().map(lambda x: uuid.uuid4())
    df['session_id'] = df['session_id'].astype(str)
    return df

def fl_article(df,how):
    m=df[['session_id','article_id','article_type','url_primary_section','url_secondary_section']].assign(page_in_sess=df.groupby('session_id').cumcount()+1)
    final=m[m.article_id.eq(m.groupby('session_id')['article_id'].transform(how))]
    if how == 'first':
        return final.groupby('session_id').first()
    else:
        return final.groupby('session_id').last()


def promotions_page_nums(df):
    return df.groupby('session_id').agg({'page_type':lambda x: list(np.where(x=='promotions')[0]+1)}).rename(columns={'page_type':'promotions_page_nums'})    
    

def paid_subscription(df,action_ids,pay_actions):
    _dict = ({'n_page':'last',
          'feature':'first',
          'pur_brand':'first', 
          'sale_code':'first',
          'promotions_no':'first',
       'purchase_platform':'first'})
    rename_dict = ({'n_page_last':'feature_n_page',
                'feature_first':'feature',
                'pur_brand_first':'pur_brand',
                'sale_code_first':'sale_code',
                'promotions_no_first':'promotions_no',
                'purchase_platform_first':'purchase_platform'})    
    return  (df.loc[((df["action_id"].isin(action_ids))| (df["action_id"].isin(pay_actions)))&(df['pur_brand'].notnull()),
            ["session_id","n_page","feature",'pur_brand', "sale_code", "promotions_no","purchase_platform"]]
            .groupby('session_id').agg(_dict).rename(columns=rename_dict))


def to_int(df):
    columns_types = list(df.dtypes[df.dtypes=='float64'].index)
    df[columns_types] = df[columns_types].fillna(0)
    df[columns_types] = df[columns_types].astype(np.int64)
    return df
     

def agg(df):
    config = load_json()   
    #df = task(load_query,return_df=True)
    #df.sort_values(by=['uid','event_time'],inplace=True)
    agg_dict = agg_config()
    df.openmode = df.openmode.fillna(False)
    df['page_type'] = df['page_type'].str.lower()
    df.loc[df.page_type=='homepage','page_type'] = 'home'    
    df = create_session(df)
    
    df = subsription(df)
    df = df.assign(n_page=df.groupby('session_id').cumcount()+1,inplace=True)
    agg_df = df.groupby('session_id').agg(agg_dict)
    
    agg_df.columns = ["_".join(x) if isinstance(x, tuple) else x for x in agg_df.columns.ravel()]
    agg_df.columns = col_change(list(agg_df.columns))
    agg_df = agg_df.join(unique_num_of_pages(df))
    #agg_df = agg_df.join(count_home(df))
    
    agg_df['duration'] = round(agg_df['duration'],0)
    agg_df['duration'] = agg_df['duration'].apply(lambda x: str(datetime.timedelta(seconds=x)))
    agg_df = agg_df.join(page_types_count(df))
    agg_df = agg_df.join(fl_article(df,how='first'),how='left',rsuffix = '_first')
    agg_df = agg_df.join(fl_article(df,how='last'),how='left',rsuffix = '_last')
    agg_df = agg_df.join(promotions_page_nums(df))
    action_ids, pay_actions = config['action_ids'], config['pay_actions']
    agg_df['home_rate'] = (agg_df['home']/agg_df['total_pages']*100).round(0).fillna(0).astype(str)+'%'
    agg_df = agg_df.join(paid_subscription(df,action_ids,pay_actions))
    agg_df = generate_uuid(agg_df)
    agg_df = to_int(agg_df)
    return agg_df

def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('--config',help= 'path to config.json file',default='session_config.json')
    parser.add_argument('--date',help= 'date to analyze',default=get_date())
    parser.add_argument('--start_date',help= 'Start date',default=None)
    parser.add_argument('--end_date',help= 'End date',default=get_date())
    #parser.add_argument('--to_csv',help= 'to csv?',default=None)
    args = parser.parse_args()
    return args


def gen_dates(start,end):
    return [d.strftime('%Y-%m-%d') for d in pd.date_range(start=start,end=end, freq='D')]


def main():
    args = sys.argv[1:]       
    args = parse_args(args)
    dates = []
    if args.start_date is None:
        dates.append(args.date)
    else:
        dates = gen_dates(args.start_date,args.end_date)
    print("Dates",dates)
    for date in dates:
        load_query = query_l('load_query').format(date)
        print('Query for {} has loaded'.format(date))
        df = task(load_query,return_df=True)
        #print(df.columns)
        agg_df = agg(df)
        completed, job = load_from_df(agg_df)
        if not completed:
            handle_errors(job)
        else:
            if task(query_l('clean_query')):
                print('Sessions for {} has created successfully'.format(date))
        





if __name__=="__main__":
    main()


