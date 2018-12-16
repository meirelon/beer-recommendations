from os import path
from datetime import datetime
import string
import re
import warnings
warnings.filterwarnings('ignore')
import urllib.request
from itertools import compress
import time
import random

import lxml.html as LH
import requests
from bs4 import BeautifulSoup as bs

import numpy as np
import pandas as pd

from scipy.spatial.distance import cdist

def get_request(u):
    count = 0
    while count < 10:
        try:
            r = requests.get(u)
            break
        except:
            count += 1
            if count > 10:
                print("failed to make request")
                return None
            time.sleep(10)
    return r


def splitDftoDict(df, split_col):
    dict_to_return = {}
    for element in df[split_col].unique():
        dict_to_return[element] = df[df[split_col]==element]
    return dict_to_return


def get_beer_styles():
    url = "https://www.beeradvocate.com/beer/styles/"
    r = get_request(url)
    all_tags = bs(r.content, "html.parser")
    beer_style_links = [x.get('href') for x in all_tags.findAll('a', attrs={'href': re.compile("/beer/styles/\d{1,}/")})]
    beer_style_names = [x.text.strip() for x in all_tags.findAll('a', attrs={'href': re.compile("/beer/styles/\d{1,}/")})]
    return beer_style_links, beer_style_names


def get_beer_style_info(beer_style, beer_link, page=0):
    print(beer_style, end=' ')
    time.sleep(abs(random.normalvariate(2,0.5)))
    beer_style_url = "https://www.beeradvocate.com{beer_style}?sort=revsD&start={page}".format(beer_style=beer_link, page=0)
    r = get_request(beer_style_url)
    all_tags = bs(r.content, "html.parser")

    beer_links = [x.get('href') for x in all_tags.findAll('a', attrs={'href': re.compile("/beer/profile/\d{1,}/")})][::2]
    brewery_link = [re.findall(string=x.get('href'), pattern="[/]beer[/]profile[/]\d{1,}[/]")[0]
                    for x in all_tags.findAll('a', attrs={'href': re.compile("/beer/profile/\d{1,}/")})][::2]
    tbl = [x.text.strip() for x in all_tags.findAll('td', attrs={'class' : re.compile("hr_bottom_light")})]
    beers = tbl[::6]
    brewery = tbl[1:][::6]
    abv = tbl[2:][::6]
    ratings = [x.replace(",", "") for x in tbl[3:][::6]]
    score = tbl[4:][::6]
    beer_style_name = [beer_style] * len(beers)
    beer_style_name_clean = [re.sub(string=beer_style,
                                    pattern="[(]|[)]",
                                    repl="").strip().replace(" / ", "_").replace(" ", "_").lower().strip()] * len(beers)

    df = pd.DataFrame.from_dict({"beer":beers,
                   "brewery":brewery,
                   "abv":abv,
                   "ratings":ratings,
                   "score":score,
                   "link" : beer_links,
                   "brewery_link" : brewery_link,
                   "beer_style" : beer_style_name,
                   "beer_style_clean" : beer_style_name_clean})
    return df


def get_brewery_info(brewery_link):
    try:
        url = "https://www.beeradvocate.com" + brewery_link
        r = requests.get(url)
        all_tags = bs(r.content, "html.parser")

        score = float([x.text.strip() for x in all_tags.findAll('span', attrs={'class' : re.compile("ba-ravg")})][0])
        beer_stats = [x.text.strip() for x in all_tags.findAll('div', attrs={'id' : re.compile("item_stats")})][0].split("\n")[1:][::2]
        beers_total, reviews_total, ratings_total = int(beer_stats[0].replace(",", "")), int(beer_stats[1].replace(",", "")), int(beer_stats[2].replace(",", ""))
        location, website = [x.get("href") for x in all_tags.findAll('a', attrs={'href' : re.compile("http"), "target" : re.compile("_blank")})][0:2]
        zipcode = re.findall(string=location, pattern="\d+")[-1]

        brewery_df = pd.DataFrame({"brewery_score":score,
                  "beers_total":beers_total,
                  "reviews_total":reviews_total,
                  "ratings_total":ratings_total,
                  "location":location,
                  "website":website,
                  "zipcode":zipcode},index=[brewery_link])

        return brewery_df
    except:
        return None




def get_beer_vector(beer_link, ratings):
    total_pages = min([12, round(int(ratings)/25)-1])
    df_list = []
    for page in range(0,total_pages):
        time.sleep(abs(random.normalvariate(1,0.5)))
        url = "https://www.beeradvocate.com{beer_link}?view=beer&sort=&start={page}".format(beer_link=beer_link, page=page)
        r = get_request(url)
        all_tags = bs(r.content, "html.parser")
        taste_list = [x for i,x in enumerate([x.text.strip()
                                   for x in all_tags.findAll('span', attrs={'class' : re.compile("muted")})])
           if re.search(pattern="[|]", string=x)]

        df_page = pd.DataFrame([[float(x.split(":")[1]) for x in vector.split("|")]
                       for vector in taste_list], columns=["look", "smell", "taste", "feel", "overall"])
        df_list.append(df_page)
        page += 25

    vectors_to_matrix = pd.concat(df_list, axis=0)
    beer_vector = vectors_to_matrix.mean().to_frame().transpose()

    beer_vector['records'] = vectors_to_matrix.shape[0]
    beer_vector['link'] = beer_link

    return beer_vector


def get_beer_style_recommendations(df):
    beer_vector_list = [get_beer_vector(beer_link=x, ratings=y) for x,y in df[['link', 'ratings']].values.tolist()]
    try:
        beer_vector_df = pd.concat(beer_vector_list, axis=0)
    except:
        return None

    a = beer_vector_df[["look", "smell", "taste", "feel", "overall", "records"]]
    d = pd.DataFrame(cdist(a, a), columns=beer_vector_df['link'])
    d_ranked = d.rank(axis=1).transpose()
    rec_df = pd.DataFrame([{"link":d_ranked.index[d_ranked.iloc[:,col] == 1].values[0],
                   "recommendations":list(d_ranked.index[(d_ranked.iloc[:,col] > 1) & (d_ranked.iloc[:,col] < 5)].values)}
     for col in list(range(len(d.columns)))])

    rec_df_wide = rec_df.recommendations.apply(pd.Series).merge(rec_df, left_index=True, right_index=True).drop(["recommendations"], axis=1)
    rec_df_wide.columns = ["rec1", "rec2", "rec3", "link"]

    recs_df_long = pd.melt(rec_df_wide, id_vars=["link"],
                  value_vars=["rec1", "rec2", "rec3"],
                  value_name="beer_rec",
                  var_name="rec_rank")
    recs_df_long.loc[recs_df_long['rec_rank'] == 'rec1', 'rec_rank'] = '1'
    recs_df_long.loc[recs_df_long['rec_rank'] == 'rec2', 'rec_rank'] = '2'
    recs_df_long.loc[recs_df_long['rec_rank'] == 'rec3', 'rec_rank'] = '3'
    recs_df_long['rec_rank'] = recs_df_long['rec_rank'].astype(int)


    return recs_df_long
