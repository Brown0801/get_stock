import pandas as pd
import requests
from bs4 import BeautifulSoup
import parmap
import itertools
import sqlite3

def get_name(code):   #종목명 구하기
    url = 'https://finance.naver.com/item/main.nhn?code=' + code
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html5lib")
    tags = soup.select("#middle > div.h_company > div.wrap_company > h2 > a")
    tag = tags[0].text
    return tag



def cal_earn(date, code):
    ######################################데이터 불러오기 & 가공######################################
    con = sqlite3.connect('./Json/stocks_price_vol.db')
    df = pd.read_sql("SELECT * FROM" + " " + "'" + code + "'", con, index_col='날짜')
    df = df.sort_index(ascending=True)

    # 날짜 인덱싱 넘버 구하기
    date_idx = df.index.tolist()
    idx_num = date_idx.index(date)

    edit = lambda x: float(x.replace('%', ''))
    df['등락률'] = df['등락률'].apply(edit)

    foreigner = df['외국인보유율'].apply(edit)

    edit2 = lambda x: float(x.replace(',', ''))
    df['기관순매매'] = df['기관순매매'].apply(edit2)
    df['외국인순매매'] = df['외국인순매매'].apply(edit2)


    ##################################수익률 계산하기###############################

    ###기간내 고가####
    p_h = df['고가'][idx_num+1:idx_num+11].max()
    ###해당일종가(구매가)
    p_b = df['종가'][idx_num]
    ###수익률
    earn = int(((p_h - p_b)/p_b)*100)

    print(date, code, get_name(code), earn)