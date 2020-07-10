import pandas as pd
import sqlite3
from tqdm import tqdm
from datetime import datetime


# a = datetime.strptime("2020.01.30", "%Y.%m.%d").date()
# b = datetime.now().date()
#
# timedelta = (b-a).days
# print(type(timedelta))


def get_kospi():
    now = datetime.now()
    try:
        path = "./temp/index.db"
        con = sqlite3.connect(path)
        kospi1 = pd.read_sql("SELECT * FROM kospi", con, index_col="날짜")

        last_dt = datetime.strptime(kospi1.index[0], "%Y.%m.%d").date()
        time_delta = (now.date() - last_dt).days

        if time_delta <= 6:
            pages = 3

        else:
            pages = 30

        kospi2 = pd.DataFrame()
        for page in tqdm(range(1, pages), mininterval=1, desc='KOSPI'):
            url_kospi = 'https://finance.naver.com/sise/sise_index_day.nhn?code=KOSPI'
            pg_url = '{url}&page={page}'.format(url=url_kospi, page=page)
            kospi = pd.read_html(pg_url, header=0)[0].dropna()
            kospi2 = kospi2.append(kospi).dropna()

        kospi2 = kospi2.set_index(['날짜'])

        if kospi2.index[0] == now.strftime("%Y.%m.%d"):
            if now.hour < 16:
                kospi2 = kospi2[1:]
                kospi1 = kospi1.append(kospi2)
                kospi1 = kospi1.drop_duplicates()
                kospi1 = kospi1.sort_index(ascending=False)
                kospi1.to_sql('kospi', con, if_exists='replace')

            else:
                kospi1 = kospi1.append(kospi2)
                kospi1 = kospi1.drop_duplicates()
                kospi1 = kospi1.sort_index(ascending=False)
                kospi1.to_sql('kospi', con, if_exists='replace')

        else:
            kospi1 = kospi1.append(kospi2)
            kospi1 = kospi1.drop_duplicates()
            kospi1 = kospi1.sort_index(ascending=False)
            kospi1.to_sql('kospi', con, if_exists='replace')


    except:

        pages = 1000
        kospi1 = pd.DataFrame()
        for page in tqdm(range(1, pages), mininterval=1, desc='KOSPI'):
        # for page in range(1, pages):

            # try:
            url_kospi = 'https://finance.naver.com/sise/sise_index_day.nhn?code=KOSPI'
            pg_url = '{url}&page={page}'.format(url=url_kospi, page=page)
            kospi = pd.read_html(pg_url, header=0)[0].dropna()

            kospi1 = kospi1.append(kospi)

        kospi1 = kospi1.drop_duplicates(['날짜'], keep='first')
        kospi1 = kospi1.set_index(['날짜']).sort_index(ascending=False)


        if kospi1.index[0] == now.strftime("%Y.%m.%d"):    #조회 시도하는 날짜가 장중
            if now.hour < 16:    #장 마감 전인가?? : 당일 시세 데이터 제외
                kospi1 = kospi1[1:]
                path = "./temp/index.db"
                con = sqlite3.connect(path)
                kospi1.to_sql('kospi', con, if_exists='replace')
            else:  #장 마감 후 : 당일 시세 데이터 포함
                path = "./temp/index.db"
                con = sqlite3.connect(path)
                kospi1.to_sql('kospi', con, if_exists='replace')
        else:  #조회 시도하는 날이 장중이 아닐때
            path = "./temp/index.db"
            con = sqlite3.connect(path)
            kospi1.to_sql('kospi', con, if_exists='replace')

def get_kosdaq():
    now = datetime.now()
    try:
        path = "./temp/index.db"
        con = sqlite3.connect(path)
        kosdaq1 = pd.read_sql("SELECT * FROM kosdaq", con, index_col="날짜")


        last_dt = datetime.strptime(kosdaq1.index[0], "%Y.%m.%d").date()
        time_delta = (now.date() - last_dt).days

        if time_delta <= 6:
            pages = 3

        else:
            pages = 30

        kosdaq2 = pd.DataFrame()
        for page in tqdm(range(1, pages), mininterval=1, desc='KOSDAQ'):
            url_kosdaq = 'https://finance.naver.com/sise/sise_index_day.nhn?code=kosdaq'
            pg_url = '{url}&page={page}'.format(url=url_kosdaq, page=page)
            kosdaq = pd.read_html(pg_url, header=0)[0].dropna()
            kosdaq2 = kosdaq2.append(kosdaq).dropna()

        kosdaq2 = kosdaq2.set_index(['날짜'])

        if kosdaq2.index[0] == now.strftime("%Y.%m.%d"):
            if now.hour < 16:
                kosdaq2 = kosdaq2[1:]
                kosdaq1 = kosdaq1.append(kosdaq2)
                kosdaq1 = kosdaq1.drop_duplicates()
                kosdaq1 = kosdaq1.sort_index(ascending=False)
                kosdaq1.to_sql('kosdaq', con, if_exists='replace')

            else:
                kosdaq1 = kosdaq1.append(kosdaq2)
                kosdaq1 = kosdaq1.drop_duplicates()
                kosdaq1 = kosdaq1.sort_index(ascending=False)
                kosdaq1.to_sql('kosdaq', con, if_exists='replace')

        else:
            kosdaq1 = kosdaq1.append(kosdaq2)
            kosdaq1 = kosdaq1.drop_duplicates()
            kosdaq1 = kosdaq1.sort_index(ascending=False)
            kosdaq1.to_sql('kosdaq', con, if_exists='replace')


    except:
        pages = 1000
        kosdaq1 = pd.DataFrame()
        for page in tqdm(range(1, pages), mininterval=1, desc='KOSDAQ'):
        # for page in range(1, pages):

            # try:
            url_kosdaq = 'https://finance.naver.com/sise/sise_index_day.nhn?code=kosdaq'
            pg_url = '{url}&page={page}'.format(url=url_kosdaq, page=page)
            kosdaq = pd.read_html(pg_url, header=0)[0].dropna()

            kosdaq1 = kosdaq1.append(kosdaq)

        kosdaq1 = kosdaq1.drop_duplicates(['날짜'], keep='first')
        kosdaq1 = kosdaq1.set_index(['날짜']).sort_index(ascending=False)

        if kosdaq1.index[0] == now.strftime("%Y.%m.%d"):    #조회 시도하는 날짜가 장중
            if now.hour < 16:    #장 마감 전인가?? : 당일 시세 데이터 제외
                kosdaq1 = kosdaq1[1:]
                path = "./temp/index.db"
                con = sqlite3.connect(path)
                kosdaq1.to_sql('kosdaq', con, if_exists='replace')
            else:  #장 마감 후 : 당일 시세 데이터 포함
                path = "./temp/index.db"
                con = sqlite3.connect(path)
                kosdaq1.to_sql('kosdaq', con, if_exists='replace')
        else:  #조회 시도하는 날이 장중이 아닐때
            path = "./temp/index.db"
            con = sqlite3.connect(path)
            kosdaq1.to_sql('kosdaq', con, if_exists='replace')


get_kospi()
get_kosdaq()