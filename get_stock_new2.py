import pandas as pd
import parmap
import itertools
import sqlite3
from tqdm import tqdm
from datetime import datetime


def get_price(code):
    now = datetime.now()

    try:
        path = "./temp/" + str(code) + ".db"
        con = sqlite3.connect(path)
        price1 = pd.read_sql("SELECT * FROM price", con, index_col="날짜")

        last_dt = datetime.strptime(price1.index[0], "%Y.%m.%d").date()
        time_delta = (now.date() - last_dt).days

        if time_delta <= 6:
            pages = 3

        else:
            pages = 30

        #새 데이터 불러오기 (조금만)
        price2 = pd.DataFrame()
        for page in range(1, pages):
            url_price = 'https://finance.naver.com/item/sise_day.nhn?code=' + code
            pg_url = '{url}&page={page}'.format(url=url_price, page=page)
            price = pd.read_html(pg_url, header=0)[0].dropna()
            price2 = price2.append(price)

        price2 = price2.set_index(['날짜'])

        if price2.index[0] == now.strftime("%Y.%m.%d"):
            if now.hour < 16:
                price2 = price2[1:]
                price1 = price1.append(price2)
                price1 = price1.drop_duplicates()
                price1 = price1.sort_index(ascending=False)
                price1.to_sql('price', con, if_exists='replace')

            else:
                price1 = price1.append(price2)
                price1 = price1.drop_duplicates()
                price1 = price1.sort_index(ascending=False)
                price1.to_sql('price', con, if_exists='replace')

        else:
            price1 = price1.append(price2)
            price1 = price1.drop_duplicates()
            price1 = price1.sort_index(ascending=False)
            price1.to_sql('price', con, if_exists='replace')


    except:
        pages = 1000

        price1 = pd.DataFrame()

        # for page in tqdm(range(1, pages), mininterval=1, desc=code):
        for page in range(1, pages):

            # try:
            url_price = 'https://finance.naver.com/item/sise_day.nhn?code=' + code
            pg_url = '{url}&page={page}'.format(url=url_price, page=page)
            price = pd.read_html(pg_url, header=0)[0].dropna()

            price1 = price1.append(price)

        price1 = price1.drop_duplicates(['날짜'], keep='first')
        price1 = price1.set_index(['날짜']).sort_index(ascending=False)

        if price1.index[0] == now.strftime("%Y.%m.%d"):    #조회 시도하는 날짜가 장중
            if now.hour < 16:    #장 마감 전인가?? : 당일 시세 데이터 제외
                price1 = price1[1:]
                path = "./temp/" + str(code) + ".db"
                con = sqlite3.connect(path)
                price1.to_sql('price', con, if_exists='replace')
            else:  #장 마감 후 : 당일 시세 데이터 포함
                path = "./temp/" + str(code) + ".db"
                con = sqlite3.connect(path)
                price1.to_sql('price', con, if_exists='replace')
        else:  #조회 시도하는 날이 장중이 아닐때
            path = "./temp/" + str(code) + ".db"
            con = sqlite3.connect(path)
            price1.to_sql('price', con, if_exists='replace')



# get_price('095570', 30)


if __name__ == '__main__':

    codes = pd.read_excel('코드리스트2.xlsx', converters={'종목코드': str})
    code = codes['종목코드']

    parmap.map(get_price, code, pm_pbar=True)