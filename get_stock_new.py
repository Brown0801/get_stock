import pandas as pd
import requests
from bs4 import BeautifulSoup
import parmap
import itertools
import sqlite3

def get_price(code, page):
    try:
        url_price = 'https://finance.naver.com/item/sise_day.nhn?code=' + code
        pg_url = '{url}&page={page}'.format(url=url_price, page=page)
        price = pd.read_html(pg_url, header=0)[0].dropna()

        # price = price.set_index("날짜")

        con = sqlite3.connect('./Json/stock_price.db')
        price.to_sql(code, con, if_exists='append')

    except:
        print(code, "에러났데")
        pass

# get_price('037070', 1000)


def append_price():

    # codes = pd.read_excel('코드리스트2.xlsx', converters={'종목코드':str})['종목코드']
    codes = ['037070']
    pages = range(1,1000)

    input_list = list(itertools.product(codes, pages))
    parmap.starmap(get_price, input_list, pm_pbar=True)

def drop_duplicates(code):
    con = sqlite3.connect('./Json/stock_price.db')
    data = pd.read_sql("SELECT * FROM" + " " + "'" + code + "'", con)
    data = data.drop_duplicates(['날짜'], keep='first')
    data = data.set_index('날짜').sort_index(ascending=False)
    del data['level_0']
    del data['index']
    data.to_sql(code, con, if_exists='replace')

def drop_all():
    # codes = pd.read_excel('코드리스트2.xlsx', converters={'종목코드': str})['종목코드']
    codes = ['037070']
    parmap.map(drop_duplicates, codes, pm_pbar=True)

if __name__ == "__main__":
    append_price()
    drop_all()

# drop_duplicates('037070')
