import pandas as pd
import requests
from bs4 import BeautifulSoup
import parmap
import itertools
import sqlite3

def get_frgn(code, page):
    try:
        url_frgn = 'https://finance.naver.com/item/frgn.nhn?code=' + code
        pg_url = '{url}&page={page}'.format(url=url_frgn, page=page)
        frgn = pd.read_html(pg_url, header=1, encoding='euc-kr')[2].dropna()

        frgn1 = frgn.set_index("날짜")

        edit = lambda x: float(x.replace('%', ''))
        edit2 = lambda x: float(x.replace(',', ''))

        data = pd.DataFrame()
        data['등락률'] = frgn1['등락률'].apply(edit)
        data['기관순매매'] = frgn1['순매매량'].apply(edit2)
        data['외국인순매매'] = frgn1['순매매량.1'].apply(edit2)
        data['외국인보유주수'] = frgn1['보유주수']
        data['외국인보유율'] = frgn1['보유율'].apply(edit)

        con = sqlite3.connect('./Json/stock_info.db')
        data.to_sql(code, con, if_exists='append')

    except:
        pass


def append_frgn():

    codes = pd.read_excel('코드리스트2.xlsx', converters={'종목코드':str})['종목코드']
    pages = range(1,200)

    input_list = list(itertools.product(codes, pages))
    parmap.starmap(get_frgn, input_list, pm_pbar=True)

def drop_duplicates(code):
    con = sqlite3.connect('./Json/stock_info_test.db')
    data = pd.read_sql("SELECT * FROM" + " " + "'" + code + "'", con)
    data = data.drop_duplicates(['날짜'], keep='first')
    data = data.set_index('날짜').sort_index(ascending=False)
    data.to_sql(code, con, if_exists='replace')

def drop_all():
    codes = pd.read_excel('코드리스트2.xlsx', converters={'종목코드': str})['종목코드']
    parmap.map(drop_duplicates, codes, pm_pbar=True)

if __name__ == "__main__":
    append_frgn()
    drop_all()
