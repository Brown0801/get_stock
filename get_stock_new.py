import pandas as pd
import parmap
import itertools
import sqlite3

def get_price(code, page):
    # try:
    url_price = 'https://finance.naver.com/item/sise_day.nhn?code=' + code
    pg_url = '{url}&page={page}'.format(url=url_price, page=page)
    price = pd.read_html(pg_url, header=0)[0].dropna()

    # con = sqlite3.connect('./Json/stock_price.db')
    # price.to_sql(code, con, if_exists='append')

    path = "./temp/" + str(code) + "price" + str(page) + ".xlsx"

    price.to_excel(path)


    # except:
    #     print(code, page, "에러났데")
    #     pass

# get_price('037070', 1000)


def append_price():

    # codes = pd.read_excel('코드리스트2.xlsx', converters={'종목코드':str})['종목코드']
    codes = ['009810']
    pages = range(1,1000)

    input_list = list(itertools.product(codes, pages))
    parmap.starmap(get_price, input_list, pm_pbar=True)

def sum_price(code):
    print(code, 'start')
    data = pd.DataFrame()
    for page in range(1,999):
        path = "./temp/" + str(code) + "price" + str(page) + ".xlsx"
        data1 = pd.read_excel(path)
        data = data.append(data1)

    data = data.drop_duplicates(['날짜'], keep='first')
    data = data.set_index('날짜').sort_index(ascending=False)
    del data['Unnamed: 0']

    con = sqlite3.connect('./Json/stock_price.db')
    data.to_sql(code, con, if_exists='replace')

    print(code, 'end')

# sum_price('009810')


if __name__ == "__main__":
    append_price()

codes = pd.read_excel('코드리스트2.xlsx', converters={'종목코드':str})['종목코드']
for code in codes:
    sum_price(code)
