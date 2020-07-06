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

    path = "./temp/" + str(code) + ".db"

    con = sqlite3.connect(path)
    price.to_sql('price', con, if_exists='append')


    # except:
    #     print(code, page, "에러났데")
    #     pass

# get_price('037070', 1000)


def append_price():

    codes = pd.read_excel('코드리스트_test.xlsx', converters={'종목코드':str})['종목코드']
    # codes = ['095570']
    pages = range(1,1000)

    for code in codes:
        csv = pd.DataFrame(columns=['날짜', '종가', '전일비', '시가', '고가', '저가', '거래량'])
        path = "./temp/" + str(code) + ".db"
        con = sqlite3.connect(path)
        csv.to_sql('price', con, if_exists='replace')

        input_list = list(itertools.product([code], pages))
        parmap.starmap(get_price, input_list, pm_pbar=True)

def sum_price(code):
    # print(code, 'start')
    path = "./temp/" + str(code) + "price" + ".csv"
    data = pd.read_csv(path)

    data = data.drop_duplicates(['날짜'], keep='first')
    # data = data.set_index('날짜').sort_index(ascending=False)
    #
    print(data)



    # del data['Unnamed: 0']
    #
    # con = sqlite3.connect('./Json/stock_price.db')
    # data.to_sql(code, con, if_exists='replace')
    #
    # print(code, 'end')

# sum_price('095570')


if __name__ == "__main__":
    append_price()
#
#     # # codes = pd.read_excel('코드리스트2.xlsx', converters={'종목코드':str})['종목코드']
#     # codes = ['095570']
#     # for code in codes:
#     #     sum_price(code)
