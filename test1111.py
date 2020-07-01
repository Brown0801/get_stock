import pandas as pd
import requests
from bs4 import BeautifulSoup
import parmap
import itertools
import sqlite3
import warnings


def get_wics(code):
    try:
        url = 'https://navercomp.wisereport.co.kr/v2/company/c1070001.aspx?cmp_cd=' + code
        html = requests.get(url).text
        soup = BeautifulSoup(html, "html5lib")
        tags = soup.select("#pArea > div.wrapper-table > div > table > tbody > tr:nth-child(1) > td > dl > dt:nth-child(4)")
        tag = tags[0].text
        tag = tag.replace('WICS : ', '')
        return tag
    except:
        return 'N/A'

def get_eps(code):  #현재의 EPS 구하기
    try:
        url = 'https://navercomp.wisereport.co.kr/v2/company/c1070001.aspx?cmp_cd=' + code
        html = requests.get(url).text
        soup = BeautifulSoup(html, "html5lib")
        tags = soup.select("#pArea > div.wrapper-table > div > table > tbody > tr:nth-child(3) > td > dl > dt:nth-child(1) > b")
        tag = tags[0].text
        return int(tag.replace(',', ''))
    except:
        return 'N/A'

def get_bps(code):  #현재의 BPS 구하기
    try:
        url = 'https://navercomp.wisereport.co.kr/v2/company/c1070001.aspx?cmp_cd=' + code
        html = requests.get(url).text
        soup = BeautifulSoup(html, "html5lib")
        tags = soup.select("#pArea > div.wrapper-table > div > table > tbody > tr:nth-child(3) > td > dl > dt:nth-child(2) > b")
        tag = tags[0].text
        return int(tag.replace(',', ''))
    except:
        return 'N/A'

def get_move(code):   #유동주식수 구하기
    try:
        url = 'https://navercomp.wisereport.co.kr/v2/company/c1070001.aspx?cmp_cd=' + code + '&cn='
        html = requests.get(url).text
        soup = BeautifulSoup(html, "html5lib")
        tags = soup.select("#cTB711 > tbody > tr > td:nth-child(9)")
        tag = tags[0].text
        tag = tag.replace("\n", "")
        tag = tag.replace("												", "")
        tag = tag.replace("주", "")
        tag = tag.replace(",", "")
        return float(tag)
    except:
        return "N/A"



# def ffff(code):
#     warnings.filterwarnings(action='ignore')
#
#     df1 = pd.read_excel('코드리스트2.xlsx', converters={'종목코드': str})
#     df1 = df1.set_index('종목코드')
#
#     df1['EPS'][code] = get_eps(code)
#
#     df2 = df1.loc[code]
#
#     df2.to_csv('코드리스트2.csv', header='True', mode='w', encoding='utf-8-sig')
#
#
#
# def hurry():
#     df1 = pd.read_excel('코드리스트2.xlsx', converters={'종목코드': str})
#     codes = df1['종목코드']
#
#     parmap.map(ffff, codes, pm_pbar=True)
#
#
#
#
# if __name__ == '__main__':
#     hurry()


def to_db():
    data = pd.read_excel('코드리스트2.xlsx', converters={'종목코드': str, '업종코드' : str})
    con = sqlite3.connect('./Json/infos.db')
    data.to_sql('infos', con, if_exists='replace', index=False)

def get_info(code):
    con = sqlite3.connect('./Json/infos.db')
    cur = con.cursor()

    eps = str(get_eps(code))
    bps = str(get_bps(code))
    move = str(get_move(code))
    wics = get_wics(code)

    cur.execute("UPDATE infos SET EPS=" + "'" + eps + "'" + "WHERE 종목코드=" + "'" + code + "'")
    con.commit()

    cur.execute("UPDATE infos SET BPS=" + "'" + bps + "'" + "WHERE 종목코드=" + "'" + code + "'")
    con.commit()

    cur.execute("UPDATE infos SET 유동주식수=" + "'" + move + "'" + "WHERE 종목코드=" + "'" + code + "'")
    con.commit()

    cur.execute("UPDATE infos SET '업종(WICS)'=" + "'" + wics + "'" + "WHERE 종목코드=" + "'" + code + "'")
    con.commit()

    con.close()

# # def execute():
# #     codes = pd.read_excel('코드리스트2.xlsx', converters={'종목코드': str})['종목코드']
# #     parmap.map(get_info, codes, pm_pbar=True)
# #
# #
# # if __name__ == '__main__':
# #     execute()
#
# codes = pd.read_excel('코드리스트2.xlsx', converters={'종목코드': str})['종목코드']
# i = 0
# all = len(codes)
# for code in codes:
#     i = i + 1
#     print(i, "/", all)
#     get_info(code)


con = sqlite3.connect('./Json/infos.db')
# df1 = pd.read_sql("SELECT * FROM infos WHERE [업종(WICS)]='화학'", con)
df1 = pd.read_sql("SELECT * FROM infos", con)
wics = df1['업종(WICS)'].drop_duplicates()

for kind in wics:
    try:
        df2 = pd.read_sql("SELECT * FROM infos WHERE [업종(WICS)]=" + "'" + kind + "'" + "and EPS > 0", con, index_col='종목코드')
        codes = df2.index

        con1 = sqlite3.connect('./Json/stocks_price_vol.db')

        for code in codes:
            price = pd.read_sql("SELECT * FROM" + " " + "'" + code + "'", con1, index_col='날짜')['종가']['2020.06.22']

            df2['시가총액'] = df2['상장주식수(주)'] * price

        min = df2['시가총액'].idxmin()
        name = df2['기업명'][min]
        print(kind, min, name)

    except:
        print(kind, code, "여긴없데")
