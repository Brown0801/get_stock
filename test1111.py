import pandas as pd
import requests
from bs4 import BeautifulSoup
import parmap
import itertools
import sqlite3
import warnings
from datetime import datetime


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

def get_name(code):   #종목명 구하기
    url = 'https://finance.naver.com/item/main.nhn?code=' + code
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html5lib")
    tags = soup.select("#middle > div.h_company > div.wrap_company > h2 > a")
    tag = tags[0].text
    return tag


def get_infos(code):
    try:
        infos = pd.DataFrame(columns=['종목명', '종목코드', 'EPS', 'BPS', '유동주식수', 'WICS', '업데이트'])

        now = datetime.now()

        name = [get_name(code)]
        eps = [get_eps(code)]
        bps = [get_bps(code)]
        move = [get_move(code)]
        wics = [get_wics(code)]
        update = [now.strftime("%Y.%m.%d")]


        infos['종목명'] = name
        infos['종목코드'] = [str(code)]
        infos['EPS'] = eps
        infos['BPS'] = bps
        infos['유동주식수'] = move
        infos['WICS'] = wics
        infos['업데이트'] = update

        path = "./temp/" + str(code) + ".db"
        con = sqlite3.connect(path)

        infos.to_sql('infos', con, if_exists='replace')

    except:
        print(code, '에러-재시도')

#     print(infos)
#
# get_infos('095570')


def find_null(code):
    path = "./temp/" + str(code) + ".db"
    con = sqlite3.connect(path)

    try:
        data = pd.read_sql("SELECT * FROM infos", con)
        name = data['종목명']
        pass

    except:
        print(code, '재시도')
        get_infos(code)


def plus_info(code):
    try:
        path = "./temp/" + str(code) + ".db"
        con = sqlite3.connect(path)

        infos = pd.read_sql("SELECT * FROM infos", con)

        data = pd.read_excel('코드리스트2.xlsx', converters={'종목코드': str, '업종코드': str})
        data = data.set_index('종목코드').loc[code]

        infos['업종코드'] = data['업종코드']
        infos['업종'] = data['업종']
        infos['상장주식수'] = data['상장주식수']
        infos['자본금'] = data['자본금']
        infos['액면가'] = data['액면가']
        infos['시장구분'] = data['시장구분']

        infos.to_sql('infos', con, if_exists='replace')

    except:
        print(code, '에러')
        pass

# plus_info('095570')


def del_trash(code):
    try:
        path = "./temp/" + str(code) + ".db"
        con = sqlite3.connect(path)

        infos = pd.read_sql("SELECT * FROM infos", con)
        del level_0
        infos.to_sql('infos', con, if_exists='replace')

    except:
        pass

# del_trash('095570')


if __name__ == '__main__':

    codes = pd.read_excel('코드리스트2.xlsx', converters={'종목코드': str})
    code = codes['종목코드']

    parmap.map(get_infos, code, pm_pbar=True)
    parmap.map(find_null, code, pm_pbar=True)
    parmap.map(plus_info, code, pm_pbar=True)
    parmap.map(del_trash, code, pm_pbar=True)





