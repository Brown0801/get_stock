import pandas as pd
import parmap
import sqlite3
from datetime import datetime
from tqdm import tqdm
import multiprocessing

num_cores = multiprocessing.cpu_count()

def get_frgn(code):
    now = datetime.now()

    try:
        path = "./temp/" + str(code) + ".db"
        con = sqlite3.connect(path)
        frgn1 = pd.read_sql("SELECT * FROM frgn", con, index_col="날짜").sort_index(ascending=False)


        last_dt = datetime.strptime(frgn1.index[0], "%Y.%m.%d").date()
        time_delta = (now.date() - last_dt).days

        if time_delta <= 6 :
            pages = 3

        else:
            pages = 30

        #새 데이터 불러오기 (조금만)
        frgn2 = pd.DataFrame()
        for page in range(1, pages):
            url_frgn = 'https://finance.naver.com/item/frgn.nhn?code=' + code
            pg_url = '{url}&page={page}'.format(url=url_frgn, page=page)
            frgn = pd.read_html(pg_url, header=1, encoding='euc-kr')[2]
            frgn2 = frgn2.append(frgn).dropna()

        frgn2 = frgn2.set_index(['날짜'])
        frgn2.rename(columns={'순매매량':'기관', '순매매량.1':'외국인', '보유주수':'외국인보유주수', '보유율':'외국인보유율'}, inplace = True)

        if frgn2.index[0] == now.strftime("%Y.%m.%d"):
            if now.hour < 19:
                frgn2 = frgn2[1:]
                frgn1 = frgn1.append(frgn2)
                frgn1 = frgn1.drop_duplicates()
                frgn1 = frgn1.sort_index(ascending=False)
                frgn1.to_sql('frgn', con, if_exists='replace')

            else:
                frgn1 = frgn1.append(frgn2)
                frgn1 = frgn1.drop_duplicates()
                frgn1 = frgn1.sort_index(ascending=False)
                frgn1.to_sql('frgn', con, if_exists='replace')

        else:
            frgn1 = frgn1.append(frgn2)
            frgn1 = frgn1.drop_duplicates()
            frgn1 = frgn1.sort_index(ascending=False)
            frgn1.to_sql('frgn', con, if_exists='replace')




            if frgn2.index[0] == now.strftime("%Y.%m.%d"):
                if now.hour < 19:
                    frgn2 = frgn2[1:]
                    frgn1 = frgn1.append(frgn2)
                    frgn1 = frgn1.drop_duplicates()
                    frgn1 = frgn1.sort_index(ascending=False)
                    frgn1.to_sql('frgn', con, if_exists='replace')

                else:
                    frgn1 = frgn1.append(frgn2)
                    frgn1 = frgn1.drop_duplicates()
                    frgn1 = frgn1.sort_index(ascending=False)
                    frgn1.to_sql('frgn', con, if_exists='replace')

            else:
                frgn1 = frgn1.append(frgn2)
                frgn1 = frgn1.drop_duplicates()
                frgn1 = frgn1.sort_index(ascending=False)
                frgn1.to_sql('frgn', con, if_exists='replace')


    except:
        pages = 1000

        frgn1 = pd.DataFrame()

        # for page in tqdm(range(1, pages), mininterval=1, desc=code):
        for page in range(1, pages):

            # try:
            url_frgn = 'https://finance.naver.com/item/frgn.nhn?code=' + code
            pg_url = '{url}&page={page}'.format(url=url_frgn, page=page)
            frgn = pd.read_html(pg_url, header=1, encoding='euc-kr')[2]
            frgn1 = frgn1.append(frgn).dropna()

        frgn1 = frgn1.drop_duplicates(['날짜'], keep='first')
        frgn1 = frgn1.set_index(['날짜']).sort_index(ascending=False)
        frgn1.rename(columns={'순매매량':'기관', '순매매량.1':'외국인', '보유주수':'외국인보유주수', '보유율':'외국인보유율'}, inplace = True)

        if frgn1.index[0] == now.strftime("%Y.%m.%d"):    #조회 시도하는 날짜가 장중
            if now.hour < 19:    #장 마감 전인가?? : 당일 시세 데이터 제외
                frgn1 = frgn1[1:]
                path = "./temp/" + str(code) + ".db"
                con = sqlite3.connect(path)
                frgn1.to_sql('frgn', con, if_exists='replace')
            else:  #장 마감 후 : 당일 시세 데이터 포함
                path = "./temp/" + str(code) + ".db"
                con = sqlite3.connect(path)
                frgn1.to_sql('frgn', con, if_exists='replace')
        else:  #조회 시도하는 날이 장중이 아닐때
            path = "./temp/" + str(code) + ".db"
            con = sqlite3.connect(path)
            frgn1.to_sql('frgn', con, if_exists='replace')

def del_frgn(code):
    try:
        path = "./temp/" + str(code) + ".db"
        con = sqlite3.connect(path)
        cursor = con.cursor()
        cursor.execute("drop table frgn")

    except:
        pass


# get_frgn('095570')
# del_frgn('095570')


if __name__ == '__main__':

    codes = pd.read_excel('코드리스트2.xlsx', converters={'종목코드': str})
    code = codes['종목코드']

    parmap.map(get_frgn, code, pm_pbar=True, pm_processes=num_cores)
    # parmap.map(del_frgn, code, pm_pbar=True, pm_processes=num_cores)