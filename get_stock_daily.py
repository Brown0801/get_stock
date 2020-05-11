import pandas as pd
import sqlite3
import parmap

def get_stock_daily(code):

    # print(code, "시작")

    try:
        #일단 기존 DB의 내용을 읽는다
        con = sqlite3.connect('.\Json\stocks_price_vol.db')
        origin = pd.read_sql("SELECT * FROM" + " " + "'" + code + "'", con, index_col='날짜').sort_index(ascending=False)

        #DB에 저장된 마지막 날자를 구한다
        last_date = origin.index[0]

        #최근 날자의 정보를 구해와본다
        price = pd.DataFrame()
        frgn = pd.DataFrame()

        for page in range(1, 5):
            url_price = 'https://finance.naver.com/item/sise_day.nhn?code=' + code
            pg_url = '{url}&page={page}'.format(url=url_price, page=page)
            price = price.append(pd.read_html(pg_url, header=0)[0]).dropna()

            url_frgn='https://finance.naver.com/item/frgn.nhn?code=' + code
            pg_url = '{url}&page={page}'.format(url=url_frgn, page=page)
            frgn = frgn.append(pd.read_html(pg_url, header=1, encoding='euc-kr')[2]).dropna()

        price1 = price.set_index("날짜")
        frgn1 = frgn.set_index("날짜")

        data = price1
        data['등락률'] = frgn1['등락률']
        data['기관순매매'] = frgn1['순매매량']
        data['외국인순매매'] = frgn1['순매매량.1']
        data['외국인보유주수'] = frgn1['보유주수']
        data['외국인보유율'] = frgn1['보유율']


        #새로구한 data df에 마지막 날짜의 idx_num을 구함

        date_idx = data.index.tolist()
        idx_num = date_idx.index(last_date)

        #마지막 날짜 이후의 데이터만 분리
        #앞에 1 부터는 아직 7시 안넘었을 때 사용
        # data1 = data[:idx_num]
        data1 = data[1:idx_num]


        #기존 DB에 APPEND로 저장 - 이후에 DB 데이터 활용 시 무조건 정렬 필요함
        con_1 = sqlite3.connect('.\Json\stocks_price_vol.db')
        data1.to_sql(code, con_1, if_exists='append')
        #print(data1)
        # print(code, "종료")

    except:
        pass
        # print(code, "에러")

# get_stock_daily('095570')



codes = pd.read_excel('코드리스트.xlsx', converters={'종목코드':str})
code = codes['종목코드']

if __name__ == '__main__':
    input_list = code
    parmap.map(get_stock_daily, input_list, pm_pbar=True)