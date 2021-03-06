import pandas as pd
import requests
from bs4 import BeautifulSoup
import parmap
import itertools
import sqlite3

def get_kind(code):
    url = 'https://navercomp.wisereport.co.kr/v2/company/c1070001.aspx?cmp_cd=' + code
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html5lib")
    tags = soup.select("#pArea > div.wrapper-table > div > table > tbody > tr:nth-child(1) > td > dl > dt:nth-child(3)")
    tag = tags[0].text
    # tag = tag.replace('KOSPI : ', '')
    return tag

def get_name(code):   #종목명 구하기
    url = 'https://finance.naver.com/item/main.nhn?code=' + code
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html5lib")
    tags = soup.select("#middle > div.h_company > div.wrap_company > h2 > a")
    tag = tags[0].text
    return tag

def get_issue(code):   #주식 발행량 구하기
    url = 'https://finance.naver.com/item/sise.nhn?code=' + code
    html = requests.get(url).text

    soup = BeautifulSoup(html, "html5lib")
    tags = soup.select("#content > div.section.inner_sub > div:nth-child(1) > table > tbody > tr:nth-child(12) > td:nth-child(4) > span > span")
    tag = tags[0].text
    tag1 = tag.replace(',', '')
    tag2 = int(tag1)
    return tag2

def get_move(code):   #유동주식수 구하기
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

def get_eps(code):  #현재의 EPS 구하기
    try:
        url = 'https://finance.naver.com/item/main.nhn?code=' + code
        html = requests.get(url).text
        soup = BeautifulSoup(html, "html5lib")
        tags = soup.select("#tab_con1 > div:nth-child(5) > table > tbody:nth-child(2) > tr > td > em:nth-child(3)")
        tag = tags[0].text
        return int(tag.replace(',', ''))
    except:
        return 'N/A'

def get_bps(code):  #현재의 BPS 구하기
    try:
        url = 'https://finance.naver.com/item/main.nhn?code=' + code
        html = requests.get(url).text
        soup = BeautifulSoup(html, "html5lib")
        tags = soup.select("#tab_con1 > div:nth-child(5) > table > tbody:nth-child(3) > tr:nth-child(2) > td > em:nth-child(3)")
        tag = tags[0].text
        return int(tag.replace(',', ''))
    except:
        return 'N/A'


def buy(date, code):
    try:
        ######################################데이터 불러오기 & 가공######################################
        con = sqlite3.connect('./Json/stocks_price_vol.db')
        df = pd.read_sql("SELECT * FROM" + " " + "'" + code + "'", con, index_col='날짜')
        df = df.sort_index(ascending=True)

        # 날짜 인덱싱 넘버 구하기
        date_idx = df.index.tolist()
        idx_num = date_idx.index(date)

        edit = lambda x: float(x.replace('%', ''))
        df['등락률'] = df['등락률'].apply(edit)

        foreigner = df['외국인보유율'].apply(edit)

        edit2 = lambda x: float(x.replace(',', ''))
        df['기관순매매'] = df['기관순매매'].apply(edit2)
        df['외국인순매매'] = df['외국인순매매'].apply(edit2)

        ######################################매수조건 수치 계산######################################
        # CCI 구하기
        m1 = (df['고가'] + df['저가'] + df['종가']) / 3
        m2 = m1.rolling(window=9).mean()
        d = abs(m1 - m2).rolling(window=9).mean()
        cci = (m1 - m2) / (d * 0.0015)

        #블린저밴드 구하기
        avg_60 = df['종가'].rolling(window=60).mean()
        std_60 = df['종가'].rolling(window=60).std()
        bollin_plus = avg_60 + std_60 * 2
        bollin_minus = avg_60 - std_60 * 2

        #주가평균선 구하기
        avg_5 = df['종가'].rolling(window=5).mean()
        avg_20 = df['종가'].rolling(window=20).mean()
        #avg_60은 블린저밴드 부분에 있음
        avg_120 = df['종가'].rolling(window=120).mean()
        avg_240 = df['종가'].rolling(window=240).mean()

        df['거래대금'] = df['거래량'] * df['종가']

        # movable = int((get_move(code)/get_issue(code))*100)



        ####### 거래량 기준 #### 이게 평균 수익률이 훨씬 높음률    하 이거 모르겠다
        if df['거래량'].rolling(window=20).mean()[idx_num -1] * 5 < df['거래량'][idx_num] \
            and int(df['거래대금'].rolling(window=20).mean()[idx_num -1] / 100000000) <= 10 \
            and df['고가'][idx_num] < bollin_plus[idx_num] \
            and avg_5[idx_num -1] < avg_5[idx_num] \
            and df['종가'][idx_num] > avg_5[idx_num] and df['종가'][idx_num] > avg_60[idx_num] :
                # and df['거래량'][idx_num] > df['거래량'][idx_num +1] \

            # and df['종가'][idx_num] >= df['시가'][idx_num +1] :
            # and df['종가'][idx_num] * get_issue(code) <= 100000000000 :
            #     and df['거래대금'].rolling(window=20).max()[idx_num - 1] < 500000000 \

                    # and df['종가'][idx_num] > avg_5[idx_num] and df['종가'][idx_num] > avg_60[idx_num] \
            # and df['저가'][idx_num] > avg_5[idx_num] \
            # and avg_5[idx_num] > avg_20[idx_num] > avg_60[idx_num] \
            # and df['고가'][idx_num] < bollin_plus[idx_num] and df['저가'][idx_num] > bollin_minus[idx_num] \
            # and df['종가'][idx_num +1] <= bollin_plus[idx_num+1] \
            # and df['종가'][idx_num]*get_issue(code) <= 100000000000 \



            # and df['거래량'][idx_num] * 0.5 <= df['거래량'][idx_num +1] \



            # and high_52 <= df['종가'][idx_num] :





            price_buy = df['종가'][idx_num+1]
            df_after = df[idx_num + 1:idx_num +21]  # 20일 이내로 한정
            price_max = df_after['고가'].max()  #222 최고가(고가기준)
            max_date = df_after['고가'].idxmax()  # 최고가 날자
            earn_high = int(((price_max - price_buy) / price_buy) * 100)  # 최고가(종가기준) 달성시 수익률

            print(earn_high, code, get_name(code), date, max_date)



        else:
            pass


    except:
        pass


def excute(date, code):  #매수-매도 실행
    buy(date, code)


def anal():  #csv로 저장된 탐색결과를 불러와서 평균최고수익률을 계산 및 출력한다
    result = pd.read_csv('analyze_result.csv')
    earn_h = result['최고수익률']
    avg_h = earn_h.mean()
    print(avg_h)


# excute('2019.12.12', '095570')    ####테스트용

#다중실행 함수
def exec18():
    #date 불러오기
    dates = pd.read_excel('workingdays_201912.xlsx', converters={'영업일': str})
    date1 = dates['영업일']
    # date1 = ['2020.06.01', '2020.06.02', '2020.06.03', '2020.06.04', '2020.06.05', '2020.06.08', '2020.06.09', '2020.06.10', '2020.06.11', '2020.06.12', '2020.06.15', '2020.06.16', '2020.06.17', '2020.06.18']    ####테스트용

    #code 불러오기
    codes = pd.read_excel('코드리스트.xlsx', converters={'종목코드': str})
    code = codes['종목코드']
    # code = ['011790']    ####테스트용

    #불러온 date 라인별로 실행
    for date in date1:
        # print(date)
        date = [date]
        input_list = list(itertools.product(date, code))
        parmap.starmap(excute, input_list, pm_pbar=False)


#실제 실행코드
if __name__ == '__main__':

    ### 탐색실행
    exec18()


