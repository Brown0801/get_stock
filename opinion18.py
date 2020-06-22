import pandas as pd
import requests
from bs4 import BeautifulSoup
import parmap
import itertools
import sqlite3
import csv



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


        # print(cci[idx_num], bollin_plus[idx_num], avg_5[idx_num], avg_20[idx_num], avg_60[idx_num], avg_120[idx_num])

        ######################################기존 매수판단######################################

        if cci[idx_num] > 0 and cci[idx_num -1] < 0 \
                and df['종가'][idx_num] <= bollin_plus[idx_num] \
                and avg_5[idx_num] >= avg_20[idx_num] >= avg_60[idx_num] >= avg_120[idx_num]:

            #이동평균선 수렴 전략은 오히려 수익률이 낮게 나옴

            # print(date, code, get_name(code), get_kind(code), "buy")
            # return code, date

        # ########이동평균선 수렴 + 거래량 기준 전략##################
        # if df['거래량'].rolling(window=60).mean()[idx_num -1] *3 <= df['거래량'][idx_num] \
        #         and avg_20[idx_num] * 1.1 >= avg_5[idx_num] >= avg_20[idx_num] \
        #         and avg_60[idx_num] * 1.1 >= avg_20[idx_num] >= avg_60[idx_num] \
        #         and avg_120[idx_num] * 1.1 >= avg_60[idx_num] >= avg_120[idx_num] \
        #         and avg_240[idx_num] * 1.1 >= avg_120[idx_num] >= avg_240[idx_num] :
        #     p_buy = df['종가'][idx_num +1]
        #     # print(date, code, "buy", p_buy)


            df_after = df[idx_num + 1:idx_num + 41]  # 40일 이내로 한정
            price_max = df_after['종가'].max()  # 최고가(종가기준)
            max_date = df_after['종가'].idxmax()  # 최고가 날자
            earn_high = int(((price_max - df['종가'][idx_num]) / df['종가'][idx_num]) * 100)  # 최고가(종가기준) 달성시 수익률

            print(code, date, max_date, earn_high)


            return code, date


        else:
            pass

    except:
        pass

def sell(date, code):
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

        avg_120 = df['종가'].rolling(window=120).mean()


        ###########일단 최대값으로############# (매도 기준 잡기 전 까지)
        df_after = df[idx_num +1:idx_num +41]  # 40일 이내로 한정
        price_max = df_after['종가'].max()  # 최고가(종가기준)
        max_date = df_after['종가'].idxmax()  # 최고가 날자
        earn_high = int(((price_max - df['종가'][idx_num]) / df['종가'][idx_num]) * 100)  # 최고가(종가기준) 달성시 수익률

        print(code, date, max_date, earn_high)
        # print(earn_high)

        data = [code, get_name(code), date, max_date, earn_high]



        # for i in range(len(df[idx_num:].index)):
        #     idx_num2 = idx_num + i

            ###########거래량 기준 매도#############
            # if df['거래량'][idx_num2] <= df['거래량'].rolling(window=60).mean()[idx_num2] *0.5 \
            #     and df['등락률'][idx_num2] < 0:

            ###########이평선 기준 매도############  -> 처참한 결
            # if df['종가'][idx_num2] <= avg_120[idx_num2]:
            #         p_buy = df['종가'][idx_num+1]
            #         p_sell = df['종가'][idx_num2+1]
            #         earn_rate = int(((p_sell - p_buy)/p_buy)*100)
            #         print(date, df.index[idx_num2], code, "sell", earn_rate)


        #######################결과를 csv로 저장##################################
        # f = open('analistics_20200428_b.csv', 'a', newline="", encoding='ANSI')
        # wr = csv.writer(f)
        # wr.writerow(data)
        # f.close()


    except:
        pass





def excute(date, code):  #매수-매도 실행
    buy(date, code)
    # sell(date, code)




# excute('2019.12.12', '095570')

#다중실행 함수
def exec18():
    #date 불러오기
    dates = pd.read_excel('workingdays_201912.xlsx', converters={'영업일': str})
    date1 = dates['영업일']
    # date1 = ['2019.12.02']

    #code 불러오기
    codes = pd.read_excel('코드리스트.xlsx', converters={'종목코드': str})
    code = codes['종목코드']
    # code = ['011790']

    #불러온 date 라인별로 실행
    for date in date1:
        # print(date)
        date = [date]
        input_list = list(itertools.product(date, code))
        parmap.starmap(excute, input_list, pm_pbar=False)


#실제 실행코드
if __name__ == '__main__':
    exec18()