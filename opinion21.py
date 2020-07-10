import pandas as pd
import sqlite3
import parmap
import itertools

def cal_obv(code):
    path = "./temp/" + code + ".db"
    con = sqlite3.connect(path)
    df = pd.read_sql("SELECT * FROM price", con, index_col="날짜").sort_index(ascending=True)

    df['change'] = (df['시가'] - df['종가'])/abs(df['시가']-df['종가'])
    df['base'] = df['거래량'] * df['change']
    df['OBV'] = df['base'].cumsum()

    obv = df['OBV']

    return obv

def get_index(market):
    path = "./temp/index.db"
    con = sqlite3.connect(path)
    data = pd.read_sql("select * from " + market, con).set_index(['날짜'])

    return data

# get_index('KOSPI', '2020.01.30')


def cal_op21(code, date):

    try:
        path = "./temp/" + code + ".db"
        con = sqlite3.connect(path)

        price = pd.read_sql("select * from price", con, index_col="날짜").sort_index(ascending=True)
        infos = pd.read_sql("select * from infos", con)

        date_idx = price.index.tolist()
        idx_num = date_idx.index(date)

        name = infos['종목명'][0]
        eps = infos['EPS'][0]
        market = infos['시장구분'][0]
        # index = get_index(market).sort_index(ascending=True)
        # index_dt = index.index.tolist()
        # idx_dt_num = index_dt.index(date)
        # index_avg = index['체결가'].rolling(window=5).mean()


        #심리적 저점 기준점 구하기

        low_p_240 = price['종가'][idx_num-240:idx_num].sort_values(ascending=True)[0]   #당일을 포함하지 않는다 (포함시 idx_num +1 까지로 바꿔야함)
        low_d_240 = date_idx.index(price['종가'][idx_num-240:idx_num].sort_values(ascending=True).index[0])
        low_p_20 = price['종가'][idx_num-240:idx_num].sort_values(ascending=True)[1]
        low_d_20 = date_idx.index(price['종가'][idx_num-240:idx_num].sort_values(ascending=True).index[1])


        # low_p_240 = price['종가'][idx_num-240:idx_num].min()   #당일을 포함하지 않는다 (포함시 idx_num +1 까지로 바꿔야함)
        # low_d_240 = date_idx.index(price['종가'][idx_num-240:idx_num].idxmin())
        # low_p_20 = price['종가'][idx_num-20:idx_num].min()
        # low_d_20 = date_idx.index(price['종가'][idx_num-20:idx_num].idxmin())

        #주가평균선 구하기
        avg_5 = price['종가'].rolling(window=5).mean()
        avg_20 = price['종가'].rolling(window=20).mean()
        avg_60 = price['종가'].rolling(window=60).mean()
        avg_120 = price['종가'].rolling(window=120).mean()
        avg_240 = price['종가'].rolling(window=240).mean()


        #심리적 저점 = low_p_240 보다는 위에, low_p_20 보다는 아래에

        #y = ax + b

        present_p = price['종가'].iloc[idx_num]

        past_p = price['종가'].iloc[idx_num -1]
        avg_vol = price['거래량'].rolling(window=20).mean()


        if (low_d_20 - low_d_240) == 0 :
            psy_low = low_p_240
            psy_high = int(psy_low * 1.3)

        else:
            a = (low_p_20 - low_p_240)/(low_d_20 - low_d_240)
            b = low_p_20 - (a * low_d_20)

            psy_low = int((a * idx_num) + b)
            psy_high = int(psy_low * 1.3)

        change = int(((present_p - past_p)/present_p)*100)


        avg_60 = price['종가'].rolling(window=60).mean()
        std_60 = price['종가'].rolling(window=60).std()
        bollin_plus = avg_60 + std_60 * 2
        bollin_minus = avg_60 - std_60 * 2


        # obv = cal_obv(code)

        # box = int(((price['종가'][idx_num - 60: idx_num].max() - price['종가'][idx_num - 60: idx_num].min()) / price['종가'][
        #                                                                                                    idx_num - 60: idx_num].min()) * 100)

        price['change'] = ((price['전일비'] /price['종가']) *100)
        box_during = price['change'][idx_num -60 : idx_num]
        box_max = box_during.max()

        mkt_vol = int(present_p*infos['상장주식수']/100000000)

        #매수판단
        #             and present_p > price['고가'][idx_num-5:idx_num].max() \



        # if box <= 5 \
        if present_p > price['종가'][idx_num - 20: idx_num].max() \
            and price['거래량'].iloc[idx_num] >= avg_vol.iloc[idx_num] * 2 \
            and (present_p >= psy_low and present_p <= psy_high) \
            and a > 0 \
            and mkt_vol < 100000000000 \
                :

        # if eps > 0 \
        #     and (present_p >= psy_low and present_p <= psy_high) \
        #     and a > 0 \
        #     and present_p > (price[['시가', '종가']][idx_num - 20:idx_num].max()).max() \
        #     and change >= 3 \
        #     and price['거래량'][idx_num] >= avg_vol[idx_num -1] * 5 \
        #     and avg_240[idx_num -1] < avg_240[idx_num] \
        #         :


            # for i in range(1,60):
            #     idx_num2 = idx_num + i
            #     price2 = price['종가'][idx_num2]
            #     date2 = date_idx[idx_num2]
            #
            #     if buy_p < avg_5[idx_num2] \
            #         and price['거래량'][idx_num2] / avg_vol[idx_num2] >= 2 \
            #             :
            #         price_max2 = price['고가'][idx_num2+1:idx_num2+61].max()
            #         max_date2 = price['고가'][idx_num2+1:idx_num2+61].idxmax()
            #         earn_high2 = int(((price_max2 - buy_p) / buy_p) * 100)
            #
            #         print(earn_high2, code, name, date, date2, max_date2, int(price['거래량'][idx_num2] / avg_vol[idx_num2]))


            # for i in range(1,60):
            #     idx_num2 = idx_num + i
            #     price2 = price['종가'][idx_num2]
            #     date2 = date_idx[idx_num2]
            #
            #     if price2 <= bollin_minus[idx_num2] \
            #         :
            #         buy_p = price['시가'][idx_num2 +1]
            #         price_max2 = price['고가'][idx_num2+1:idx_num2+61].max()
            #         max_date2 = price['고가'][idx_num2+1:idx_num2+61].idxmax()
            #         earn_high2 = int(((price_max2 - buy_p) / buy_p) * 100)
            #
            #         print(earn_high2, code, name, date, date2, max_date2)



            # for i in range(1, 60):
            #     idx_num2 = idx_num + i
            #     af_p_dt = price['저가'][idx_num:idx_num+60].idxmin()
            #     af_p_dt_num = date_idx.index(af_p_dt)
            #
            #
            #     if idx_num2 == af_p_dt_num \
            #             :
            #         date2 = date_idx[idx_num2]
            #         buy_p = price['시가'][idx_num2 + 1]
            #         price_max2 = price['고가'][idx_num2 + 1:idx_num2 + 61].max()
            #         max_date2 = price['고가'][idx_num2 + 1:idx_num2 + 61].idxmax()
            #         earn_high2 = int(((price_max2 - buy_p) / buy_p) * 100)
            #
            #         print(earn_high2, code, name, date, date2, max_date2)


            # for i in range(1, 20):
            #     idx_num2 = idx_num + i
            #     date2 = date_idx[idx_num2]
            #
            #     if present_p > (price[['시가', '종가']][idx_num2 - 20:idx_num2].max()).max() \
            #         and change >= 3 \
            #         and price['거래량'][idx_num2] >= avg_vol[idx_num2 -1] * 3 \
            #         :
            #
            #         buy_p = price['시가'][idx_num2 + 1]
            #         price_max2 = price['고가'][idx_num2 + 1:idx_num2 + 61].max()
            #         max_date2 = price['고가'][idx_num2 + 1:idx_num2 + 61].idxmax()
            #         earn_high2 = int(((price_max2 - buy_p) / buy_p) * 100)
            #
            #         print(earn_high2, code, name, date, date2, max_date2)

            buy_p = price['종가'].iloc[idx_num +1]

            df_after = price[idx_num + 2:idx_num + 62]  # 40일 이내로 한정

            price_max = df_after['고가'].max()  # 222 최고가(고가기준)
            max_date = df_after['고가'].idxmax()  # 최고가 날자
            earn_high = int(((price_max - buy_p) / buy_p) * 100)  # 최고가(종가기준) 달성시 수익률

            # mkt_vol = int(present_p*infos['상장주식수']/100000000)
            # move_vol = int(infos['유동주식수']*present_p/100000000)
            # ratio = int((move_vol/mkt_vol)*100)
            # candle = int(((price['고가'][idx_num] - price['저가'][idx_num]) / price['저가'][idx_num])*100)


            print(earn_high, code, name, date, max_date)

    except:
        # print(code, date, "에러")
        pass


# cal_op21('095570', '2019.12.02')


if __name__ == "__main__":
    dates = pd.read_excel('workingdays_201901.xlsx')['영업일']
    codes = pd.read_excel('코드리스트2.xlsx', converters={'종목코드':str})['종목코드']

    for date in dates:
        date = [date]
        input_list = list(itertools.product(codes, date))
        parmap.starmap(cal_op21, input_list, pm_pbar=False)