import pandas as pd
import sqlite3


def opinion20(date):
    kind_wics = pd.read_csv('wics_kind.csv', converters={'코드': str})['wics']

    con = sqlite3.connect('./Json/infos.db')

    for kind in kind_wics:
        try:
            df2 = pd.read_sql("SELECT * FROM infos WHERE [업종(WICS)] = " + "'" + kind + "'", con, index_col='종목코드')
            codes = df2.index

            df3 = pd.DataFrame(columns={'kind', 'code', 'mkt_vol', 'earn', 'max_date', 'trade_vol', 'buy_price', 'max_price'})

            for code in codes:
                try:
                    path = "./temp/" + str(code) + ".db"
                    con1 = sqlite3.connect(path)

                    price = pd.read_sql("SELECT * FROM price", con1, index_col='날짜').sort_index(ascending=True)
                    infos = pd.read_sql("SELECT * FROM infos", con1)

                    date_idx = price.index.tolist()
                    idx_num = date_idx.index(date)

                    mkt_vol = price['종가'][idx_num] * infos['상장주식수']

                    price['거래대금'] = price['종가'] * price['거래량']


                    trade_vol = price['거래대금'].rolling(window=5).sum()[idx_num]


                    # price_after = price[idx_num+1 : idx_num +21]
                    # price_max = price_after['고가'].max()
                    # max_date = price_after['고가'].idxmax()

                    price_max = price['종가'][idx_num + 20]
                    max_date = price.index[idx_num + 20]



                    earn = int(((price_max - price['종가'][idx_num])/price['종가'][idx_num])*100)

                    df4 = pd.DataFrame({'kind': kind, 'code':code, 'mkt_vol':mkt_vol, 'earn':earn, 'max_date':max_date, 'trade_vol':trade_vol, 'buy_price':price['종가'][idx_num], 'max_price':price_max})
                    df3 = df3.append(df4, ignore_index=True, sort=False)

                except:
                    # print(kind, code, '데이터없음')
                    pass

            df3 = df3.set_index('code')

            min = df3['mkt_vol'].idxmin()
            name = df2['기업명'][min]
            earn = df3['earn'][min]
            print(kind, "/", name, "/", min, "/", date, "/", max_date, "/", int(df3['trade_vol'].sum()/100000000), "/", earn, df3['buy_price'][min], df3['max_price'][min])

        except:
            print(kind, "여긴없데")
            pass


opinion20('2019.01.02')