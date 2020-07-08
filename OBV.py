import pandas as pd
import sqlite3

def obv(code):
    path = "./temp/" + code + ".db"
    con = sqlite3.connect(path)
    df = pd.read_sql("SELECT * FROM price", con, index_col="날짜").sort_index(ascending=True)

    df['change'] = (df['시가'] - df['종가'])/abs(df['시가']-df['종가'])
    df['base'] = df['거래량'] * df['change']
    df['OBV'] = df['base'].cumsum()

    obv = df['OBV']

    return obv



print(obv('095570'))