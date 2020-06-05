
def get_name(code):   #종목명 구하기
    url = 'https://finance.naver.com/item/main.nhn?code=' + code
    html = requests.get(url).text
    soup = BeautifulSoup(html, "html5lib")
    tags = soup.select("#middle > div.h_company > div.wrap_company > h2 > a")
    tag = tags[0].text
    return tag

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

        if df['거래량'].rolling(window=20).mean()[idx_num - 1] * 5 < df['거래량'][idx_num] \
                and df['종가'][idx_num] > avg_5[idx_num] and df['종가'][idx_num] > avg_60[idx_num] \
                and df['저가'][idx_num] > avg_5[idx_num] \
                and avg_5[idx_num] > avg_20[idx_num] \
                and df['고가'][idx_num] < bollin_plus[idx_num] and df['저가'][idx_num] > bollin_minus[idx_num]:

            print(date, code, get_name(code))


def exec18():
    #date 불러오기
    # dates = pd.read_excel('workingdays_2019.xlsx', converters={'영업일': str})
    # date1 = dates['영업일']
    date1 = ['2020.06.04']    ####테스트용

    #code 불러오기
    codes = pd.read_excel('코드리스트.xlsx', converters={'종목코드': str})
    code = codes['종목코드']
    # code = ['011790']    ####테스트용

    #불러온 date 라인별로 실행
    for date in date1:
        print(date)
        date = [date]
        input_list = list(itertools.product(date, code))
        parmap.starmap(excute, input_list, pm_pbar=False)


if __name__ == '__main__':
    exec18()