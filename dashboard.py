import xlwt
import pandas as pd
import numpy as np
import cytoolz.curried
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc

pd.get_option("display.max_rows")
pd.get_option("display.max_columns")
pd.set_option("display.max_rows", 1000)
pd.set_option("display.max_columns", 1000)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.unicode.east_asian_width', True)
def mymerge(x, y):
    m = pd.merge(x, y, on=[i for i in list(x) if i in list(y)], how='outer')
    return m

conn_lite = conn_local_lite('summary.sqlite3')
cur_lite = conn_lite.cursor()

normal = sqlc.selectAll('ifrs前後-綜合損益表-一般業', conn_lite).replace('None', np.nan)
hold = sqlc.selectAll('ifrs前後-綜合損益表-金控業', conn_lite).rename(columns={'利息淨收益':'營業收入'}).replace('None', np.nan)
bank = sqlc.selectAll('ifrs前後-綜合損益表-銀行業', conn_lite).rename(columns={'利息淨收益':'營業收入', '本期綜合損益總額（稅後）':'本期綜合損益總額'}).replace('None', np.nan)
security = sqlc.selectAll('ifrs前後-綜合損益表-證券業', conn_lite).rename(columns={'收益':'營業收入'}).replace('None', np.nan)
insurance = sqlc.selectAll('ifrs前後-綜合損益表-保險業', conn_lite).replace('None', np.nan)
other = sqlc.selectAll('ifrs前後-綜合損益表-其他業', conn_lite).rename(columns={'收入':'營業收入'}).replace('None', np.nan)

inc = cytoolz.reduce(mymerge, [normal, hold, bank, security, insurance, other])
inc.dtypes
floatClumns = [col for col in list(inc) if col not in ['年', '季', '公司代號', '公司名稱']]
inc[floatClumns] = inc[floatClumns].astype(float)

def change1(df):
    df0 = df[[x for x in list(df) if df[x].dtype == 'O']]
    df1 = df[[x for x in list(df) if df[x].dtype != 'O']]
    a0 = np.array(df0)
    a1 = np.array(df1)
    v = np.vstack((a1[0], a1[1:] - a1[0:len(df) - 1]))
    h = np.hstack((a0, v))
    return pd.DataFrame(h, columns=list(df0)+list(df1))


inc=inc.groupby(['公司代號', '年']).apply(change1).reset_index(drop=True)
inc[floatClumns]=inc[floatClumns].rolling(window=4).sum()
# inc[col1]=inc[col1].rolling(window=4).sum()
inc['grow'] = inc.groupby(['公司代號'])['本期綜合損益總額'].pct_change(1)
inc['grow.ma'] = inc.groupby(['公司代號'])['grow'].apply(pd.rolling_mean, 24)*4
inc['本期綜合損益總額.wma'] = inc.groupby(['公司代號'])['本期綜合損益總額'].apply(pd.ewma, com=19)*4
inc['本期綜合損益總額.ma'] = inc.groupby('公司代號')['本期綜合損益總額'].apply(pd.rolling_mean, 12)*4
inc['營業收入(百萬元)'] = inc['營業收入']/1000
# inc = inc[['年',  '季',  '公司代號', '公司名稱', '營業收入(百萬元)', '本期淨利（淨損）', '本期綜合損益總額', '基本每股盈餘（元）', '本期綜合損益總額.wma', '本期綜合損益總額.ma', 'grow', 'grow.ma']]
# inc = inc.sort_values(['公司代號', '年', '季'])

conn_lite = conn_local_lite('TEJ.sqlite3')
cur_lite = conn_lite.cursor()
tse = pd.read_sql_query("SELECT * FROM tse_ch", conn_lite)
tse = tse[['公司代號', '產業別']]

conn_lite = conn_local_lite('summary.sqlite3')
cur_lite = conn_lite.cursor()
operation = pd.read_sql_query("SELECT * FROM 營益分析", conn_lite).drop(['營業收入(百萬元)'], axis=1)
finance = pd.read_sql_query("SELECT * FROM 財務分析", conn_lite)
df = mymerge(inc, operation)
df = mymerge(df, finance)
df = df.sort_values(['公司代號', '年', '季'])
df = df.groupby('公司代號').last().reset_index()
df = df.drop(['年', '季', '公司簡稱'], 1)

conn_pg = conn_local_pg('tse')
cur_pg = conn_pg.cursor()
close = pd.read_sql_query('SELECT 證券代號, 年月日, 收盤價 FROM "每日收盤行情(全部(不含權證、牛熊證))"', conn_pg)
close = close.rename(columns={'證券代號': '公司代號'})
close = close.sort_values(['公司代號', '年月日'])
close = close.groupby('公司代號').last().reset_index()
value = pd.read_sql_query("SELECT * FROM 個股日本益比、殖利率及股價淨值比", conn_pg)
value = value.rename(columns={'證券代號': '公司代號'})
value['本益比'] = value['本益比'].replace('-', np.nan)  # pe is '-' when pe < 0
value['股價淨值比'] = value['股價淨值比'].replace('-', np.nan)
value = value.groupby('公司代號').last().reset_index() #last when ascending, first when descending
value = value.drop(['年月日', '證券名稱'], 1)

m = mymerge(df, value)
m = mymerge(m, close)
m = mymerge(m, tse)
m['流通在外股數'] = m['本期淨利（淨損）']*1000/m['基本每股盈餘（元）']
m = m.replace('--', np.nan)
m['收盤價'] = m['收盤價'].astype(float)
m['本益比'] = m['本益比'].astype(float)
m['股價淨值比'] = m['股價淨值比'].astype(float)
m['殖利率(%)'] = m['殖利率(%)'].astype(float)
# m['殖利率'] = m['殖利率'].astype(float)
m['市值'] = m['流通在外股數']*m['收盤價']
m['pe.ave'] = m['市值']/m['本期綜合損益總額.wma']/1000
m['ave'] = (m['本益比']+100/m['殖利率(%)']+8*m['股價淨值比'])/3

m = m[['公司代號', '公司名稱', '產業別', '本期淨利（淨損）', '本期綜合損益總額', '基本每股盈餘（元）', '本期綜合損益總額.wma', 'grow', 'grow.ma',
             '營業收入(百萬元)', '本益比', '殖利率(%)', '股價淨值比', 'ave', 'pe.ave', '權益報酬率(%)', '毛利率(%)', '營業利益率(%)', '稅前純益率(%)',
             '稅後純益率(%)', '負債佔資產比率(%)', '長期資金佔不動產、廠房及設備比率(%)',
             '流動比率(%)', '速動比率(%)', '利息保障倍數(%)', '應收款項週轉率(次)', '平均收現日數', '存貨週轉率(次)', '不動產、廠房及設備週轉率(次)',
             '總資產週轉率(次)', '資產報酬率(%)', '稅前純益佔實收資本比率(%)', '純益率(%)', '每股盈餘(元)', '現金流量比率(%)', '現金流量允當比率(%)',
             '現金再投資比率(%)', '流通在外股數', '市值', '收盤價', '年月日']]


conn_lite = conn_local_lite('mysum.sqlite3')
cur_lite = conn_lite.cursor()
xl = m[(m['營業收入(百萬元)'] > 3000) & (m['本益比'] < 15) & (m['殖利率(%)'] > 5) & (m['股價淨值比'] < 1.5)].sort_values(['ave'])
sql = 'DROP TABLE xlwings'
cur_lite.execute(sql)
xl = xl.reset_index(drop=True)
xl['年月日'] = xl['年月日'].astype(str)
xl.to_sql('xl', conn_lite, index=False)
print('done')

book = xlwt.Workbook()
sheet = book.add_sheet('Sheet1')

cols = list(xl)
for i in cols:
    try:
        xl[i] = xl[i].astype(float)
    except:
        pass

for j in range(len(cols)):
    sheet.write(0, j, cols[j])
ncol = len(cols)
nrow = len(xl)
xl = xl.fillna('')
for i in range(nrow):
    for j in range(ncol):
        sheet.write(i+1, j, xl.iloc[i,j])

book.save('/home/david/Documents/dashboard1.ods')

# Book('/home/david/Desktop/dashboard.xlsx').sheets['new'].range(1, 1).options(index=False).value = xl
print('finish')
