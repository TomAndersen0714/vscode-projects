import pandas as pd
import matplotlib.dates as mdts
import datetime as dt
# 月份英文缩写到数字的映射
date_map = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
            'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
            'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}


def date_format(x):
    date = x.split('-')
    if len(date) != 3:
        return x
    else:
        # 取出年月日
        day = date[0]
        month = date[1]
        year = date[2]
        # 对年份加上2000
        year = str(int(year)+2000)
        # 对月份进行转换
        month = date_map.get(month, '0')
    return year+month+day


date = ['31-AUG-11', '10-OCT-11']
date = [date_format(x) for x in date]
# 字符串转换成日期的两种方式
# 1 pandas.Timestamp,2 datetime.datetime.date
date1 = [pd.to_datetime(x, format='%Y%m%d') for x in date]
date2 = [dt.datetime.strptime(str(x), '%Y%m%d').date() for x in date]
print(date)
print(date1)
print(date2)
