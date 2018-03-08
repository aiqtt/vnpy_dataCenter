# encoding: UTF-8

"""
数据转换常用的一些函数
"""

import json
from datetime import datetime, time, timedelta
import calendar
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure
import traceback
from vnpy.trader.vtFunction import getJsonPath
from vnpy.trader.vtObject import VtBarData, VtTickData


NIGHT_START = time(21, 0)
NIGHT_END = time(2, 30)
DAY_OPEN = time(9, 0)
DAY_END = time(15, 0, 1)

DT_SETTING = {}

#加载配置
def loadSetting():
    settingFileName = 'DT_setting.json'
    settingFilePath = getJsonPath(settingFileName, __file__)
    with open(settingFilePath) as f:
        DT_SETTING = json.load(f)
    return DT_SETTING

#取得服务器mongo数据库连接
def getServerMongoConnect(settings):
    #loadSetting()
    dbClient = None
    try:
        dbClient = MongoClient("mongodb://%s:%s@%s" % (settings['server_mongodb_user'], settings['server_mongodb_pwd'], settings['server_mongodb_ip']))
        # 调用server_info查询服务器状态，防止服务器异常并未连接成功
        dbClient.server_info()
    except ConnectionFailure:
        raise Exception("mongo connect error...")
    return dbClient

#取得本地mongo数据库连接
def getLocalMongoConnect(settings):
    dbClient = None
    try:
        # 设置MongoDB操作的超时时间为0.5秒
        dbClient = MongoClient(settings['local_mongodb_ip'], settings['local_mongodb_port'], connectTimeoutMS=500)
        # 调用server_info查询服务器状态，防止服务器异常并未连接成功
        dbClient.server_info()
    except ConnectionFailure:
        raise Exception("mongo connect error...")
    return dbClient


# ----------------------------------------------------------------------
#把文件数据的一行转换为bar对象
def fileLineToBar(line):
        bar_data = line.split(',')
        bar = VtBarData()
        bar.symbol = bar_data[0]
        bar.vtSymbol = bar_data[1]
        bar.exchange = bar_data[2]
        bar.open = float(bar_data[3])
        bar.high = float(bar_data[4])
        bar.low = float(bar_data[5])
        bar.close = float(bar_data[6])
        bar.date = bar_data[7]
        bar.time = bar_data[8]
        bar.datetime = datetime.strptime(bar_data[9], "%Y-%m-%d %H:%M:%S")
        bar.volume = int(bar_data[10])
        bar.openInterest = int(bar_data[11])
        bar.TradingDay = bar_data[12]

        return bar


    #------------------------------------------------
    #mongodb中tick库的一行数据转为tick对象
def mongodbRowToTick(row_tick):
        tickData = VtTickData()
        tickData.lastPrice = float(row_tick['lastPrice'])
        tickData.date = row_tick['date']
        tickData.time = row_tick['time']
        tickData.datetime = datetime.strptime(' '.join([row_tick['date'], row_tick['time']]), '%Y%m%d %H:%M:%S.%f')
        tickData.volume = int(row_tick['volume'])
        tickData.vtSymbol = row_tick['vtSymbol']
        tickData.symbol = row_tick['symbol']
        tickData.exchange = row_tick['exchange']
        tickData.TradingDay = row_tick['TradingDay']
        tickData.openInterest = int(row_tick['openInterest'])

        return tickData

    #------------------------------------------------
    #mongodb中bar库的一行数据转为bar对象
def mongodbRowToBar(row_bar):
    bar = VtBarData()

    bar.symbol = row_bar['symbol']
    bar.vtSymbol = row_bar['symbol']
    bar.exchange = row_bar['exchange']
    bar.open = float(row_bar['open'])
    bar.high = float(row_bar['high'])
    bar.low = float(row_bar['low'])
    bar.close = float(row_bar['close'])
    bar.date = row_bar['date']
    bar.time = row_bar['time']
    #bar.datetime = datetime.strptime(row_bar['datetime'], "%Y-%m-%d %H:%M:%S")
    bar.datetime = row_bar['datetime']
    bar.volume = int(row_bar['volume'])
    bar.openInterest = int(row_bar['openInterest'])
    bar.TradingDay = row_bar['TradingDay']

    return bar



# --------------------------------------------------------------
"""
1.把老数据中的.00000换成0.0
2.把多余的小数位去掉，只留一位
"""
def convertFloatZero(floatStr):

    flaotArr = floatStr.split('.')
    if len(flaotArr) > 1:
        #有整数部分
        if len(flaotArr[1]) > 1:
            floatStr1 = flaotArr[1]
            return float('.'.join((flaotArr[0], floatStr1[0])))
        else:
            return float(floatStr)
    else:
        #无整数部分
        #if floatStr == '.00000':
        return 0.0


# --------------------------------------------------------------
#根据时间过滤脏数据，开市时间内的，返回true
def isDirtyData(tick):
    dt = datetime.strptime(tick.time, "%H:%M:%S.%f").time()
    if ((NIGHT_END < dt < DAY_OPEN) or (DAY_END < dt < NIGHT_START)):
        return False
    else:
        return True

#检查是不是夜盘时间
def check_night(tick_datetime):
    if tick_datetime.time() >= NIGHT_START:
        return True

    return False

#判断周五的夜盘
def check_friday(tick_datetime):
    if tick_datetime.weekday() == calendar.FRIDAY:
        return True

    return  False

#取得下一个工作日，跳过周末
def getNextWorkday(day):
    if day.weekday() == calendar.FRIDAY:
        return day + timedelta(days=3)
    else:
        return day + timedelta(days=1)

# ----------------------------------------------------------------------
# 返回字符串形式的日期，如20171229
def todayDateStr():
    now_time = datetime.now()
    today_str = ''
    today_str += str(now_time.year)
    today_str += str(now_time.month)
    today_str += str(now_time.day)

    return today_str

#根据tick的datetime判断交易日
#夜盘的交易日是第二天，周五夜盘的交易日是下周一
"""
def getTradingDayByDatetime(tick_datetime):
    if check_night(tick_datetime):
        pass
        #是夜盘，判断周几
        #if check_friday(tick_datetime)
    else:
        #不是夜盘，返回当天日期
        return todayDateStr()
"""

#跳过休市时间,参数是tick或bar的datetime
#目前只处理夜盘当晚结束的情况
def skipCloseTime(time_find, night_end):
    #夜盘结束时间，要根据不同品种来修改
    #if time_find.time() == time(23, 0, 1):
    if time_find.time() == night_end:
        next_workday = getNextWorkday(time_find)
        time_find = datetime(time_find.year, time_find.month, next_workday.day, 9)
        time_find = datetime(time_find.year, time_find.month, time_find.day, 10, 30)
    elif time_find.time() == time(11, 30, 1):
        time_find = datetime(time_find.year, time_find.month, time_find.day, 13, 30)
    elif time_find.time() == time(15, 0, 1):
        time_find = datetime(time_find.year, time_find.month, time_find.day, 21)

    return time_find


#写日志
def writeLog():
    pass