# encoding: UTF-8

'''
本文件中包含的数据格式和CTA模块通用，用户有必要可以自行添加格式。
'''

from __future__ import division
from datetime import time

# 数据库名称
SETTING_DB_NAME = 'VnTrader_Setting_Db'
TICK_DB_NAME = 'VnTrader_Tick_Db'
DAILY_DB_NAME = 'VnTrader_Daily_Db'
MINUTE_DB_NAME = 'VnTrader_1Min_Db'
MINUTE_3_DB_NAME = 'VnTrader_3Min_Db'
MINUTE_5_DB_NAME = 'VnTrader_5Min_Db'
MINUTE_6_DB_NAME = 'VnTrader_6Min_Db'
MINUTE_7_DB_NAME = 'VnTrader_7Min_Db'
MINUTE_8_DB_NAME = 'VnTrader_8Min_Db'
MINUTE_9_DB_NAME = 'VnTrader_9Min_Db'
MINUTE_10_DB_NAME = 'VnTrader_10Min_Db'
MINUTE_15_DB_NAME = 'VnTrader_15Min_Db'
MINUTE_30_DB_NAME = 'VnTrader_30Min_Db'
MINUTE_60_DB_NAME = 'VnTrader_60Min_Db'

DAY_DB_NAME   = 'VnTrader_Day_Db'
WEEK_DB_NAME  = 'VnTrader_Week_Db'

MINUTE_TO_DB_NAME = {1:MINUTE_DB_NAME, 3: MINUTE_3_DB_NAME, 5: MINUTE_5_DB_NAME, 6: MINUTE_6_DB_NAME, 7: MINUTE_7_DB_NAME, 8: MINUTE_8_DB_NAME, 9: MINUTE_9_DB_NAME, 10: MINUTE_10_DB_NAME, 15: MINUTE_15_DB_NAME, 30: MINUTE_30_DB_NAME, 60: MINUTE_60_DB_NAME, 3600:DAY_DB_NAME}


# 行情记录模块事件
EVENT_DATARECORDER_LOG = 'eDataRecorderLog'     # 行情记录日志更新事件

# CTA引擎中涉及的数据类定义
from vnpy.trader.vtConstant import EMPTY_UNICODE, EMPTY_STRING, EMPTY_FLOAT, EMPTY_INT

########################################################################
#期货交易时间段
MORNING_START = time(9, 0)
MORNING_REST = time(10, 15)
MORNING_RESTART = time(10, 30)
MORNING_END = time(11, 30)
AFTERNOON_START = time(13, 30)
AFTERNOON_END = time(15, 1)
NIGHT_START = time(21, 0)
NIGHT_END = time(2, 31)
