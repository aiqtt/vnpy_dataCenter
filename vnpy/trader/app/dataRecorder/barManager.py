# encoding: UTF-8

"""
线上bar数据的生成，只在线上使用
1.使用tick，生成1min-bar
2.用1min-bar生成其他的xminbar
3.使用hour-bar生成day-bar
"""

import copy
from datetime import datetime, timedelta , time
from vnpy.trader.vtObject import VtTickData, VtBarData
from vnpy.trader.vtFunction import grandMinutes
from .drBase import *


class BarManager(object):
    """
    K线合成器，支持：
    1. 基于Tick合成1分钟K线
    2. 基于1分钟K线合成X分钟K线（X可以是2、3、5、10、15、30、60）
    """

    #----------------------------------------------------------------------
    def __init__(self, onBar, onMyXminBar, onDayBar):
        """Constructor"""
        self.bar = None             # 1分钟K线对象
        #需要生成的bar种类
        self.xmins = (3, 5, 6, 7, 8, 9, 10, 15, 30, 60)

        #参数是drEngine的回调函数
        self.onBarCBack = onBar          # 1分钟K线回调函数
        self.onMyXminBarCBack = onMyXminBar     #x分钟k线的回调
        self.onDayBarCBack = onDayBar       #日线的回调

        self.lastTick = None        # 上一TICK缓存对象

        self.myXminBar = {}
        self.myXminBar[3] = None
        self.myXminBar[5] = None
        self.myXminBar[6] = None
        self.myXminBar[7] = None
        self.myXminBar[8] = None
        self.myXminBar[9] = None
        self.myXminBar[10] = None
        self.myXminBar[15] = None
        self.myXminBar[30] = None
        self.myXminBar[60] = None

        #self.DAY_END = time(15, 00)
        #self.NIGHT_START = time(20, 57)      # 夜盘启动和停止时间


    #----------------------------------------------------------------------
    def onBar(self, bar):
        """分钟线更新"""
        dicta = bar.__dict__
        try:
            del dicta['gatewayName']
            del dicta['rawData']
        except Exception, e:
            print e

        self.onBarCBack(bar)

    #----------------------------------------------------------------------
    def onXBar(self, xmin, bar):
        """X分钟线更新"""
        vtSymbol = bar.vtSymbol
        dicta = bar.__dict__
        del dicta['gatewayName']
        del dicta['rawData']

        self.onMyXminBarCBack(xmin, bar)
        if xmin == 60:
            #生成小时线的时候去判断日线
            if bar.datetime.hour == 14:
                self.onDayBarCBack(bar)


    #----------------------------------------------------------------------
    def updateTick(self, tick):
        """TICK更新"""
        newMinute = False   # 默认不是新的一分钟

        # 小时线生成----需要补全小时线的判断
        # 说明：
        #1.判断59min-bar的数据缺失，强制生成59的bar(中午进程不断，13的半小时跟上午11点的半小时会合并，要去掉)
        #3.bar的hour和tick的hour不同，表示进入了下一小时，这时bar的minute不是59，证明59min-bar缺失了
        hour_not_end = ((self.bar != None) and
                        (tick.datetime.hour != 13) and
                        (self.bar.datetime.hour != tick.datetime.hour) and (self.bar.datetime.minute != 59))

        if hour_not_end:
            # 生成上一分钟K线的时间戳
            self.bar.datetime = self.bar.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
            self.bar.date = self.bar.datetime.strftime('%Y%m%d')
            self.bar.time = self.bar.datetime.strftime('%H:%M:%S.%f')

            not_end_bar = copy.deepcopy(self.bar)
            # 推送已经结束的上一分钟K线
            self.onBar(self.bar)

            #复制上一个bar来生成59min-bar
            not_end_bar.datetime = not_end_bar.datetime.replace(minute=59)
            not_end_bar.time = not_end_bar.datetime.strftime('%H:%M:%S.%f')
            #推送补的59min的bar
            self.onBar(not_end_bar)

            #恢复正常流程
            self.bar = None

        # 尚未创建对象
        if not self.bar:
            self.bar = VtBarData()
            newMinute = True
        # 新的一分钟
        elif (self.bar.datetime.minute != tick.datetime.minute):
            # 生成上一分钟K线的时间戳
            self.bar.datetime = self.bar.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
            self.bar.date = self.bar.datetime.strftime('%Y%m%d')
            self.bar.time = self.bar.datetime.strftime('%H:%M:%S.%f')

            # 推送已经结束的上一分钟K线
            self.onBar(self.bar)

            # 创建新的K线对象
            self.bar = VtBarData()
            newMinute = True

        # 初始化新一分钟的K线数据
        if newMinute:
            self.bar.vtSymbol = tick.vtSymbol
            self.bar.symbol = tick.symbol
            self.bar.exchange = tick.exchange
            #self.bar.TradingDay = tick.TradingDay

            self.bar.open = tick.lastPrice
            self.bar.high = tick.lastPrice
            self.bar.low = tick.lastPrice
        # 累加更新老一分钟的K线数据
        else:
            self.bar.high = max(self.bar.high, tick.lastPrice)
            self.bar.low = min(self.bar.low, tick.lastPrice)

        # 通用更新部分
        self.bar.close = tick.lastPrice
        self.bar.datetime = tick.datetime
        self.bar.TradingDay = tick.TradingDay
        self.bar.openInterest = tick.openInterest

        if self.lastTick:
            currntVolume = int(tick.volume)
            if currntVolume == 0:  # 晚上9点或早上，即开市的第一个tick的volume
                self.bar.volume = 0
            elif int(tick.volume) < int(self.lastTick.volume):
                # 开市的情况下，lastTick是上一日的，因此判断是负数的话不用lastTick
                self.bar.volume = int(tick.volume)
            else :
                self.bar.volume += (int(tick.volume) - int(self.lastTick.volume)) # 当前K线内的成交量

        # 缓存Tick
        self.lastTick = tick

    #----------------------------------------------------------------------
    #需要生成那些分钟bar，从setFileMode的参数mins中取得
    def updateBar(self, bar):

        #for min in self.myXminBar:
        for min in self.xmins:
            self.updateXminBar(min, bar)

    #----------------------------------------------------------------------
    #x分钟K线更新
    def updateXminBar(self, minute, bar):

        # 尚未创建对象
        if not self.myXminBar[minute]:
            self.myXminBar[minute] = VtBarData()

            self.myXminBar[minute].vtSymbol = bar.vtSymbol
            self.myXminBar[minute].symbol = bar.symbol
            self.myXminBar[minute].exchange = bar.exchange
            self.myXminBar[minute].TradingDay = bar.TradingDay

            self.myXminBar[minute].open = bar.open
            self.myXminBar[minute].high = bar.high
            self.myXminBar[minute].low = bar.low

            self.myXminBar[minute].datetime = bar.datetime
        # 累加老K线
        else:
            self.myXminBar[minute].high = max(self.myXminBar[minute].high, bar.high)
            self.myXminBar[minute].low = min(self.myXminBar[minute].low, bar.low)

        # 通用部分
        self.myXminBar[minute].close = bar.close
        #self.myXminBar[minute].datetime = bar.datetime
        self.myXminBar[minute].openInterest = bar.openInterest
        self.myXminBar[minute].volume += int(bar.volume)

        # X分钟已经走完
        #上午10点15休盘15分钟要生成30min的bar
        ten_rest_10min = (minute == 10) and (bar.datetime.hour == 10 and bar.datetime.minute == 14)
        ten_rest_30min = (minute == 30) and (bar.datetime.hour == 10 and bar.datetime.minute == 14)

        day_end = (bar.datetime.hour == 14 and bar.datetime.minute == 59)
        #BAR周期判断，整除代表当前bar结束，推送去保存，开始计算下一个bar
        mins_flag = (bar.datetime.hour * 60 + bar.datetime.minute + 1) % minute
        #if not (bar.datetime.minute + 1) % minute:  # 可以用X整除
        if (not mins_flag) or ten_rest_10min or ten_rest_30min or day_end:
            # 生成上一X分钟K线的时间戳
            self.myXminBar[minute].datetime = self.myXminBar[minute].datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
            self.myXminBar[minute].date = self.myXminBar[minute].datetime.strftime('%Y%m%d')
            self.myXminBar[minute].time = self.myXminBar[minute].datetime.strftime('%H:%M:%S.%f')

            # 推送
            self.onXBar(minute, self.myXminBar[minute])

            # 清空老K线缓存对象
            self.myXminBar[minute] = None


# ----------------------------------------------------------------------
    #用给定的bar，只进行bar的累计处理，一般在外部通过在循环中调用
    def updateGivingBar(self, bar):
        #givingBar这个变量只在本函数内使用
        if not self.givingBar:
            self.givingBar = VtBarData()

            self.givingBar.vtSymbol = bar.vtSymbol
            self.givingBar.symbol = bar.symbol
            self.givingBar.exchange = bar.exchange
            self.givingBar.TradingDay = bar.TradingDay

            self.givingBar.open = bar.open
            self.givingBar.high = bar.high
            self.givingBar.low = bar.low

            self.givingBar.datetime = bar.datetime
        else:
            self.givingBar.high = max(self.givingBar.high, bar.high)
            self.givingBar.low = min(self.givingBar.low, bar.low)

        # 通用部分
        self.givingBar.close = bar.close
        self.givingBar.openInterest = bar.openInterest
        self.givingBar.volume += int(bar.volume)



