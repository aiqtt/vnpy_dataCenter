# encoding: UTF-8

"""
bar数据的生成
1.使用tick，生成1min-bar
2.用1min-bar生成其他的xminbar
3.使用hour-bar生成day-bar
4.使用workMode来处理两种保存数据的处理,历史数据存文件，实时数据村db
"""

import copy
from datetime import datetime, timedelta , time
from vnpy.trader.vtObject import VtTickData, VtBarData
from vnpy.trader.vtFunction import grandMinutes
from vnpy.trader.app.dataRecorder.drBase import *



class FileBarManager(object):
    """
    K线合成器，支持：
    1. 基于Tick合成1分钟K线
    2. 基于1分钟K线合成X分钟K线（X可以是2、3、5、10、15、30、60）
    """

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""
        self.bar = None             # 1分钟K线对象
        self.dayBar = None
        self.givingBar = None       #只在updateGivingBar函数中使用

        self.file_1min = None
        self.file_3min = None
        self.file_5min = None
        self.file_6min = None
        self.file_7min = None
        self.file_8min = None
        self.file_9min = None
        self.file_10min = None
        self.file_15min = None
        self.file_30min = None
        self.file_60min = None
        self.file_day   = None

        self.lastTick = None        # 上一TICK缓存对象
        self.myXminLastBar = {}         # 上一1min-bar的缓存
        self.myXminLastBar[3] = None
        self.myXminLastBar[5] = None
        self.myXminLastBar[6] = None
        self.myXminLastBar[7] = None
        self.myXminLastBar[8] = None
        self.myXminLastBar[9] = None
        self.myXminLastBar[10] = None
        self.myXminLastBar[15] = None
        self.myXminLastBar[30] = None
        self.myXminLastBar[60] = None


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

        #setFileMode方法独有的设置
        self.xmins = None
        self.xmins_inday = 'inday'
        self.xmins_normal = 'normal'
        self.xmins_all = 'all'

        #self.DAY_END = time(15, 00)
        #self.NIGHT_START = time(20, 57)      # 夜盘启动和停止时间

    #----------------------------------------------------------------------
    #mins表示需要生成那些分钟bar
    def setFileMode(self, mins, file_1min=None, file_3min=None, file_5min=None, file_6min=None, file_7min=None, file_8min=None, file_9min=None, file_10min=None, file_15min=None, file_30min=None, file_60min=None, file_day=None):
        if mins == self.xmins_inday:
            self.xmins = (6, 7, 8, 9)
        elif mins == self.xmins_normal:
            self.xmins = (3, 5, 10, 15, 30, 60)
        else:
            #mins=='all'
            self.xmins = (3, 5, 6, 7, 8, 9, 10, 15, 30, 60)

        self.file_1min = file_1min
        self.file_3min = file_3min
        self.file_5min = file_5min
        self.file_6min = file_6min
        self.file_7min = file_7min
        self.file_8min = file_8min
        self.file_9min = file_9min
        self.file_10min = file_10min
        self.file_15min = file_15min
        self.file_30min = file_30min
        self.file_60min = file_60min
        self.file_day   = file_day

    #----------------------------------------------------------------------
    def onBar(self, bar):
        """分钟线更新"""
        dicta = bar.__dict__
        del dicta['gatewayName']
        del dicta['rawData']

        #if self.file_1min:
        self.file_1min.writerow(dicta)
        self.updateBar(bar)


    #----------------------------------------------------------------------

    def onXBar(self, xmin, bar):
        """X分钟线更新"""
        vtSymbol = bar.vtSymbol
        dicta = bar.__dict__
        del dicta['gatewayName']
        del dicta['rawData']

        if xmin == 3:
            self.file_3min.writerow(dicta)
        if xmin == 5:
            self.file_5min.writerow(dicta)
        if xmin == 6:
            self.file_6min.writerow(dicta)
        if xmin == 7:
            self.file_7min.writerow(dicta)
        if xmin == 8:
            self.file_8min.writerow(dicta)
        if xmin == 9:
            self.file_9min.writerow(dicta)
        if xmin == 10:
            self.file_10min.writerow(dicta)
        if xmin == 15:
            self.file_15min.writerow(dicta)
        if xmin == 30:
            self.file_30min.writerow(dicta)
        if xmin == 60:
            self.file_60min.writerow(dicta)
            self.updateDay(bar)

    #----------------------------------------------------------------------
    def onDayBar(self, bar):
        vtSymbol = bar.vtSymbol
        dicta = bar.__dict__
        del dicta['gatewayName']
        del dicta['rawData']

        self.file_day.writerow(dicta)

    #----------------------------------------------------------------------
    def updateFileTick(self, tickData):
        # 先对读file的tick做预处理

        if not tickData:
            # 传入的数据是None，表示文件末尾，强制结束当前bar，不然会丢掉最后一天
            if self.bar and self.lastTick:
                self.bar.datetime = self.bar.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
                self.bar.date = self.bar.datetime.strftime('%Y%m%d')
                self.bar.time = self.bar.datetime.strftime('%H:%M:%S.%f')

                # 推送已经结束的上一分钟K线
                self.onBar(self.bar)
            return

        tick = self.loadTickData(tickData)

        # 丢掉15:00收盘的tick
        if tick.datetime.hour == 15:
            return

        if tick.lastPrice == 0.0:  # 过滤当前价为0的。
            return

        dt = time(tick.datetime.hour, tick.datetime.minute)

        # 过滤休市时间的tick
        if ((MORNING_START <= dt < MORNING_REST) or
            (MORNING_RESTART <= dt < MORNING_END) or
            (AFTERNOON_START <= dt < AFTERNOON_END) or
            (dt >= NIGHT_START) or
            (dt < NIGHT_END)):
            self.updateTick(tick)

    #----------------------------------------------------------------------
    def updateTick(self, tick):
        """TICK更新"""
        newMinute = False   # 默认不是新的一分钟

        # 小时线生成----需要补全小时线的判断
        # 说明：
        # 1.判断59min-bar的数据缺失，强制生成整点的bar(中午进程不断，13的半小时跟上午11点的半小时会合并，要去掉)
        # 2.文件模式下，要防止整点结束的夜盘会多一个小时bar，所以tick是9点时不判断
        # 3.bar的hour和tick的hour不同，表示进入了下一小时，这时bar的min不是59，证明59min-bar缺失了
        hour_not_end = ((self.bar is not None) and
                        (tick.datetime.hour != 13) and
                        (tick.datetime.hour != 9) and
                        (self.bar.datetime.hour != tick.datetime.hour) and (self.bar.datetime.minute != 59))

        if hour_not_end:
            # 生成上一分钟K线的时间戳
            self.bar.datetime = self.bar.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
            self.bar.date = self.bar.datetime.strftime('%Y%m%d')
            self.bar.time = self.bar.datetime.strftime('%H:%M:%S.%f')

            not_end_bar = copy.deepcopy(self.bar)
            # 推送已经结束的上一分钟K线
            self.onBar(self.bar)

            # 复制上一个bar来生成59min-bar
            not_end_bar.datetime = not_end_bar.datetime.replace(minute=59)
            not_end_bar.time = not_end_bar.datetime.strftime('%H:%M:%S.%f')
            # 推送补得59min的bar
            self.onBar(not_end_bar)

            # 恢复正常流程
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
        # TradingDay写在这里，把夜盘收盘的tick算在9点里面，避免9点的分钟bar被算到前一天
        self.bar.TradingDay = tick.TradingDay
        self.bar.openInterest = tick.openInterest

        # 当前K线内的成交量
        if self.lastTick:
            currntVolume = int(tick.volume)
            if currntVolume == 0:  # 晚上9点或早上，即开市的第一个tick的volume
                self.bar.volume = 0
            elif int(tick.volume) < int(self.lastTick.volume):
                # 开市的情况下，lastTick是上一日的，因此判断是负数的话不用lastTick
                self.bar.volume = int(tick.volume)
            else :
                #self.bar.volume += (int(tick.volume) - int(self.lastTick.volume))
                self.bar.volume += (tick.volume - self.lastTick.volume)

        # 缓存Tick
        self.lastTick = tick

    # ----------------------------------------------------------------------
    # 需要生成那些分钟bar，从setFileMode的参数mins中取得
    def updateBar(self, bar):
        for min in self.xmins:
            self.updateXminBar(min, bar)

    #----------------------------------------------------------------------
    # x分钟K线更新
    def updateXminBar(self, minute, bar):
        inday_mins = (7, 8, 9)
        # 夜盘结束，要清理5,6,7,8,9分钟的bar
        night_inday_end = (self.myXminBar[minute] is not None) and (bar.datetime.hour == 9 and self.myXminLastBar[minute].datetime.hour != 9)
        if night_inday_end:
            if minute in inday_mins:
                self.myXminBar[minute].datetime = self.myXminBar[minute].datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
                self.myXminBar[minute].date = self.myXminBar[minute].datetime.strftime('%Y%m%d')
                self.myXminBar[minute].time = self.myXminBar[minute].datetime.strftime('%H:%M:%S.%f')
                # 推送
                self.onXBar(minute, self.myXminBar[minute])

                # bar置空，回到正常流程
                self.myXminBar[minute] = None

        # 9点时要判断半点结束夜盘的小时bar
        night_hour_end = (minute == 60) and (self.myXminBar[minute] is not None) and (bar.datetime.hour == 9 and self.myXminLastBar[minute].datetime.hour != 9) and (self.myXminLastBar[minute].datetime.minute < 31)
        if night_hour_end:
            self.myXminBar[minute].datetime = self.myXminBar[minute].datetime.replace(minute=0, second=0,
                                                                                      microsecond=0)  # 将秒和微秒设为0
            self.myXminBar[minute].date = self.myXminBar[minute].datetime.strftime('%Y%m%d')
            self.myXminBar[minute].time = self.myXminBar[minute].datetime.strftime('%H:%M:%S.%f')
            # 推送
            self.onXBar(minute, self.myXminBar[minute])

            # bar置空，回到正常流程
            self.myXminBar[minute] = None

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

        if minute not in inday_mins:
            if bar.datetime.hour == 9 and self.myXminBar[minute].datetime.hour != 9:
                # 9点的bar过来的时候，更新下时间，把夜盘的时间替代掉
                self.myXminBar[minute].datetime = bar.datetime
        self.myXminBar[minute].openInterest = bar.openInterest
        self.myXminBar[minute].volume += int(bar.volume)

        # X分钟已经走完
        # 上午10点15休盘15分钟要生成10min和30min的bar
        ten_rest_10min = (minute == 10) and (bar.datetime.hour == 10 and bar.datetime.minute == 14)
        ten_rest_30min = (minute == 30) and (bar.datetime.hour == 10 and bar.datetime.minute == 14)
        #if not (bar.datetime.minute + 1) % minute:  # 可以用X整除
        day_inday_end = (bar.datetime.hour == 14 and bar.datetime.minute == 59)
        mins_flag = (bar.datetime.hour * 60 + bar.datetime.minute + 1) % minute
        if (not mins_flag) or day_inday_end or ten_rest_10min or ten_rest_30min:
            # 生成上一X分钟K线的时间戳
            self.myXminBar[minute].datetime = self.myXminBar[minute].datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
            self.myXminBar[minute].date = self.myXminBar[minute].datetime.strftime('%Y%m%d')
            self.myXminBar[minute].time = self.myXminBar[minute].datetime.strftime('%H:%M:%S.%f')

            # 推送
            self.onXBar(minute, self.myXminBar[minute])

            # 清空老K线缓存对象
            self.myXminBar[minute] = None

        # 缓存上一个1min-bar
        self.myXminLastBar[minute] = bar


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

# ----------------------------------------------------------------------
    # 天更新
    def updateDay(self,  bar):
        newDay = False

        # 尚未创建对象
        if not self.dayBar:
            self.dayBar = VtBarData()
            newDay = True
        elif self.dayBar.TradingDay != bar.TradingDay:
            print "updateDay:%s" % bar.date
            # 生成上一X分钟K线的时间戳
            self.dayBar.datetime = self.dayBar.datetime.replace(hour=0,minute=0,second=0, microsecond=0)  # 将秒和微秒设为0
            self.dayBar.date = self.dayBar.datetime.strftime('%Y%m%d')
            self.dayBar.time = self.dayBar.datetime.strftime('%H:%M:%S.%f')

            # 推送
            self.onDayBar(self.dayBar)

            # 清空老K线缓存对象
            self.dayBar = VtBarData()
            newDay = True

        if newDay:
            self.dayBar.vtSymbol = bar.vtSymbol
            self.dayBar.symbol = bar.symbol
            self.dayBar.exchange = bar.exchange
            self.dayBar.TradingDay = bar.TradingDay

            self.dayBar.open = bar.open
            self.dayBar.high = bar.high
            self.dayBar.low = bar.low
        else:
            # 累加老K线
            self.dayBar.high = max(self.dayBar.high, bar.high)
            self.dayBar.low = min(self.dayBar.low, bar.low)

        # 通用部分
        self.dayBar.close = bar.close
        self.dayBar.datetime = bar.datetime
        self.dayBar.openInterest = bar.openInterest
        self.dayBar.volume += int(bar.volume)

    # ----------------------------------------------------------------------
    def loadTickData(self, tick):

        tickData = VtTickData()
        #tickData.lastPrice = float(tick['lastPrice'])
        #要处理小数位数，只保留一位小数
        #线上录得数据写文件，比如原始数据是个整数3967，写到文件里的会是3967.000000000001
        tickData.lastPrice = round(float(tick['lastPrice']), 1)
        tickData.date = tick['date']
        tickData.time = tick['time']
        tickData.datetime = datetime.strptime(' '.join([tick['date'], tick['time']]), '%Y%m%d %H:%M:%S.%f')
        tickData.volume = int(tick['volume'])
        tickData.vtSymbol = tick['vtSymbol']
        tickData.symbol = tick['symbol']
        tickData.exchange = tick['exchange']
        tickData.TradingDay = tick['TradingDay']
        #tickData.openInterest = int(tick['openInterest'])
        #防止持仓量可能带小数
        tickData.openInterest = int(float(tick['openInterest']))

        return tickData


    # ----------------------------------------------------------------------
    # 最终结束的时候，清理内存中未推送的最后一个日线bar
    # add by 210180205
    def clearEndBar(self):
        if self.dayBar is not None:
            print "clearEndBar, date:%s" % self.dayBar.datetime
            # 生成上一X分钟K线的时间戳
            self.dayBar.datetime = self.dayBar.datetime.replace(hour=0,minute=0,second=0, microsecond=0)  # 将秒和微秒设为0
            self.dayBar.date = self.dayBar.datetime.strftime('%Y%m%d')
            self.dayBar.time = self.dayBar.datetime.strftime('%H:%M:%S.%f')

            # 推送
            self.onDayBar(self.dayBar)
