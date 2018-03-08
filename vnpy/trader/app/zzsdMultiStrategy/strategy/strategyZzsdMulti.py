# encoding: UTF-8

"""
一个ATR-RSI指标结合的交易策略，适合用在股指的1分钟和5分钟线上。

注意事项：
1. 作者不对交易盈利做任何保证，策略代码仅供参考
2. 本策略需要用到talib，没有安装的用户请先参考www.vnpy.org上的教程安装
3. 将IF0000_1min.csv用ctaHistoryData.py导入MongoDB后，直接运行本文件即可回测策略

"""

from .sarManager import *
from ..zzsdMultiBase import *
from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtConstant import *
from vnpy.trader.app.zzsdMultiStrategy.zzsdMultiTemplate import (ZzsdMuitiTemplate,
                                                     BarManager, 
                                                     ArrayManager)


########################################################################
class ZzsdMultiStrategy(ZzsdMuitiTemplate):
    """结合ATR和RSI指标的一个分钟线交易策略"""
    className = 'ZzsdMultiStrategy'
    author = u'用Python的交易员'

    # 策略参数
    vtSymbol = ''            #策略处理的当前合约
    strategyCycle = ''       #策略处理的周期  半小时还是1小时
    sarAcceleration = 0.02  #sar加速因子
    sarMaxNum = 0.2         #sa最大值

    trailingPercent = 0.8   # 百分比移动止损
    initDays = 10           # 初始化数据所用的天数
    fixedSize = 1           # 每次交易的数量


    # 策略变量

    intraTradeHigh = 0                  # 移动止损用的持仓期内最高价
    intraTradeLow = 0                   # 移动止损用的持仓期内最低价

    TICK_MODE = 'tick'
    BAR_MODE = 'bar'

    # 参数列表，保存了参数的名称
    paramList = ['name',
                 'className',
                 'author',
                 'vtSymbol',
                 'strategyCycle',
                 'sarAcceleration',
                 'sarMaxNum',
                 'trailingPercent']    

    # 变量列表，保存了变量的名称
    varList = ['inited',
               'trading',
               'pos']

    #----------------------------------------------------------------------
    def __init__(self, ctaEngine, setting):
        """Constructor"""
        super(ZzsdMultiStrategy, self).__init__(ctaEngine, setting)
        
        # 创建K线合成器对象
        if setting['strategyCycle'] == '60min':
            self.bm = BarManager(self.onBar, 60, self.onXminBar)

        if setting['strategyCycle'] == '30min':
            self.bm = BarManager(self.onBar, 30, self.onXminBar)

        if setting['strategyCycle'] == '5min':
            self.bm = BarManager(self.onBar, 5, self.onXminBar)

        self.am = ArrayManager()

        self.weekSarManager = SarManager(name="week")     #周线管理
        self.daySarManager = SarManager(name="day")       #日线sar管理
        self.hourSarManager = SarManager(name="hour")      #小时或者半小时线管理



        self.isMainSymbol = True  ##是否是主力

        self.sarDayValue = 0   #最新sar day值
        self.sarHourValue = 0  #小时或者半小时sar当前值
        self.currentDayBar = None #日线当前bar
        self.currentHourBar = None  #小时或者半小时线 当前bar

        self.mode = self.TICK_MODE    # 回测模式，默认为K线


        
        # 注意策略类中的可变对象属性（通常是list和dict等），在策略初始化时需要重新创建，
        # 否则会出现多个策略实例之间数据共享的情况，有可能导致潜在的策略逻辑错误风险，
        # 策略类中的这些可变对象属性可以选择不写，全都放在__init__下面，写主要是为了阅读
        # 策略时方便（更多是个编程习惯的选择）        

    #----------------------------------------------------------------------
    def onInit(self):
        """初始化策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略初始化' %self.name)

        self.putEvent()

    #----------------------------------------------------------------------
    def onStart(self):
        """启动策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略启动' %self.name)
        self.putEvent()

    #----------------------------------------------------------------------
    def onStop(self):
        """停止策略（必须由用户继承实现）"""
        self.writeCtaLog(u'%s策略停止' %self.name)
        self.putEvent()

    #----------------------------------------------------------------------
    def onTick(self, tick):
        """收到行情TICK推送（必须由用户继承实现）"""

        if self.mode == self.TICK_MODE:
            self.daySarManager.updateTick(tick)
            isReverse = self.hourSarManager.updateTick(tick)
            if isReverse:
                self.handleZzsdStrategy(tick.askPrice1, tick.bidPrice1)


        self.bm.updateTick(tick)

    #----------------------------------------------------------------------
    def onBar(self, bar):
        """收到1min Bar推送（必须由用户继承实现）处理策略"""
        #self.cancelAll()

        #self.sell(self.hourSarManager.lowValue, 23, True)

        if  self.mode == self.BAR_MODE:  ##中途更新，看是否突破
            self.daySarManager.updateMiddleBar(bar)
            self.weekSarManager.updateMiddleBar(bar)
            isReverse = self.hourSarManager.updateMiddleBar(bar)
            if isReverse:

                if bar.date == "20150716":
                    bacd =2

                self.writeCtaLog(u"小时线反转")

                self.handleZzsdStrategy(self.hourSarManager.revSarValue, self.hourSarManager.revSarValue)

        self.bm.updateBar(bar)


        # 发出状态更新事件
        self.putEvent()

    def setMode(self, mode):
        self.mode = mode

    ##每次成交手数
    def setFixSize(self,size):
        self.fixedSize = size



    ##处理策略，触发信号
    def  handleZzsdStrategy(self,   askPrice1, bidPrice1):

        isLong   = False
        isTigger = False ## 是否触发信号
        isWeekReverse = False ##是否和周趋势相反
        ##都是上升旗形
        if  (self.daySarManager.sarType == SarManager.SAR_TYPE_HEAD_DOWN or self.daySarManager.sarType == SarManager.SAR_TYPE_QI_UP) \
            and (self.hourSarManager.sarType == SarManager.SAR_TYPE_HEAD_DOWN or self.hourSarManager.sarType == SarManager.SAR_TYPE_QI_UP):
        #if (self.hourSarManager.sarType == SarManager.SAR_TYPE_HEAD_UP or self.hourSarManager.sarType == SarManager.SAR_TYPE_QI_UP):
            if self.hourSarManager.isLong == True:  ##反转后是上升的
                isTigger = True
                isLong = True


        ##都是下降旗形
        if  (self.daySarManager.sarType == SarManager.SAR_TYPE_HEAD_UP or self.daySarManager.sarType == SarManager.SAR_TYPE_QI_DOWN) \
            and (self.hourSarManager.sarType == SarManager.SAR_TYPE_HEAD_UP or self.hourSarManager.sarType == SarManager.SAR_TYPE_QI_DOWN):
        #if  (self.hourSarManager.sarType == SarManager.SAR_TYPE_HEAD_DOWN or self.hourSarManager.sarType == SarManager.SAR_TYPE_QI_DOWN):
            if self.hourSarManager.isLong == False:  ##反转后是下降的
                isTigger = True
                isLong = False


        ##判断 2个逆势单

        if (self.daySarManager.sarType == SarManager.SAR_TYPE_QI_UP) and self.daySarManager.isLong == False and self.daySarManager.sarBigUpOrDown():
            ##day 大趋势上升旗形，当前下降的
            if (self.hourSarManager.sarType == SarManager.SAR_TYPE_HEAD_UP or self.hourSarManager.sarType == SarManager.SAR_TYPE_QI_DOWN) \
                    and self.hourSarManager.isLong == False:
                ## day 上行旗形 趋势下  操作周期下行，
                self.daySarManager.sarBigUpOrDown()
                isTigger = True
                isLong = False

        if (self.daySarManager.sarType == SarManager.SAR_TYPE_QI_DOWN)  and self.daySarManager.isLong == True and self.daySarManager.sarBigUpOrDown():
            if (self.hourSarManager.sarType == SarManager.SAR_TYPE_HEAD_DOWN or self.hourSarManager.sarType == SarManager.SAR_TYPE_QI_UP) \
                    and self.hourSarManager.isLong == True: ## day 上行旗形 趋势下  操作周期下行，
                isTigger = True
                isLong = True


        ##过程中多头仓位  sar反转后是下降旗形，直接平仓
        if self.getPos()> 0 and self.hourSarManager.optForType == SarManager.SAR_TYPE_QI_DOWN:
            self.cancelAllStop()  ##先撤销
            self.sell(bidPrice1, self.pos, False)


        if self.getPos()< 0 and self.hourSarManager.optForType == SarManager.SAR_TYPE_QI_UP:
            self.cancelAllStop()  ##先撤销
            self.cover(askPrice1, abs(self.pos), False)


        if not isTigger:
            return


        ###判断周趋势  如果周趋势和下单趋势相反，风险敞口降低一半
        if  (self.weekSarManager.sarType == SarManager.SAR_TYPE_HEAD_DOWN or self.weekSarManager.sarType == SarManager.SAR_TYPE_QI_UP):
            if isLong == False:
                isWeekReverse = False


        elif(self.weekSarManager.sarType == SarManager.SAR_TYPE_HEAD_UP or self.weekSarManager.sarType == SarManager.SAR_TYPE_QI_DOWN):
            if isLong == True:
                isWeekReverse = False



        ###调整止损点
        if self.getPos() > 0 and isLong == True:
            self.cancelAllStop()  ##先撤销
            self.sell(self.hourSarManager.lowValue, self.pos, True)

        if self.getPos() < 0 and isLong == False:
            self.cancelAllStop()  ##先撤销
            self.cover(self.hourSarManager.highValue, abs(self.pos), True)


        ##开仓平仓
        if self.isMainSymbol == False:
            ##非主力合约反向开仓立即平仓
            if self.getPos() < 0:
                if isLong == True:
                    self.cancelAllStop()
                    self.cover(askPrice1, abs(self.pos), False)

            if self.getPos() > 0:
                if isLong == False:
                    self.cancelAllStop()
                    self.sell(bidPrice1, self.pos, False)

            return  ##非主力只调整止损



        if self.getPos() == 0:
            if isLong == True:
                self.buy(askPrice1, self.fixedSize, self.hourSarManager.lowValue, False,isWeekReverse)

            if isLong == False:
                self.short(bidPrice1 , self.fixedSize,self.hourSarManager.highValue, False,isWeekReverse)

        if self.getPos() > 0:
            if isLong == True: ##加仓
                ##判断止损点是否比上一次开仓价高，高就开仓
                if self.isScaleIn(self.hourSarManager.lowValue,DIRECTION_LONG) == True:
                    self.buy(askPrice1 , self.fixedSize,self.hourSarManager.lowValue, False, isWeekReverse)

            if isLong == False:
                self.sell(bidPrice1, self.pos, False)
                self.short(bidPrice1, self.fixedSize,self.hourSarManager.highValue, False, isWeekReverse)

        if self.getPos() < 0:
            if isLong == True:
                self.cover(askPrice1, abs(self.pos), False)
                self.buy(askPrice1 , self.fixedSize, self.hourSarManager.lowValue, False, isWeekReverse)

            if isLong == False: ##加仓
                if self.isScaleIn(self.hourSarManager.highValue,DIRECTION_SHORT) == True:
                    self.short(bidPrice1, self.fixedSize,self.hourSarManager.highValue, False, isWeekReverse)


    ###更新hour sarmanager
    def onXminBar(self, bar):
        self.writeCtaLog('onXminBar')
        self.hourSarManager.updateBar(bar)




    #----------------------------------------------------------------------
    def setDayBar(self,initData):
        self.daySarManager.initBar(initData)

    def updateDayBar(self, bar):
         self.daySarManager.updateBar(bar)


    def setWeekBar(self,initData):
        self.weekSarManager.initBar(initData)

    def updateWeekBar(self, bar):
         self.weekSarManager.updateBar(bar)


    def setIsMainSymbol(self,isMainSymbol):
        self.isMainSymbol = isMainSymbol


    #----------------------------------------------------------------------
    def setHourBar(self, initData):
        self.hourSarManager.initBar(initData)

    #----------------------------------------------------------------------
    def onOrder(self, order):
        """收到委托变化推送（必须由用户继承实现）"""
        pass

    def getPos(self):
        if self.ctaEngine.engineType == ENGINETYPE_BACKTESTING:
            return self.pos

        if  self.ctaEngine.engineType == ENGINETYPE_TRADING:
            return self.ctaEngine.getPos(self.vtSymbol)

    #----------------------------------------------------------------------
    def onTrade(self, trade):
        # 发出状态更新事件
        ##交易来后设置止损
        if self.getPos() > 0:
             self.cancelAllStop()  ##先撤销
             self.sell(self.hourSarManager.lowValue , self.pos, True)

        if self.getPos() < 0:
            self.cancelAllStop()  ##先撤销
            self.cover(self.hourSarManager.highValue , abs(self.pos), True)


        self.putEvent()

    #----------------------------------------------------------------------
    def onStopOrder(self, so):
        """停止单推送"""
        pass