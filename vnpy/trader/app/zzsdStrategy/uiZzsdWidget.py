# encoding: UTF-8
'''
ZZSD模块相关的GUI控制组件
'''


from vnpy.event import Event
from vnpy.trader.vtEvent import *
from vnpy.trader.uiBasicWidget import QtGui, QtCore, QtWidgets, BasicCell
from vnpy.trader.uiKLine import *
from vnpy.trader.uiBasicWidget import *
from collections import OrderedDict
from vnpy.trader.app.zzsdStrategy.zzsdEngine import EVENT_ZZSD_LOG ,EVENT_ZZSD_STRATEGY
from vnpy.trader.app.zzsdStrategy.zzsdBase import DAY_DB_NAME,MINUTE_30_DB_NAME,MINUTE_60_DB_NAME

from vnpy.trader.app.zzsdStrategy.language import text

########################################################################
class ZzsdEngineManager(QtWidgets.QMainWindow):
    signal = QtCore.Signal(type(Event()))
    signal_tick = QtCore.Signal(type(Event()))

    def __init__(self, zzsdEngine, eventEngine, parent=None):
        """Constructor"""
        super(ZzsdEngineManager, self).__init__(parent)

        self.zzsdEngine = zzsdEngine
        self.eventEngine = eventEngine

        self.name = "zzsd"
        self.stopOrderMonitor = None
        self.klineDay = None
        self.klineOpt = None

        self.dayBarData = {}
        self.hourBarData = {}

        self.initUi()
        self.registerEvent()

        self.currrentSymbol = ""



        # 记录日志
        #self.ctaEngine.writeCtaLog(text.CTA_ENGINE_STARTED)

    #----------------------------------------------------------------------
    def initUi(self):
        """初始化界面"""
        self.setWindowTitle(text.ZZSD_STRATEGY)

        ##log
        logMonitor = LogMonitor( self.zzsdEngine ,self.eventEngine)
        widgetLogM, dockLogM = self.createDock(logMonitor, u"log", QtCore.Qt.LeftDockWidgetArea)
        ##合约
        self.marketMonitor = MarketMonitor( None ,self.eventEngine)
        widgetMarketM, dockMarketM = self.createDock(self.marketMonitor, vtText.MARKET_DATA, QtCore.Qt.LeftDockWidgetArea)


        ##条件单
        self.stopOrderMonitor = StopOrderMonitor( None ,self.eventEngine)
        widgetStopOrderM, dockStopOrderM = self.createDock(self.stopOrderMonitor, u"条件单", QtCore.Qt.LeftDockWidgetArea)
        self.updateMonitor()  ##更新一下数据

        self.marketMonitor.cellDoubleClicked.connect(self.symbolSelect)
        self.stopOrderMonitor.cellDoubleClicked.connect(self.symbolSelect)

        ##K线跟随

        self.klineOpt = KLineWidget()
        widgetklineOptM, dockklineOptM = self.createDock(self.klineOpt, u"操作周期线", QtCore.Qt.RightDockWidgetArea)

        self.klineDay = KLineWidget()
        widgetklineDayM, dockklineDayM = self.createDock(self.klineDay, u"周线", QtCore.Qt.RightDockWidgetArea)




    ##显示合约k线
    def symbolSelect(self, row=None,column=None):

        symbol = self.stopOrderMonitor.item(row,0).text()
        print(symbol)
        if symbol:
            self.loadKlineData(symbol)



    def loadKlineData(self,symbol):

        if self.currrentSymbol == symbol:
            return;

        self.currrentSymbol = symbol

        self.loadBar(symbol)

        self.klineDay.clearData()
        self.klineOpt.clearData()

        self.klineOpt.KLtitle.setText(symbol+" opt",size='20pt')
        self.klineOpt.loadDataBarArray(self.hourBarData[symbol].barData)

        self.klineDay.KLtitle.setText(symbol+" day",size='20pt')
        self.klineDay.loadDataBarArray(self.dayBarData[symbol].barData)


    def refreshKline(self,symbol):
        if self.hourBarData.has_key(symbol):
            hourBar = VtBarData()
            hourBar.__dict__ = self.hourBarData[symbol].barData[-1]

            dayBar =  VtBarData()
            dayBar.__dict__ = self.dayBarData[symbol].barData[-1]

            #print(hourBar.__dict__)
            #print(dayBar.__dict__)

            self.klineOpt.onBar(hourBar)
            self.klineDay.onBar(dayBar)


    def loadBar(self, symbol):

        if not self.dayBarData.has_key(symbol):
            dbDayList = self.loadAllBarFromDb(DAY_DB_NAME,symbol)

            self.dayBarData[symbol] = BarManager(dbDayList,symbol,isDay=True)

        if not self.hourBarData.has_key(symbol):
            dbHourList = self.loadAllBarFromDb(MINUTE_60_DB_NAME,symbol)
            self.hourBarData[symbol] =  BarManager(dbHourList,symbol,60)



    #----------------------------------------------------------------------
    def loadAllBarFromDb(self, dbName, collectionName):
        """从数据库中读取Bar数据，startDate是datetime对象"""
        d = {}
        if self.zzsdEngine.client :  ##rpc模式
            barData = self.zzsdEngine.client.mainEngine.dbQuery(dbName, collectionName,d)
        else:
            barData = self.zzsdEngine.mainEngine.dbQuery(dbName, collectionName,d)

        return barData


    def updateMonitor(self):
        ##更新条件单
        varOrders = self.zzsdEngine.getStopOrders()
        self.stopOrderMonitor.updateAllData(varOrders)


    def processTickEvent(self, event):
        """收到事件更新"""
        tick = event.dict_['data']

        if self.dayBarData.has_key(tick.symbol):
            self.dayBarData[tick.symbol].updateTick(tick)

        if self.hourBarData.has_key(tick.symbol):
            self.hourBarData[tick.symbol].updateTick(tick)


        self.refreshKline(tick.symbol)

    #----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.signal.connect(self.updateMonitor)
        self.eventEngine.register(EVENT_ZZSD_STRATEGY+self.name, self.signal.emit)

        self.signal_tick.connect(self.processTickEvent)
        self.eventEngine.register(EVENT_TICK, self.signal_tick.emit)


    #----------------------------------------------------------------------
    def createDock(self, widget, widgetName, widgetArea):
        """创建停靠组件"""
        dock = QtWidgets.QDockWidget(widgetName)
        dock.setWidget(widget)
        dock.setObjectName(widgetName)
        dock.setFeatures(dock.DockWidgetFloatable|dock.DockWidgetMovable)
        self.addDockWidget(widgetArea, dock)
        return widget, dock



########################################################################
class StopOrderMonitor(BasicMonitor):
    """日志监控"""

    #----------------------------------------------------------------------
    def __init__(self, mainEngine, eventEngine, parent=None):
        """Constructor"""
        super(StopOrderMonitor, self).__init__(mainEngine, eventEngine, parent)


        d = OrderedDict()
        d['vtSymbol'] = {'chinese': u"合约", 'cellType':BasicCell}
        d['orderType'] = {'chinese': u"类型", 'cellType':BasicCell}
        d['direction'] = {'chinese':u"方向", 'cellType':BasicCell}
        d['offset'] = {'chinese':u"offset", 'cellType':BasicCell}
        d['price'] = {'chinese':u"价格", 'cellType':BasicCell}
        d['volume'] = {'chinese':u"volume", 'cellType':BasicCell}
        d['status'] = {'chinese':u"status", 'cellType':BasicCell}

        self.setHeaderDict(d)

        self.setFont(BASIC_FONT)
        self.initTable()




########################################################################
class LogMonitor(BasicMonitor):
    """日志监控"""

    #----------------------------------------------------------------------
    def __init__(self, mainEngine, eventEngine, parent=None):
        """Constructor"""
        super(LogMonitor, self).__init__(mainEngine, eventEngine, parent)

        d = OrderedDict()
        d['logTime'] = {'chinese': u"time", 'cellType':BasicCell}
        d['logContent'] = {'chinese': u"content", 'cellType':BasicCell}
        d['gatewayName'] = {'chinese':u"gateway", 'cellType':BasicCell}
        self.setHeaderDict(d)

        self.setEventType(EVENT_ZZSD_LOG)
        self.setFont(BASIC_FONT)
        self.initTable()
        self.registerEvent()


########################################################################
class MarketMonitor(BasicMonitor):
    """市场监控组件"""

    #----------------------------------------------------------------------
    def __init__(self, mainEngine, eventEngine, parent=None):
        """Constructor"""
        super(MarketMonitor, self).__init__(mainEngine, eventEngine, parent)

        # 设置表头有序字典
        d = OrderedDict()
        d['symbol'] = {'chinese':u"合约", 'cellType':BasicCell}
        d['lastPrice'] = {'chinese':u"最新价", 'cellType':BasicCell}
        d['volume'] = {'chinese':u"仓位", 'cellType':BasicCell}

        self.setHeaderDict(d)

        # 设置数据键
        self.setDataKey('symbol')

        # 设置监控事件类型
        self.setEventType(EVENT_TICK)

        # 设置字体
        self.setFont(BASIC_FONT)

        # 设置允许排序
        self.setSorting(True)

        # 初始化表格
        self.initTable()

        # 注册事件监听
        self.registerEvent()



########################################################################
class BarManager(object):
    """
    K线合成器，支持：

    """

    #----------------------------------------------------------------------
    def __init__(self, initBarList, symbol, xmin=None, isDay=False):
        """Constructor"""

        self.symbol = symbol
        self.barData = initBarList          # bar数组

        self.isDay = isDay
        self.dayHasAppend = False

        self.xmin = xmin            # X的值

        self.lastTick = None        # 上一TICK缓存对象


    ##========-----------------------------
    def updateTickDay(self, tick):
        if self.dayHasAppend == False:
            bar_day = {}
            bar_day["vtSymbol"] = tick.vtSymbol
            bar_day["symbol"] = tick.symbol
            bar_day["exchange"] = tick.exchange

            bar_day["open"] = tick.lastPrice
            bar_day["high"] = tick.lastPrice
            bar_day["low"] = tick.lastPrice
            bar_day["datetime"] = tick.datetime

            self.barData.append(bar_day)
            self.dayHasAppend = True

        bar_day = self.barData[-1]

        bar_day["high"] = max(bar_day["high"], tick.lastPrice)
        bar_day["low"] = min(bar_day["low"], tick.lastPrice)

        # 通用更新部分
        bar_day["close"] = tick.lastPrice
        bar_day["openInterest"] = tick.openInterest
        bar_day["volume"] = 10 ##没有用


    def updateTick(self, tick):
        if self.symbol == tick.symbol:
            if self.isDay == True:
                self.updateTickDay(tick)
            else:
                self.updateTickOpt(tick)

    #----------------------------------------------------------------------
    def updateTickOpt(self, tick):
        """TICK更新"""

        optBar = self.barData[-1]
        if optBar["datetime"].hour !=  tick.datetime.hour or optBar["datetime"].day != tick.datetime.day:

            optBar = {}
            optBar["vtSymbol"] = tick.vtSymbol
            optBar["symbol"] = tick.symbol
            optBar["exchange"] = tick.exchange

            optBar["open"] = tick.lastPrice
            optBar["high"] = tick.lastPrice
            optBar["low"] = tick.lastPrice
            optBar["datetime"] = tick.datetime

            self.barData.append(optBar)


        optBar["high"] = max(optBar["high"], tick.lastPrice)
        optBar["low"] = min(optBar["low"], tick.lastPrice)

        # 通用更新部分
        optBar["close"] = tick.lastPrice
        optBar["openInterest"] = tick.openInterest
        optBar["volume"] = 10 ##没有用
