# encoding: UTF-8

'''
本文件中实现了行情数据记录引擎，用于汇总TICK数据，并生成K线插入数据库。

使用DR_setting.json来配置需要收集的合约，以及主力合约代码。
---取消把数据写文件的操作  modify by JingHui 20171113
---取消载入配置，因为保存所有的行情 modify by JingHui 20171113
'''

import json
import csv
import os
import copy
from collections import OrderedDict
from datetime import datetime, timedelta , time
from time import sleep
from Queue import Queue, Empty
from threading import Thread

from vnpy.event import Event
from vnpy.trader.vtEvent import *
from vnpy.trader.vtFunction import todayDate, getJsonPath, convertFloatMaxValue, getPlatform, todayDateStr
from vnpy.trader.vtObject import VtSubscribeReq, VtLogData, VtBarData, VtTickData

from .drBase import *
from .barManager import BarManager
from .language import text

from vnpy.trader.vtCJsonEncoder import CJsonEncoder



class DrEngine(object):
    """数据记录引擎"""
    
    settingFileName = 'DR_setting.json'
    settingFilePath = getJsonPath(settingFileName, __file__)

    #----------------------------------------------------------------------
    def __init__(self, mainEngine, eventEngine):
        """Constructor"""
        self.mainEngine = mainEngine
        self.eventEngine = eventEngine
        
        # 主力合约代码映射字典，key为具体的合约代码（如IF1604），value为主力合约代码（如IF0000）
        self.activeSymbolDict = {}
        
        # Tick对象字典
        self.tickSymbolSet = set()
        #mongo库中的合约，用于判断新合约的出现
        self.mongoSymbolSet = set()
        
        # K线合成器字典
        self.bmDict = {}
        
        # 配置字典
        self.settingDict = OrderedDict()
        
        # 负责执行数据库插入的单独线程相关
        self.active = False                     # 工作状态
        self.queue = Queue()                    # 队列
        self.thread = Thread(target=self.run)   # 线程

        #tick存为文件的设定
        self.tick_file_path = None
        self.txtActive = False
        self.txtQueue = Queue()
        self.txtThread = Thread(target=self.runTxt)

        # 载入设置，订阅行情
        self.loadSetting()

        #控制夜盘结束写写小时bar
        self.afterCloseActive = False
        self.afterCloseThread = Thread(target=self.runAfterClose)
        #控制夜盘结束写写小时bar
        self.nightCloseActive = False
        self.nightCloseThread = Thread(target=self.runNightClose)

        # 启动数据插入线程
        self.start()
    
        # 注册事件监听
        self.registerEvent()  
    
    #----------------------------------------------------------------------
    def loadSetting(self):
        #加载配置
        with open(self.settingFilePath) as f:
            drSetting = json.load(f)
            #判断os
            os_type = getPlatform()
            if os_type == 'Windows':
                self.tick_file_path = drSetting['windows_tick_file_path']
            else:
                self.tick_file_path = drSetting['linux_tick_file_path']
            if not os.path.exists(self.tick_file_path):
                os.mkdir(self.tick_file_path)
    
    #----------------------------------------------------------------------
    def getSetting(self):
        """获取配置"""
        return self.settingDict, self.activeSymbolDict

    #----------------------------------------------------------------------
    #载入mongo库中现有的合约
    #在mainEngine中dbConnect方法中建立连接后调用
    def loadMongoSymbol(self):
        min_db = self.mainEngine.dbClient[MINUTE_DB_NAME]
        for col_name in min_db.collection_names():
            self.mongoSymbolSet.add(col_name)


    #----------------------------------------------------------------------
    def addContract(self, event):
        """更新合约数据"""
        contract = event.dict_['data']

        if contract.productClass == u'期货':
            #print(contract.symbol)
            req = VtSubscribeReq()
            req.symbol = contract.symbol
            self.mainEngine.subscribe(req, "CTP")

            # 创建BarManager对象
            self.bmDict[contract.symbol] = BarManager(self.onBar, self.onXBar, self.handleDayBar)

            self.tickSymbolSet.add(contract.symbol)

            #判断是否新合约
            if contract.symbol not in self.mongoSymbolSet:
                #新合约要先给数据库表建索datetime索引
                self.mainEngine.dbCreateIndex(TICK_DB_NAME, contract.symbol)
                for xmin in MINUTE_TO_DB_NAME:
                    self.mainEngine.dbCreateIndex(MINUTE_TO_DB_NAME[xmin], contract.symbol)

            # 保存到配置字典中
            if contract.symbol not in self.settingDict:
                d = {
                    'symbol': contract.symbol,
                    'gateway': "CTP",
                    'tick': True
                }
                self.settingDict[contract.symbol] = d

    #----------------------------------------------------------------------
    def procecssTickEvent(self, event):
        """处理行情事件"""
        tick = event.dict_['data']
        vtSymbol = tick.vtSymbol
        
        # 生成datetime对象
        if not tick.datetime:
            tick.datetime = datetime.strptime(' '.join([tick.date, tick.time]), '%Y%m%d %H:%M:%S.%f')

        #判断脏数据
        if self.isDirtyData(tick):
            #脏数据不去做bar处理
            return

        self.onTick(tick)
        bm = self.bmDict.get(vtSymbol, None)
        if bm:
            bm.updateTick(tick)

    #----------------------------------------------------------------------
    #判断脏数据,是脏数据的，返回True
    def isDirtyData(self,tick):
        # 中间启停去掉脏数据
        time_now = datetime.now()
        time_delt = (tick.datetime - time_now).total_seconds()

        if time_delt > 180 or time_delt < -180:
            # 大于3分钟,是脏数据
            return True

        #期货判断交易时间
        dt = time(tick.datetime.hour, tick.datetime.minute)
        # 如果在交易事件内，则为有效数据，无需清洗
        if ((MORNING_START <= dt < MORNING_REST) or
            (MORNING_RESTART <= dt < MORNING_END) or
            (AFTERNOON_START <= dt < AFTERNOON_END) or
            (dt >= NIGHT_START) or
            (dt < NIGHT_END)):
            return False

        return True

    #----------------------------------------------------------------------
    def onTick(self, tick):
        """Tick更新"""
        vtSymbol = tick.vtSymbol
        
        if vtSymbol in self.tickSymbolSet:
            #self.insertData(TICK_DB_NAME, vtSymbol, copy.deepcopy(tick))
            self.insertTickToTxtQueue(vtSymbol, copy.deepcopy(tick))

            """
            if vtSymbol in self.activeSymbolDict:
                activeSymbol = self.activeSymbolDict[vtSymbol]
                self.insertData(TICK_DB_NAME, activeSymbol, tick)
            """

            self.writeDrLog(text.TICK_LOGGING_MESSAGE.format(symbol=tick.vtSymbol,
                                                             time=tick.time, 
                                                             last=tick.lastPrice, 
                                                             bid=tick.bidPrice1, 
                                                             ask=tick.askPrice1))

    #----------------------------------------------------------------------
    #把tick存文件，按合约交易日，每天一文件
    def insertTickToTxtFile(self, symbol, tick, id_):
        fieldnames = ['_id','gatewayName', 'symbol', 'exchange', 'vtSymbol', 'lastPrice', 'lastVolume', 'volume',
                      'openInterest','time','date','datetime','openPrice','highPrice','lowPrice','preClosePrice',
                      'upperLimit','lowerLimit', 'bidPrice1','bidPrice2', 'bidPrice3', 'bidPrice4', 'bidPrice5',
                      'askPrice1', 'askPrice2', 'askPrice3', 'askPrice4', 'askPrice5',
                      'bidVolume1', 'bidVolume2', 'bidVolume3', 'bidVolume4', 'bidVolume5',
                      'askVolume1', 'askVolume2', 'askVolume3', 'askVolume4', 'askVolume5',
                      'TradingDay', 'PreSettlementPrice', 'PreOpenInterest', 'ClosePrice', 'SettlementPrice', 'AveragePrice']

        tick = tick.__dict__
        tick['_id'] = id_
        del tick['rawData']
        # 去掉持仓量的小数位0
        tick['openInterest'] = int(tick['openInterest'])

        tradingDay = tick['TradingDay']
        if tradingDay == None or tradingDay == '':
            tradingDay = todayDateStr()
        #多加一层按天的路径
        filepath = self.tick_file_path + tradingDay + '/'
        #创建文件夹
        if not os.path.exists(filepath):
            os.mkdir(filepath)
        fileName = symbol + '.txt'
        filepath = filepath + fileName

        try:
            with open(filepath, 'ab+') as tick_file:
                dict_writer = csv.DictWriter(tick_file, fieldnames=fieldnames)
                dict_writer.writerow(tick)
                tick_file.close()
        except Exception, Argument:
            print "Exception:%s" % Argument
            self.writeLog(Argument)

    #----------------------------------------------------------------------
    def onBar(self, bar):
        """分钟线更新"""
        vtSymbol = bar.vtSymbol
        
        self.insertData(MINUTE_DB_NAME, vtSymbol, bar)

        vtSymbol = bar.vtSymbol

        bm = self.bmDict.get(vtSymbol, None)
        if bm:
            bm.updateBar(bar)

        """
        if vtSymbol in self.activeSymbolDict:
            activeSymbol = self.activeSymbolDict[vtSymbol]
            self.insertData(MINUTE_DB_NAME, activeSymbol, bar)
        """
        
        self.writeDrLog(text.BAR_LOGGING_MESSAGE.format(symbol=bar.vtSymbol, 
                                                        time=bar.time, 
                                                        open=bar.open, 
                                                        high=bar.high, 
                                                        low=bar.low, 
                                                        close=bar.close))        

    #----------------------------------------------------------------------
    def onXBar(self, xmin, bar):
        """X分钟线更新"""
        vtSymbol = bar.vtSymbol

        self.insertData(MINUTE_TO_DB_NAME[xmin], vtSymbol, bar)

        """
        if vtSymbol in self.activeSymbolDict:
            activeSymbol = self.activeSymbolDict[vtSymbol]
            self.insertData(MINUTE_DB_NAME, activeSymbol, bar)
        """

        self.writeDrLog(text.BAR_LOGGING_MESSAGE.format(symbol=bar.vtSymbol,
                                                        time=bar.time,
                                                        open=bar.open,
                                                        high=bar.high,
                                                        low=bar.low,
                                                        close=bar.close))

    #----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.eventEngine.register(EVENT_TICK, self.procecssTickEvent)
        self.eventEngine.register(EVENT_CONTRACT, self.addContract)

    #----------------------------------------------------------------------
    #20171214重写：在小时线后调用本方法，合约的日线单个生成
    def handleDayBar(self, bar):
        contact_ = bar.vtSymbol
        today = todayDate()
        todayStr = today.strftime("%Y%m%d")
        d = {'TradingDay': todayStr}
        barData = self.mainEngine.dbQuery(MINUTE_60_DB_NAME, contact_, d, 'TradingDay')

        day_bar = None
        for bar in barData:
            # 尚未创建对象
            if not day_bar:
                day_bar = VtBarData()

                day_bar.vtSymbol = bar['vtSymbol']
                day_bar.symbol = bar['symbol']
                day_bar.exchange = bar['exchange']

                day_bar.open = bar['open']
                day_bar.high = bar['high']
                day_bar.low = bar['low']
            # 累加老K线
            else:
                day_bar.high = max(day_bar.high, bar['high'])
                day_bar.low = min(day_bar.low, bar['low'])

            # 通用部分
            day_bar.close = bar['close']
            day_bar.datetime = bar['datetime']
            day_bar.TradingDay = bar['TradingDay']
            day_bar.openInterest = bar['openInterest']
            day_bar.volume += int(bar['volume'])

        if day_bar:
            day_bar.datetime = datetime(today.year, today.month, today.day)
            day_bar.date = day_bar.datetime.strftime('%Y%m%d')
            day_bar.time = day_bar.datetime.strftime('%H:%M:%S.%f')

            self.mainEngine.dbInsert(DAY_DB_NAME, contact_, day_bar.__dict__)

    # ----------------------------------------------------------------------
    #调用mainEnginede db查询
    def dbQuery(self, dbName, collectionName, d):
        return self.mainEngine.dbQuery(dbName, collectionName, d)

    #----------------------------------------------------------------------
    def insertData(self, dbName, collectionName, data):
        """插入数据到数据库（这里的data可以是VtTickData或者VtBarData）"""
        self.queue.put((dbName, collectionName, data.__dict__))


    def insertTickToTxtQueue(self, symbol, tick):
        self.txtQueue.put((symbol, tick))

    #----------------------------------------------------------------------
    def run(self):
        """运行插入线程"""
        while self.active:
            try:
                dbName, collectionName, d = self.queue.get(block=True, timeout=1)
                self.mainEngine.dbInsert(dbName, collectionName, d)
            except Empty:
                pass

    #----------------------------------------------------------------------
    def runTxt(self):
        id_ = 1
        while self.txtActive:
            try:
                vtSymbol, d = self.txtQueue.get(block=True, timeout=1)
                self.insertTickToTxtFile(vtSymbol, d, id_)
                id_ += 1
            except Empty:
                pass

    # ----------------------------------------------------------------------
    # 用一个进程判断下午盘结束
    def runAfterClose(self):
        while self.afterCloseActive:
            currentTime = datetime.now().time().replace(second=0, microsecond=0)
            if currentTime == time(15, 2):
                self.clearAfterClose()
                self.afterCloseActive = False
            sleep(25)

    # ----------------------------------------------------------------------
    #下午收盘时，清理未完成的bar，只需要清理1分钟的bar
    def clearAfterClose(self):
        for symbol in self.bmDict:
            bm = self.bmDict[symbol]
            try:
                if bm.bar:
                    if bm.bar.datetime.hour != 15:
                        #15点收盘时，如果没有15点的tick，需要手动把14:59的bar推送
                        bm.bar.datetime = bm.bar.datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
                        bm.bar.date = bm.bar.datetime.strftime('%Y%m%d')
                        bm.bar.time = bm.bar.datetime.strftime('%H:%M:%S.%f')
                        bm.onBar(bm.bar)
            except Exception, e:
                pass

    # ----------------------------------------------------------------------
    #用一个进程判断夜盘结束
    def runNightClose(self):
        while self.nightCloseActive:
            currentTime = datetime.now().time().replace(second=0, microsecond=0)
            if currentTime == time(2, 32):
                # 判断2点半的夜盘收盘
                self.clearNightClose()
                self.nightCloseActive = False
            sleep(25)

    # ----------------------------------------------------------------------
    #夜盘收盘时，清理未完成的bar
    def clearNightClose(self):
        for symbol in self.bmDict:
            bm = self.bmDict[symbol]
            for xmin in bm.xmins:
                try:
                    if bm.myXminBar[xmin]:
                        bm.myXminBar[xmin].datetime = bm.myXminBar[xmin].datetime.replace(second=0, microsecond=0)  # 将秒和微秒设为0
                        bm.myXminBar[xmin].date = bm.myXminBar[xmin].datetime.strftime('%Y%m%d')
                        bm.myXminBar[xmin].time = bm.myXminBar[xmin].datetime.strftime('%H:%M:%S.%f')
                        bm.onXBar(xmin, bm.myXminBar[xmin])
                except Exception, e:
                    pass


    #----------------------------------------------------------------------
    def start(self):
        """启动"""
        self.active = True
        self.thread.start()

        self.txtActive = True
        self.txtThread.start()

        self.afterCloseActive = True
        self.afterCloseThread.start()

        self.nightCloseActive = True
        self.nightCloseThread.start()
        
    #----------------------------------------------------------------------
    def stop(self):
        """退出"""
        if self.active:
            self.active = False
            self.thread.join()

            self.txtActive = False
            self.txtThread.join()

            self.nightCloseActive = False
            self.nightCloseThread.join()
        
    #----------------------------------------------------------------------
    def writeDrLog(self, content):
        """快速发出日志事件"""
        log = VtLogData()
        log.logContent = content
        event = Event(type_=EVENT_DATARECORDER_LOG)
        event.dict_['data'] = log
        self.eventEngine.put(event)   

