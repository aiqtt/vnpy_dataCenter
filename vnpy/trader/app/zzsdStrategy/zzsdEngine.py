# encoding: UTF-8

'''
本文件中实现了CTA策略引擎，针对CTA类型的策略，抽象简化了部分底层接口的功能。

关于平今和平昨规则：
1. 普通的平仓OFFSET_CLOSET等于平昨OFFSET_CLOSEYESTERDAY
2. 只有上期所的品种需要考虑平今和平昨的区别
3. 当上期所的期货有今仓时，调用Sell和Cover会使用OFFSET_CLOSETODAY，否则
   会使用OFFSET_CLOSE
4. 以上设计意味着如果Sell和Cover的数量超过今日持仓量时，会导致出错（即用户
   希望通过一个指令同时平今和平昨）
5. 采用以上设计的原因是考虑到vn.trader的用户主要是对TB、MC和金字塔类的平台
   感到功能不足的用户（即希望更高频的交易），交易策略不应该出现4中所述的情况
6. 对于想要实现4中所述情况的用户，需要实现一个策略信号引擎和交易委托引擎分开
   的定制化统结构（没错，得自己写）
'''

from __future__ import division

import json
import tushare as ts
import os
import  copy
import traceback
from collections import OrderedDict
from datetime import datetime, timedelta, time

from vnpy.event import Event
from vnpy.trader.vtEvent import *
from vnpy.trader.vtConstant import *
from vnpy.trader.vtObject import VtTickData, VtBarData
from vnpy.trader.vtGateway import VtSubscribeReq, VtOrderReq, VtCancelOrderReq, VtLogData
from vnpy.trader.vtFunction import todayDate, getJsonPath

from vnpy.trader.app.zzsdStrategy.zzsdBase import *
from vnpy.trader.app.zzsdStrategy.strategy import STRATEGY_CLASS
from vnpy.trader.app.zzsdStrategy.strategy.strategyZzsd import *
from vnpy.trader.data.mysqlEngine import SQL_TABLENAME_STOP_ORDER

import pandas as pd
import numpy as np
import threading


########################################################################
class ZzsdEngine(object):
    """Zzsd策略引擎"""
    settingFileName = 'ZZSD_setting.json'
    settingfilePath = getJsonPath(settingFileName, __file__)
    
    STATUS_FINISHED = set([STATUS_REJECTED, STATUS_CANCELLED, STATUS_ALLTRADED])
    FIX_SIZE_AUDO = -1
    #----------------------------------------------------------------------
    def __init__(self, mainEngine, eventEngine):
        """Constructor"""
        self.mainEngine = mainEngine
        self.eventEngine = eventEngine
        
        # 当前日期
        self.today = todayDate()
        self.symbol = ""        ##策略折行的产品，比如 ru0000  rb0000
        self.mainSymbol = ""    ##主力合约
        self.secSymbol  = ""    #次主力合约
        self.account = None     ##账户
        self.capitalUseRat = 0.5  ##资金使用率 默认0.5
        self.maxVolume  = 50      ##每次开仓最大手数
        self.riskRate   = 0.02    #单笔风险敞口
        
        # 保存策略实例的字典
        # key为合约号+实例名，保证1个合约1个策略1个实例，value为策略实例
        self.strategyDict = {}
        
        # 保存vtSymbol和策略实例映射的字典（用于推送tick数据）
        # 由于可能多个strategy交易同一个vtSymbol，因此key为vtSymbol
        # value为包含所有相关strategy对象的list
        self.tickStrategyDict = {}
        
        # 保存vtOrderID和strategy对象映射的字典（用于推送order和trade数据）
        # key为vtOrderID，value为strategy对象
        self.orderStrategyDict = {}     
        
        # 本地停止单编号计数
        self.stopOrderCount = 0
        # stopOrderID = STOPORDERPREFIX + str(stopOrderCount)
        
        # 本地停止单字典
        # key为stopOrderID，value为stopOrder对象
        self.stopOrderDict = {}             # 停止单撤销后不会从本字典中删除
        self.workingStopOrderDict = {}      # 停止单撤销后会从本字典中删除
        
        # 保存策略名称和委托号列表的字典
        # key为symbol合约，value为保存orderID（限价+本地停止）的集合
        self.strategyOrderDict = {}
        
        # 成交号集合，用来过滤已经收到过的成交推送
        self.tradeSet = set()
        
        # 引擎类型为实盘
        self.engineType = ENGINETYPE_TRADING
        
        # 注册日式事件类型
        #self.mainEngine.registerLogEvent(EVENT_ZZSD_LOG)
        
        # 注册事件监听
        self.registerEvent()




    def checkPosition(self):
        print 'hello timer!'
        detailDict = self.mainEngine.dataEngine.getAllPositionDetail()
        for key in detailDict:
            print(key)
            if key != self.mainSymbol:
                return key   ##合约还有仓位，继续跟踪

        return ""


    ##获取仓位  暂不考虑同时有long和short的情况
    def getPos(self,symbol):
        detail = self.mainEngine.dataEngine.getPositionDetail(symbol)
        if detail.longPos > 0:
            return detail.longPos

        if detail.shortPos > 0:
            return -detail.shortPos

    #----------------------------------------------------------------------
    def sendOrder(self, vtSymbol, orderType, price, volume, strategy):
        """发单"""
        contract = self.mainEngine.getContract(vtSymbol)
        
        req = VtOrderReq()
        req.symbol = contract.symbol
        req.exchange = contract.exchange
        req.vtSymbol = contract.vtSymbol
        req.price = self.roundToPriceTick(contract.priceTick, price)
        req.volume = volume
        
        req.productClass = strategy.productClass
        req.currency = strategy.currency        
        
        # 设计为CTA引擎发出的委托只允许使用限价单
        req.priceType = PRICETYPE_LIMITPRICE    
        
        # CTA委托类型映射
        if orderType == CTAORDER_BUY:
            req.direction = DIRECTION_LONG
            req.offset = OFFSET_OPEN
            
        elif orderType == CTAORDER_SELL:
            req.direction = DIRECTION_SHORT
            req.offset = OFFSET_CLOSE
                
        elif orderType == CTAORDER_SHORT:
            req.direction = DIRECTION_SHORT
            req.offset = OFFSET_OPEN
            
        elif orderType == CTAORDER_COVER:
            req.direction = DIRECTION_LONG
            req.offset = OFFSET_CLOSE
            
        # 委托转换
        reqList = self.mainEngine.convertOrderReq(req)
        vtOrderIDList = []
        
        if not reqList:
            return vtOrderIDList
        
        for convertedReq in reqList:
            vtOrderID = self.mainEngine.sendOrder(convertedReq, contract.gatewayName)    # 发单
            self.orderStrategyDict[vtOrderID] = strategy                                 # 保存vtOrderID和策略的映射关系
            self.strategyOrderDict[req.symbol].add(vtOrderID)                         # 添加到策略委托号集合中
            vtOrderIDList.append(vtOrderID)
            
        self.writeCtaLog(u'策略%s发送委托，%s，%s，%s@%s' 
                         %(strategy.name, vtSymbol, req.direction, volume, price))
        
        return vtOrderIDList
    
    #----------------------------------------------------------------------
    def cancelOrder(self, vtOrderID):
        """撤单"""
        # 查询报单对象
        order = self.mainEngine.getOrder(vtOrderID)
        
        # 如果查询成功
        if order:
            # 检查是否报单还有效，只有有效时才发出撤单指令
            orderFinished = (order.status==STATUS_ALLTRADED or order.status==STATUS_CANCELLED)
            if not orderFinished:
                req = VtCancelOrderReq()
                req.symbol = order.symbol
                req.exchange = order.exchange
                req.frontID = order.frontID
                req.sessionID = order.sessionID
                req.orderID = order.orderID
                self.mainEngine.cancelOrder(req, order.gatewayName)    


       #----------------------------------------------------------------------
    def recoverStopOrder(self, vtSymbol, orderType, price, volume, strategy):
        """恢复停止单"""
        self.stopOrderCount += 1
        stopOrderID = STOPORDERPREFIX + str(self.stopOrderCount)

        so = StopOrder()
        so.vtSymbol = vtSymbol
        so.orderType = orderType
        so.price = price
        so.volume = volume
        so.strategy = strategy
        so.stopOrderID = stopOrderID
        so.status = STOPORDER_WAITING

        if orderType == CTAORDER_BUY:
            so.direction = DIRECTION_LONG
            so.offset = OFFSET_OPEN
        elif orderType == CTAORDER_SELL:
            so.direction = DIRECTION_SHORT
            so.offset = OFFSET_CLOSE
        elif orderType == CTAORDER_SHORT:
            so.direction = DIRECTION_SHORT
            so.offset = OFFSET_OPEN
        elif orderType == CTAORDER_COVER:
            so.direction = DIRECTION_LONG
            so.offset = OFFSET_CLOSE

        # 保存stopOrder对象到字典中
        self.stopOrderDict[stopOrderID] = so
        self.workingStopOrderDict[stopOrderID] = so

        # 保存stopOrderID到策略委托号集合中
        self.strategyOrderDict[so.vtSymbol].add(stopOrderID)

        # 推送停止单状态
        strategy.onStopOrder(so)



    #----------------------------------------------------------------------
    def sendStopOrder(self, vtSymbol, orderType, price, volume, strategy):
        """发停止单（本地实现）"""
        self.stopOrderCount += 1
        stopOrderID = STOPORDERPREFIX + str(self.stopOrderCount)
        
        so = StopOrder()
        so.vtSymbol = vtSymbol
        so.orderType = orderType
        so.price = price
        so.volume = volume
        so.strategy = strategy
        so.stopOrderID = stopOrderID
        so.status = STOPORDER_WAITING
        
        if orderType == CTAORDER_BUY:
            so.direction = DIRECTION_LONG
            so.offset = OFFSET_OPEN
        elif orderType == CTAORDER_SELL:
            so.direction = DIRECTION_SHORT
            so.offset = OFFSET_CLOSE
        elif orderType == CTAORDER_SHORT:
            so.direction = DIRECTION_SHORT
            so.offset = OFFSET_OPEN
        elif orderType == CTAORDER_COVER:
            so.direction = DIRECTION_LONG
            so.offset = OFFSET_CLOSE           
        
        # 保存stopOrder对象到字典中
        self.stopOrderDict[stopOrderID] = so
        self.workingStopOrderDict[stopOrderID] = so
        
        # 保存stopOrderID到策略委托号集合中
        self.strategyOrderDict[so.vtSymbol].add(stopOrderID)
        
        # 推送停止单状态
        strategy.onStopOrder(so)

        ##停止单添加到数据库  目前只处理下单插入， 发出委托后删除
        self.mainEngine.mysqlClient.dbInsert(SQL_TABLENAME_STOP_ORDER, so)
        
        return [stopOrderID]
    
    #----------------------------------------------------------------------
    def cancelStopOrder(self, stopOrderID):
        """撤销停止单"""
        # 检查停止单是否存在
        if stopOrderID in self.workingStopOrderDict:
            so = self.workingStopOrderDict[stopOrderID]
            strategy = so.strategy
            
            # 更改停止单状态为已撤销
            so.status = STOPORDER_CANCELLED
            
            # 从活动停止单字典中移除
            del self.workingStopOrderDict[stopOrderID]
            
            # 从策略委托号集合中移除
            s = self.strategyOrderDict[so.vtSymbol]
            if stopOrderID in s:
                s.remove(stopOrderID)
            
            # 通知策略
            strategy.onStopOrder(so)

            self.mainEngine.mysqlClient.dbDelete(SQL_TABLENAME_STOP_ORDER, so)

    #----------------------------------------------------------------------
    def processStopOrder(self, tick):
        """收到行情后处理本地停止单（检查是否要立即发出）"""
        vtSymbol = tick.vtSymbol
        
        # 首先检查是否有策略交易该合约
        if vtSymbol in self.tickStrategyDict:
            # 遍历等待中的停止单，检查是否会被触发
            for so in self.workingStopOrderDict.values():
                if so.vtSymbol == vtSymbol:
                    longTriggered = so.direction==DIRECTION_LONG and tick.lastPrice>=so.price        # 多头停止单被触发
                    shortTriggered = so.direction==DIRECTION_SHORT and tick.lastPrice<=so.price     # 空头停止单被触发
                    
                    if longTriggered or shortTriggered:
                        # 买入和卖出分别以涨停跌停价发单（模拟市价单）
                        if so.direction==DIRECTION_LONG:
                            price = tick.upperLimit
                        else:
                            price = tick.lowerLimit
                        
                        # 发出市价委托
                        self.sendOrder(so.vtSymbol, so.orderType, price, so.volume, so.strategy)
                        
                        # 从活动停止单字典中移除该停止单
                        del self.workingStopOrderDict[so.stopOrderID]
                        
                        # 从策略委托号集合中移除
                        s = self.strategyOrderDict[so.vtSymbol]
                        if so.stopOrderID in s:
                            s.remove(so.stopOrderID)
                        
                        # 更新停止单状态，并通知策略
                        so.status = STOPORDER_TRIGGERED
                        so.strategy.onStopOrder(so)

                        self.mainEngine.mysqlClient.dbDelete(SQL_TABLENAME_STOP_ORDER, so)

    #----------------------------------------------------------------------
    def processTickEvent(self, event):
        """处理行情推送"""
        tick = event.dict_['data']
        # 收到tick行情后，先处理本地停止单（检查是否要立即发出）
        self.processStopOrder(tick)
        
        # 推送tick到对应的策略实例进行处理
        if tick.vtSymbol in self.tickStrategyDict:
            # tick时间可能出现异常数据，使用try...except实现捕捉和过滤
            try:
                # 添加datetime字段
                if not tick.datetime:
                    tick.datetime = datetime.strptime(' '.join([tick.date, tick.time]), '%Y%m%d %H:%M:%S.%f')
            except ValueError:
                self.writeCtaLog(traceback.format_exc())
                return
                
            # 逐个推送到策略实例中
            l = self.tickStrategyDict[tick.vtSymbol]
            for strategy in l:
                self.callStrategyFunc(strategy, strategy.onTick, tick)
    
    #----------------------------------------------------------------------
    def processOrderEvent(self, event):
        """处理委托推送"""
        order = event.dict_['data']
        
        vtOrderID = order.vtOrderID
        
        if vtOrderID in self.orderStrategyDict:
            strategy = self.orderStrategyDict[vtOrderID]            
            
            # 如果委托已经完成（拒单、撤销、全成），则从活动委托集合中移除
            self.writeCtaLog(u"委托推送:%s %s"%(order.symbol,order.status))
            if order.status in self.STATUS_FINISHED:
                s = self.strategyOrderDict[order.symbol]
                if vtOrderID in s:
                    s.remove(vtOrderID)
            
            self.callStrategyFunc(strategy, strategy.onOrder, order)
    
    #----------------------------------------------------------------------
    def processTradeEvent(self, event):
        """处理成交推送"""
        trade = event.dict_['data']
        
        # 过滤已经收到过的成交回报
        if trade.vtTradeID in self.tradeSet:
            return
        self.tradeSet.add(trade.vtTradeID)
        
        # 将成交推送到策略对象中
        if trade.vtOrderID in self.orderStrategyDict:
            strategy = self.orderStrategyDict[trade.vtOrderID]
            
            # 计算策略持仓
            if trade.direction == DIRECTION_LONG:
                strategy.pos += trade.volume
            else:
                strategy.pos -= trade.volume
            
            self.callStrategyFunc(strategy, strategy.onTrade, trade)
            
            # 保存策略持仓到数据库
            self.savePosition(strategy)              


    def processAccountEvent(self,event):
        self.account = event.dict_['data']

    #----------------------------------------------------------------------
    def registerEvent(self):
        """注册事件监听"""
        self.eventEngine.register(EVENT_TICK, self.processTickEvent)
        self.eventEngine.register(EVENT_ORDER, self.processOrderEvent)
        self.eventEngine.register(EVENT_TRADE, self.processTradeEvent)
        self.eventEngine.register(EVENT_ACCOUNT,self.processAccountEvent)
 
    #----------------------------------------------------------------------
    def insertData(self, dbName, collectionName, data):
        """插入数据到数据库（这里的data可以是VtTickData或者VtBarData）"""
        self.mainEngine.dbInsert(dbName, collectionName, data.__dict__)
    
    #----------------------------------------------------------------------
    def loadBar(self, dbName, collectionName, days):
        """从数据库中读取Bar数据，startDate是datetime对象"""
        startDate = self.today - timedelta(days)
        
        d = {'datetime':{'$gte':startDate}}
        barData = self.mainEngine.dbQuery(dbName, collectionName, d, 'datetime')
        
        l = []
        for d in barData:
            bar = VtBarData()
            bar.__dict__ = d
            l.append(bar)
        return l




    def getCurrentBar(self,barData):

        day_bar =None
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
            day_bar.openInterest = bar['openInterest']
            day_bar.volume += int(bar['volume'])

        return day_bar



    def loadMinuteBar(self,symbol,minute):

        nowTime = datetime.now()
        startMinute = int(nowTime.minute/minute) * minute
        startTime = nowTime.replace(minute=startMinute,second=0, microsecond=0)

        return self.loadAllBar(MINUTE_DB_NAME,symbol,startTime)


    def loadCurrentDayMinBar(self,symbol):
        ##获取当前day 分钟bar
        nowTime = datetime.now().time()

        DAY_START = time(8, 57)         # 日盘启动和停止时间
        DAY_END = time(15, 18)
        NIGHT_START = time(20, 57)      # 夜盘启动和停止时间
        NIGHT_END = time(2, 33)

        ##晚上9点算一天开始
        startTime = datetime.now().replace(hour=21,minute=0,second=0, microsecond=0)

        ##早上 和 下午  从前一天   晚上9点开始 算一天开始
        if (nowTime > DAY_START and nowTime <  DAY_END ):
            if datetime.today().weekday() == 0:  ##周一
                startTime = startTime + timedelta(-3)
            else:
                startTime = startTime + timedelta(-1)

        if nowTime < NIGHT_END:  ##凌晨从前一天开始
            startTime = startTime + timedelta(-1)

        return self.loadAllBar(MINUTE_DB_NAME,symbol,startTime)


    #----------------------------------------------------------------------
    def loadAllBar(self, dbName, collectionName, startDate = None):
        """从数据库中读取Bar数据，startDate是datetime对象"""
        d = {}
        if startDate:
            d = {'datetime':{'$gte':startDate}}

        barData = self.mainEngine.dbQuery(dbName, collectionName,d)

        l = []
        for d in barData:
            bar = VtBarData()
            bar.__dict__ = d
            l.append(bar)
        return l

    #----------------------------------------------------------------------
    def loadTick(self, dbName, collectionName, days):
        """从数据库中读取Tick数据，startDate是datetime对象"""
        startDate = self.today - timedelta(days)
        
        d = {'datetime':{'$gte':startDate}}
        tickData = self.mainEngine.dbQuery(dbName, collectionName, d, 'datetime')
        
        l = []
        for d in tickData:
            tick = VtTickData()
            tick.__dict__ = d
            l.append(tick)
        return l    
    
    #----------------------------------------------------------------------
    def writeCtaLog(self, content):
        """快速发出CTA模块日志事件"""
        log = VtLogData()
        log.logContent = content
        log.gatewayName = 'ZZSD_STRATEGY'
        event = Event(type_=EVENT_ZZSD_LOG)
        event.dict_['data'] = log
        self.eventEngine.put(event)   
    
    #----------------------------------------------------------------------
    def loadStrategy(self, setting):
        """载入策略"""
        try:
            self.capitalUseRat = setting["capitalUseRat"]
            self.maxVolume     = setting["maxVolume"]
            self.riskRate      = setting["riskRate"]

            self.symbol = setting['vtSymbol']
            ##
            listContract = self.getProductContractList(self.today)
            self.mainSymbol = self.getMainContact(self.today, listContract)

        except Exception, e:
            self.writeCtaLog(u'载入策略出错：%s' %e)
            return

        if not self.mainSymbol:
            self.writeCtaLog(u'主力合约获取失败')
            return
        
        self.initStrategyBySymbol(self.mainSymbol,setting)

        self.secSymbol = self.checkPosition()
        if self.secSymbol and self.secSymbol != "":

            self.initStrategyBySymbol(self.secSymbol,setting)
            self.strategyDict[self.secSymbol].setIsMainSymbol(False)


        ##恢复停止单
        so = StopOrder()

        stopArray = self.mainEngine.mysqlClient.dbSelect(SQL_TABLENAME_STOP_ORDER, so,"all")
        for d in stopArray:
            if self.strategyDict.has_key(d["symbol"]) == False:
                continue

            strategy = self.strategyDict[d["symbol"]]
            if strategy and d['status'] == STOPORDER_WAITING:
                self.recoverStopOrder(d["symbol"],d["orderType"],d["price"],d["volume"],strategy)
                self.writeCtaLog(u'恢复停止单：%s  %s  %f %s' %(d["symbol"], d["orderType"], d["price"], d["volume"]))




    def initStrategyBySymbol(self,symbol,setting):

        # 创建策略实例
                # 防止策略重名
        if symbol in self.strategyDict:
            self.writeCtaLog(u'策略实例重名：%s' %name)
            return

        setting["vtSymbol"] = symbol
        strategy = ZzsdStrategy(self, setting)

        self.strategyDict[symbol] = strategy
        strategy.setFixSize(self.FIX_SIZE_AUDO)
        strategy.initDayBar(self.loadAllBar(DAY_DB_NAME,symbol ))
        strategy.initDayCurrentBar(self.getCurrentBar(self.loadCurrentDayMinBar(symbol)))

        if setting["strategyCycle"] == "60min":
            strategy.initHourBar(self.loadAllBar(MINUTE_60_DB_NAME,symbol ))
            strategy.initHourCurrentBar(self.getCurrentBar(self.loadMinuteBar(symbol,60)))
        if setting["strategyCycle"] == "30min":
            strategy.initHourBar(self.loadAllBar(MINUTE_30_DB_NAME,symbol ))
            strategy.initHourCurrentBar(self.getCurrentBar(self.loadMinuteBar(symbol,30)))
        if setting["strategyCycle"] == "5min":
            strategy.initHourBar(self.loadAllBar(MINUTE_5_DB_NAME,symbol ))
            strategy.initHourCurrentBar(self.getCurrentBar(self.loadMinuteBar(symbol,5)))

        # 创建委托号列表
        self.strategyOrderDict[symbol] = set()

        # 保存Tick映射关系
        if strategy.vtSymbol in self.tickStrategyDict:
            l = self.tickStrategyDict[strategy.vtSymbol]
        else:
            l = []
            self.tickStrategyDict[strategy.vtSymbol] = l
        l.append(strategy)

        # 订阅合约
        contract = self.mainEngine.getContract(strategy.vtSymbol)
        if contract:
            req = VtSubscribeReq()
            req.symbol = contract.symbol
            req.exchange = contract.exchange

            # 对于IB接口订阅行情时所需的货币和产品类型，从策略属性中获取
            req.currency = strategy.currency
            req.productClass = strategy.productClass

            self.mainEngine.subscribe(req, contract.gatewayName)
        else:
            self.writeCtaLog(u'%s的交易合约%s无法找到' %(name, strategy.vtSymbol))

    #----------------------------------------------------------------------
    def initStrategy(self, name):
        """初始化策略"""
        if name in self.strategyDict:
            strategy = self.strategyDict[name]
            
            if not strategy.inited:
                strategy.inited = True
                self.callStrategyFunc(strategy, strategy.onInit)
            else:
                self.writeCtaLog(u'请勿重复初始化策略实例：%s' %name)
        else:
            self.writeCtaLog(u'策略实例不存在：%s' %name)        

    #---------------------------------------------------------------------
    def startStrategy(self, name):
        """启动策略"""
        if name in self.strategyDict:
            strategy = self.strategyDict[name]
            
            if strategy.inited and not strategy.trading:
                strategy.trading = True
                self.callStrategyFunc(strategy, strategy.onStart)
        else:
            self.writeCtaLog(u'策略实例不存在：%s' %name)


    ### 获取合约最大volume
    def getSymbolVolume(self, symbol,price,stopPrice):
        ##判断资金使用率
        if self.getAccountAvailableRate() > self.capitalUseRat:
            return 0

        maxSymbolVol = self.maxVolume

        volumeMultiple = self.mainEngine.getContract(symbol).size

        maxRiskVol = int(self.account.available *self.riskRate/abs(price - stopPrice)/volumeMultiple)

        ##可用资金作为保证金的手数


        return min(maxSymbolVol, maxRiskVol)

    ##返回前一次开仓 手数
    def getSymbolPrePosition(self, symbol):
        detail = self.mainEngine.dataEngine.getPositionDetail(symbol)
        if detail.longPos != 0:
            return detail.longPos

        if detail.shortPos != 0:
            return detail.shortPos

        return 0




    #----------------------------------------------------------------------
    def stopStrategy(self, name):
        """停止策略"""
        if name in self.strategyDict:
            strategy = self.strategyDict[name]
            
            if strategy.trading:
                strategy.trading = False
                self.callStrategyFunc(strategy, strategy.onStop)
                
                # 对该策略发出的所有限价单进行撤单
                for vtOrderID, s in self.orderStrategyDict.items():
                    if s is strategy:
                        self.cancelOrder(vtOrderID)
                
                # 对该策略发出的所有本地停止单撤单
                for stopOrderID, so in self.workingStopOrderDict.items():
                    if so.strategy is strategy:
                        self.cancelStopOrder(stopOrderID)   
        else:
            self.writeCtaLog(u'策略实例不存在：%s' %name)    
            
    #----------------------------------------------------------------------
    def initAll(self):
        """全部初始化"""
        for name in self.strategyDict.keys():
            self.initStrategy(name)    
            
    #----------------------------------------------------------------------
    def startAll(self):
        """全部启动"""
        for name in self.strategyDict.keys():
            self.startStrategy(name)


            
    #----------------------------------------------------------------------
    def stopAll(self):
        """全部停止"""
        for name in self.strategyDict.keys():
            self.stopStrategy(name)    
    
    #----------------------------------------------------------------------
    def saveSetting(self):
        """保存策略配置"""
        with open(self.settingfilePath, 'w') as f:
            l = []
            
            for strategy in self.strategyDict.values():
                setting = {}
                for param in strategy.paramList:
                    setting[param] = strategy.__getattribute__(param)
                l.append(setting)
            
            jsonL = json.dumps(l, indent=4)
            f.write(jsonL)
    
    #----------------------------------------------------------------------
    def loadSetting(self):
        """读取策略配置"""
        with open(self.settingfilePath) as f:
            l = json.load(f)
            
            #for setting in l:
            self.loadStrategy(l)
                
        #self.loadPosition() ##通过订阅position实现pos初始化, 因为这里不涉及1个合约对应多策略的情况


    def getStopOrders(self):
        varDict = []
        for stopOrderID, so in self.workingStopOrderDict.items():
            so_t = copy.copy(so)
            so_t.strategy = None
            varDict.append(so_t)

        return varDict



    #----------------------------------------------------------------------
    def getStrategyVar(self, name):
        """获取策略当前的变量字典"""
        if name in self.strategyDict:
            strategy = self.strategyDict[name]
            varDict = OrderedDict()
            
            for key in strategy.varList:
                varDict[key] = strategy.__getattribute__(key)
            
            return varDict
        else:
            self.writeCtaLog(u'策略实例不存在：' + name)    
            return None
    
    #----------------------------------------------------------------------
    def getStrategyParam(self, name):
        """获取策略的参数字典"""
        if name in self.strategyDict:
            strategy = self.strategyDict[name]
            paramDict = OrderedDict()
            
            for key in strategy.paramList:  
                paramDict[key] = strategy.__getattribute__(key)
            
            return paramDict
        else:
            self.writeCtaLog(u'策略实例不存在：' + name)    
            return None   
        
    #----------------------------------------------------------------------
    def putStrategyEvent(self, name):
        """触发策略状态变化事件（通常用于通知GUI更新）"""
        event = Event(EVENT_ZZSD_STRATEGY+name)
        self.eventEngine.put(event)
        
    #----------------------------------------------------------------------
    def callStrategyFunc(self, strategy, func, params=None):
        """调用策略的函数，若触发异常则捕捉"""
        try:
            if params:
                func(params)
            else:
                func()
        except Exception:
            # 停止策略，修改状态为未初始化
            strategy.trading = False
            strategy.inited = False
            
            # 发出日志
            content = '\n'.join([u'策略%s触发异常已停止' %strategy.name,
                                traceback.format_exc()])
            self.writeCtaLog(content)
            
    #----------------------------------------------------------------------
    def savePosition(self, strategy):
        """保存策略的持仓情况到数据库"""
        flt = {'name': strategy.name,
               'vtSymbol': strategy.vtSymbol}
        
        d = {'name': strategy.name,
             'vtSymbol': strategy.vtSymbol,
             'pos': strategy.pos}
        
        self.mainEngine.dbUpdate(POSITION_DB_NAME, strategy.className,
                                 d, flt, True)
        
        content = '策略%s持仓保存成功，当前持仓%s' %(strategy.name, strategy.pos)
        self.writeCtaLog(content)
    
    #----------------------------------------------------------------------
    def loadPosition(self):
        """从数据库载入策略的持仓情况"""
        for strategy in self.strategyDict.values():
            flt = {'name': strategy.name,
                   'vtSymbol': strategy.vtSymbol}
            posData = self.mainEngine.dbQuery(POSITION_DB_NAME, strategy.className, flt)
            
            for d in posData:
                strategy.pos = d['pos']
                
    #----------------------------------------------------------------------
    def roundToPriceTick(self, priceTick, price):
        """取整价格到合约最小价格变动"""
        if not priceTick:
            return price
        
        newPrice = round(price/priceTick, 0) * priceTick
        return newPrice    
    
    #----------------------------------------------------------------------
    def stop(self):
        """停止"""
        pass


    #---------------------------------------------------------------------
    def cancelAllStop(self,name):
        ##撤销所有的停止单
        s = self.strategyOrderDict[name]

        for orderID in list(s):
            if STOPORDERPREFIX in orderID:
                self.cancelStopOrder(orderID)


    #----------------------------------------------------------------------
    def cancelAll(self, name):
        """全部撤单"""
        s = self.strategyOrderDict[name]
        
        # 遍历列表，全部撤单
        # 这里不能直接遍历集合s，因为撤单时会修改s中的内容，导致出错
        for orderID in list(s):
            if STOPORDERPREFIX in orderID:
                self.cancelStopOrder(orderID)
            else:
                self.cancelOrder(orderID)


    #------------------------------------------------
    # 数据回放相关
    #------------------------------------------------
    def getProductContractList(self, startTime):
        """  合约前缀+当前日期后12个月  """


        number_0 = self.symbol.count('0')
        listContract = []
        for i in range(11):
            time_aaa = startTime + pd.tseries.offsets.DateOffset(months=i,days=0)
            if number_0 == 3:
                listContract.append(self.symbol[0:len(self.symbol)-number_0] +str(time_aaa.year)[3:4]+str(time_aaa.month).zfill(2))
            if number_0 == 4:
                listContract.append(self.symbol[0:len(self.symbol)-number_0] +str(time_aaa.year)[2:4]+str(time_aaa.month).zfill(2))

        return listContract

    ## 判断主力合约
    def getMainContact(self, dateTime , listContract):
        mianSymbol = None
        volume = 0
        for contract in listContract:
            #print contract
             #加载日线file
            kData = self.readDayData(dateTime,contract)
            if not kData:
                continue

            if  kData and kData["volume"] > volume:
                mianSymbol = kData["symbol"]
                volume = kData["volume"]

        return mianSymbol

    ##读取日线文件到那天的数据
    def readDayData(self, dateTime,contract ):

        startDate = self.today - timedelta(20)  ##一般假期不会多于10天吧
        d = {'datetime':{'$gte':startDate}}
        dayData = self.mainEngine.dbQuery(DAY_DB_NAME, contract, d, 'datetime')

        if len(dayData) == 0:
            return None

        return dayData[-1]

    ##获取账户可用资金比率    账户可用资金/ 净值
    def getAccountAvailableRate(self):

        return (self.account.available)/self.account.balance



