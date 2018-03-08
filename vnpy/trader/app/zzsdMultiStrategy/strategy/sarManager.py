# encoding: UTF-8

"""
    一个SAR指标管理
"""
import numpy as np
import talib
from math import ceil, floor
from vnpy.trader.vtObject import VtBarData
from vnpy.trader.vtConstant import EMPTY_STRING
from vnpy.trader.app.zzsdStrategy.zzsdTemplate import (ZzsdTemplate,
                                                     BarManager,
                                                     ArrayManager)

class SarManager(object):


    name = ""

    SAR_TYPE_QI_UP     = 1
    SAR_TYPE_QI_DOWN   = 2
    SAR_TYPE_HEAD_UP   = 3   ##头肩顶  下降
    SAR_TYPE_HEAD_DOWN = 4   ##头肩顶   上升
    SAR_TYPE_DELTA_SMALL = 5  ##收敛三角形

    def __init__(self,name=''):

        self.sarArray = np.zeros(0)             #sar 指标数组
        self.highlowArray = np.zeros(0)         #根据sar指标算出的高低点数组
        self.highlowArrayPre = np.zeros(0)     #计算高低点之后的原始值
        self.highArray = []
        self.lowArray = []
        self.sarType = 0               #sar类型  1上升旗形  2 下降旗形 3 头肩顶 4 头肩底
        self.isLong = -1               #sar 当前是上升还是下降
        self.highValue = 0.0          #上升的时候之前bar的最高点， 下降时候的之前bar最低点
        self.lowValue  = 999999.0
        self.name = name
        self.barIsEnd = True          ##bar 线更新这个值为True，则操作周期第一个tick来需要保持进数组，并计算sar
        self.lastUpdateTime = None    ##bar 线更新的最后的时间
        self.revSarValue = 0.0
        self.optForType = 0     ##操作周期最后4个点是否是上升旗形或者下降旗形


    def initBar(self, initData):
        for bar in initData:
            self.highArray.append(bar.high)
            self.lowArray.append(bar.low)



        ##更新bar后更新sar array和高低点
        self.sarArray =  self.SAR()
        self.calHighLow()
        self.sarTypeCal()

        self.inited = True


    def SAR(self):

       return  talib.SAR( np.array(self.highArray),  np.array(self.lowArray))

    ###更新当前bar，就是opt结束后更新一下。   操作周期k线合成都是算在最后1min内，比如小时线，2点到3点的算在3点里
    ###日线是等今天更新完才会调用这个函数
    ### 周线数据 1周的是在周1，
    def updateBar(self, bar):


        if self.lastUpdateTime and self.lastUpdateTime == bar.datetime:
            self.highArray[-1] = bar.high
            self.lowArray[-1] = bar.low
        else:
            self.highArray[-1] = bar.high
            self.lowArray[-1] = bar.low

            self.barIsEnd = True

        self.lastUpdateTime = bar.datetime


    def calHighLow(self):
        #计算sar高低点  比 bar的最低点低 就是上升的，比bar最高点搞就是下降的

        self.highlowArray = np.zeros(len(self.highArray))  ## 重新初始化

        currentLong = -1  ### 0下降 1上升
        self.highValue = 0.0
        self.lowValue  = 999999.0



        for index in range(self.sarArray.size):


            if self.sarArray[index] >= self.highArray[index]:  ## 下降
                if currentLong != 0 :
                    self.highlowArray[index] = (self.sarArray[index])
                    currentLong = 0
                    self.lowValue =  self.lowArray[index]


            elif self.sarArray[index] <= self.lowArray[index] :  ##上升
                if currentLong != 1 :
                    self.highlowArray[index] = (self.sarArray[index])
                    currentLong = 1
                    self.highValue = self.highArray[index]




            if currentLong == 0 and self.lowValue > self.lowArray[index]:
                   self.lowValue = self.lowArray[index]
            elif currentLong == 1 and self.highValue < self.highArray[index]:
                    self.highValue = self.highArray[index]

        if currentLong != -1:
            self.isLong = currentLong


        ## 去掉中途的单点，靠近的4个点不去，为了后面判断3点和4点不开仓
        self.highlowArrayPre = np.array(self.highlowArray)

        arrayLen = len(self.highlowArray)

        while arrayLen > 4:
            arrayLen -= 1
            if self.highlowArray[arrayLen]  != 0.0 and self.highlowArray[arrayLen-1]  != 0.0: ## 单点
                self.highlowArray[arrayLen]  = 0.0
                self.highlowArray[arrayLen-1]  = 0.0

    def sarTypeCal(self):
        if self.name == "day":
            ##日线
            self.sarTypeCalDay()
        else:
            self.sarTypeCalOpt()

    ###日线形态判断，日线当前价格算一个点
    def sarTypeCalDay(self):
        ## 判断sar类型
        #取最后6个高低点
        value_1 = 0.0
        value_2 = 0.0
        value_3 = 0.0
        value_4 = 0.0
        value_5 = 0.0
        value_6 = 0.0

        for i in range(0, self.highlowArray.__len__())[::-1]:
            if self.highlowArray[i] != 0.0:
                if value_1 == 0.0:
                    value_1 = self.highlowArray[i]
                    continue
                if value_2 == 0.0:
                    value_2 = self.highlowArray[i]
                    continue
                if value_3 == 0.0:
                    value_3 = self.highlowArray[i]
                    continue
                if value_4 == 0.0:
                    value_4 = self.highlowArray[i]
                    continue
                if value_5 == 0.0:
                    value_5 = self.highlowArray[i]
                    continue
                if value_6 == 0.0:
                    value_6 = self.highlowArray[i]
                    continue

            if value_6 != 0.0:
                break


        value_price = 0.0
        need_handle = True

        if self.highlowArray[-1] == 0.0:
            ##如果第一个不为高低点，拿当前价格算第1个高低点
            if self.sarArray[-1] > self.highArray[-1]:  ## 下降
                value_price = self.lowValue ##拿当前的最低价，有可能之前已经破掉
                if value_price < value_2 and value_price < value_4:
                    need_handle = False



            elif self.sarArray[-1] < self.lowArray[-1] :  ##上升
                value_price = self.highValue
                if value_price > value_2 and value_price > value_4:
                    need_handle = False


        if need_handle == True:
            self.sarType = self.calSarTypeDetail(value_1,value_2,value_3,value_4,value_5,value_6)
        else:
            self.sarType = self.calSarTypeDetail(value_price,value_1,value_2,value_3,value_4,value_5)




    def calSarTypeDetail(self,value_1,value_2,value_3,value_4,value_5,value_6):

        sarType_tmp = 0 ##重新初始化


        ### 4个点判断上升 下降旗形
        if value_1 > value_3 and value_2 > value_4 :#上升
            sarType_tmp = 1

        elif value_1 < value_3 and value_2 < value_4 : #下降
           sarType_tmp = 2


        #### 5个点判断头肩顶和头肩底
        elif value_3 > value_1 and value_3 > value_5 and value_2 < value_1: #头肩顶
            sarType_tmp = self.SAR_TYPE_HEAD_UP


        elif value_3 < value_1 and value_3 < value_5 and value_2 > value_1:  #头肩低
            sarType_tmp = self.SAR_TYPE_HEAD_DOWN


        ####6个点判断上升 下降旗形
        elif value_1 > value_5 and value_2 > value_6 :#上升
            sarType_tmp = 1


        elif value_1 < value_5 and value_2 < value_6 : #下降
            sarType_tmp = 2


        return sarType_tmp


    def sarTypeCalOpt(self):
        ## 判断sar类型
        #取最后6个高低点
        value_1 = 0.0
        value_2 = 0.0
        value_3 = 0.0
        value_4 = 0.0
        value_5 = 0.0
        value_6 = 0.0

        for i in range(0, self.highlowArray.__len__())[::-1]:
            if self.highlowArray[i] != 0.0:
                if value_1 == 0.0:
                    value_1 = self.highlowArray[i]
                    continue
                if value_2 == 0.0:
                    value_2 = self.highlowArray[i]
                    continue
                if value_3 == 0.0:
                    value_3 = self.highlowArray[i]
                    continue
                if value_4 == 0.0:
                    value_4 = self.highlowArray[i]
                    continue
                if value_5 == 0.0:
                    value_5 = self.highlowArray[i]
                    continue
                if value_6 == 0.0:
                    value_6 = self.highlowArray[i]
                    continue

            if value_6 != 0.0:
                break

        self.sarType = 0 ##重新初始化
        self.optForType = 0


        ### 4个点判断上升 下降旗形
        if value_1 > value_3 and value_2 > value_4 :#上升
            self.sarType = 1
            self.optForType = 1

        elif value_1 < value_3 and value_2 < value_4 : #下降
            self.sarType = 2
            self.optForType = 2


        #### 5个点判断头肩顶和头肩底
        elif value_3 > value_1 and value_3 > value_5 and value_2 < value_1: #头肩顶
            self.sarType = self.SAR_TYPE_HEAD_UP


        elif value_3 < value_1 and value_3 < value_5 and value_2 > value_1:  #头肩低
            self.sarType = self.SAR_TYPE_HEAD_DOWN


        ####6个点判断上升 下降旗形
        elif value_1 > value_5 and value_2 > value_6 :#上升
            self.sarType = 1


        elif value_1 < value_5 and value_2 < value_6 : #下降
            self.sarType = 2

        #if self.name == "day":
            #print("cal type" + str(value_1)+ str(value_2)+str(value_3)+str(value_4)+str(value_5)+str(value_6))
            #print ("call type type"+str(self.sarType))

    #-----------------------------------
    def updateTick(self, tick):
        if self.barIsEnd :

            self.highArray.append(tick.lastPrice)
            self.lowArray.append(tick.lastPrice)

            currentLong = self.isLong
            ##更新bar后更新sar array和高低点
            self.sarArray = self.SAR()

            self.calHighLow()
            self.sarTypeCal()
            self.barIsEnd = False

            if currentLong != self.isLong : ##第一更bar反转
                self.revSarValue = tick.lastPrice
                self.isJump(tick.lastPrice,tick.lastPrice)
                return True

        ##更新sar指标中途数据  还没有考虑多次反转问题
        sarValue = (self.sarArray[-1])
        if self.highlowArray[-1] != 0.0 :
            sarValue = self.highlowArray[-1]

        isReverse = False
        ##中途反转，
        if self.isLong == 0 and tick.lastPrice > sarValue:

            self.highlowArray[-1] = self.lowValue  ##

            self.isLong = 1
            self.sarTypeCal()
            isReverse = True

        elif self.isLong == 1 and tick.lastPrice < sarValue:
            self.highlowArray[-1] = self.highValue  ##

            self.isLong = 0
            self.sarTypeCal()
            isReverse = True



        ##如果前2个都是突破点，不下单 ，就是3个突破点在一起
        if isReverse == True:
            if self.highlowArrayPre[-2] != 0.0 and self.highlowArrayPre[-3] != 0.0:
                return  False

        if isReverse == True:
            self.revSarValue = tick.lastPrice
            self.isJump(tick.lastPrice,tick.lastPrice)

        return isReverse


    ##中途更新数据
    def updateMiddleBar(self, bar):

        if self.barIsEnd :

            if bar.date == "20150716" and self.name == "day":
                bacd =2

            self.lowArray.append(bar.low)
            self.highArray.append(bar.high)

            currentLong = self.isLong
            ##更新bar后更新sar array和高低点
            self.sarArray = self.SAR()
            self.calHighLow()
            self.sarTypeCal()
            self.barIsEnd = False

            if currentLong != self.isLong : ##第一更bar反转
                self.revSarValue = self.sarArray[-2]
                self.isJump(bar.high,bar.low)
                return True

        if bar.date == "20150716" and self.name == "hour":
            bacd =2

        ##更新sar指标中途数据
        sarValue = self.sarArray[-1]
        if self.highlowArray[-1] != 0.0 :  ##如果sar已经反转，取反转值
            sarValue = self.highlowArray[-1]

        isReverse = False
        ##中途反转，
        if self.isLong == 0 and bar.high >= sarValue:
            self.highlowArray[-1] = self.lowValue  ##
            self.isLong = 1
            self.sarTypeCal()
            isReverse = True

        elif self.isLong == 1 and bar.low <= sarValue:
            self.highlowArray[-1] = self.highValue  ##
            self.isLong = 0
            self.sarTypeCal()
            isReverse = True

        ##如果前2个都是突破点，不下单 ，就是3个突破点在一起
        if isReverse == True:
            if self.highlowArrayPre[-2] != 0.0 and self.highlowArrayPre[-3] != 0.0:
                return  False

        if isReverse == True:
            self.revSarValue = self.sarArray[-1]
            self.isJump(bar.high,bar.low)

        return isReverse

    #判断跳空
    def isJump(self,high,low):  ##一般都是开盘跳空
        if self.isLong == 0 and self.revSarValue > high :
            self.revSarValue = high

        if self.isLong == 1 and self.revSarValue < low:
            self.revSarValue = low


    ##sar 大的上涨或者下降趋势  这个方法需要在调用判断旗形并反方向后后判断
    def sarBigUpOrDown(self):
        """上升旗形  sar突破前sar比前高 高"""
        value_1 = 0.0
        pre_index = 0
        value_2 = 0.0
        value_3 = 0.0

        for i in range(0, self.highlowArray.__len__())[::-1]:
            if self.highlowArray[i] != 0.0:
                if value_1 == 0.0:
                    value_1 = self.highlowArray[i]
                    pre_index = i
                    continue
                if value_2 == 0.0:
                    value_2 = self.highlowArray[i]
                    continue
                if value_3 == 0.0:
                    value_3 = self.highlowArray[i]
                    continue

            if value_3 != 0.0:
                break

        if self.isLong == False and self.sarArray[pre_index-1] > value_3:
            return True

        if self.isLong == True  and self.sarArray[pre_index-1] < value_3:
            return True

        return False



