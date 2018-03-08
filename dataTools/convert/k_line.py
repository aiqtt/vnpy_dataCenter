# encoding: UTF-8
"""
生成K线数据
"""
import json
import os
from vnpy.trader.vtFunction import todayDate, getJsonPath
from dataTools.fileBarManager import FileBarManager

import csv

class k_line(object):

    settingFileName = 'DT_setting.json'
    settingFilePath = getJsonPath(settingFileName, __file__)

    def __init__(self):

        self.FilePath = 'E:/tick_bar/au/'
        # for test
        #self.FilePath = 'e:/temp/rb1805_20180205/'
        #self.loadSetting()

    #----------------------------------------------------------------------
    def loadSetting(self):
        """加载配置"""
        with open(self.settingFilePath) as f:
            drSetting = json.load(f)

            self.FilePath = drSetting['data_path']
            #if not os.path.exists(self.FilePath):
            #    os.mkdir(self.FilePath)


    # --------------------------------------------------------------
    def runConvert(self):
        """
        同时打开两个文件，进行读写
        """
        fieldnames = ['_id', 'gatewayName', 'symbol', 'exchange', 'vtSymbol', 'lastPrice', 'lastVolume', 'volume',
                     'openInterest', 'time', 'date', 'datetime', 'openPrice', 'highPrice', 'lowPrice', 'preClosePrice',
                     'upperLimit', 'lowerLimit', 'bidPrice1', 'bidPrice2', 'bidPrice3', 'bidPrice4', 'bidPrice5',
                     'askPrice1', 'askPrice2', 'askPrice3', 'askPrice4', 'askPrice5',
                     'bidVolume1', 'bidVolume2', 'bidVolume3', 'bidVolume4', 'bidVolume5',
                     'askVolume1', 'askVolume2', 'askVolume3', 'askVolume4', 'askVolume5',
                     'TradingDay', 'PreSettlementPrice', 'PreOpenInterest', 'ClosePrice', 'SettlementPrice',
                     'AveragePrice']

        varFieldNames = ['vtSymbol' , 'symbol', 'exchange', 'open', 'high', 'low' , 'close' , 'date', 'time', 'datetime', 'volume', 'openInterest', 'TradingDay']

        if os.path.exists(self.FilePath+'tick/'):
            listDir = os.listdir(self.FilePath+'tick/')#文件夹下可能会有多个文件
            for f in listDir:
                file1 = self.FilePath+'tick/' + f
                if os.path.isfile(file1):
                    print file1
                    symbol = os.path.basename(file1)  #文件名就是合约名字
                    #if os.path.isfile(self.FilePath+'1min/'+symbol) :  #如果文件存在，不处理
                    #    continue
                    with open(self.FilePath+'tick/'+symbol, 'rb') as tickFile:
                        # 处理合成K线
                        file_1min = open(self.FilePath+'1min/'+symbol, 'ab+')
                        file_3min = open(self.FilePath+'3min/'+symbol, 'ab+')
                        file_5min = open(self.FilePath+'5min/'+symbol, 'ab+')
                        file_6min = open(self.FilePath+'6min/'+symbol, 'ab+')
                        file_7min = open(self.FilePath+'7min/'+symbol, 'ab+')
                        file_8min = open(self.FilePath+'8min/'+symbol, 'ab+')
                        file_9min = open(self.FilePath+'9min/'+symbol, 'ab+')
                        file_10min = open(self.FilePath+'10min/'+symbol, 'ab+')
                        file_15min = open(self.FilePath+'15min/'+symbol, 'ab+')
                        file_30min = open(self.FilePath+'30min/'+symbol, 'ab+')
                        file_60min = open(self.FilePath+'60min/'+symbol, 'ab+')
                        file_day = open(self.FilePath+'day/'+symbol, 'ab+')

                        barmanger = FileBarManager()#xmins_normal, xmins_all
                        barmanger.setFileMode(barmanger.xmins_all,
                                              file_1min=csv.DictWriter(file_1min, varFieldNames),
                                              file_3min=csv.DictWriter(file_3min, varFieldNames),
                                              file_5min=csv.DictWriter(file_5min, varFieldNames),
                                              file_6min=csv.DictWriter(file_6min, varFieldNames),
                                              file_7min=csv.DictWriter(file_7min, varFieldNames),
                                              file_8min=csv.DictWriter(file_8min, varFieldNames),
                                              file_9min=csv.DictWriter(file_9min, varFieldNames),
                                              file_10min=csv.DictWriter(file_10min, varFieldNames),
                                              file_15min=csv.DictWriter(file_15min, varFieldNames),
                                              file_30min=csv.DictWriter(file_30min, varFieldNames),
                                              file_60min=csv.DictWriter(file_60min, varFieldNames),
                                              file_day=csv.DictWriter(file_day, varFieldNames))

                        dict_reader = csv.DictReader(tickFile, fieldnames=fieldnames)
                        for line in dict_reader:
                            #print( line['_id'])
                            barmanger.updateFileTick(line)
                        # 读文件结束后再调一次，防止最后一个bar不生成
                        barmanger.updateFileTick(None)
                        # 读文件结束后再调一次，推送最后一个日bar去保存
                        barmanger.clearEndBar()
                        file_1min.close()
                        file_3min.close()
                        file_5min.close()
                        file_6min.close()
                        file_7min.close()
                        file_8min.close()
                        file_9min.close()
                        file_10min.close()
                        file_15min.close()
                        file_30min.close()
                        file_60min.close()
                        file_day.close()
                print 'end fro file...'
            print 'end fro listDir...'


if __name__ == '__main__':
    print('---start convert ---')
    kLine = k_line()
    kLine.runConvert()
    print('----end----')