# encoding: UTF-8

"""
给mongodb建索引
"""

from pymongo import ASCENDING

from dataTools.dtFunctions import loadSetting, getServerMongoConnect
from vnpy.trader.app.dataRecorder.drBase import *

WORK_MODE_LOCAL = 'local'
WORK_MODE_SERVER = 'server'

settings = loadSetting()
serverMongoClient = getServerMongoConnect(settings)

# 给一个品种的所有xmin的库的合约加索引
def makeOneSymbolAllIndex(symbol):
    #tick
    db = serverMongoClient[TICK_DB_NAME]
    col = db[symbol]
    try:
        col.create_index([("datetime", ASCENDING)], background=True)
        print "tick, %s create datetime index" % (symbol)
    except Exception, Argument:
        print "Exception:%s" % Argument

    #xmin-bar
    for xmin in MINUTE_TO_DB_NAME:
        db = MINUTE_TO_DB_NAME[xmin]
        col = db[symbol]
        try:
            col.create_index([("datetime", ASCENDING)], background=True)
            print "%s bar, %s create datetime index" % (xmin, symbol)
        except Exception, Argument:
            print "Exception:%s" % Argument

if __name__ == '__main__':
    """
    使用之前先在mongo shell中检查index是否创建
    db.getCollection('hc1801').getIndexes()
    """
    makeOneSymbolAllIndex('jm1801')
    makeOneSymbolAllIndex('jm1802')
    makeOneSymbolAllIndex('jm1803')
    makeOneSymbolAllIndex('jm1804')
    makeOneSymbolAllIndex('jm1805')
    makeOneSymbolAllIndex('jm1806')
    makeOneSymbolAllIndex('jm1807')
    makeOneSymbolAllIndex('jm1808')
    makeOneSymbolAllIndex('jm1809')
    makeOneSymbolAllIndex('jm1810')
    makeOneSymbolAllIndex('jm1811')
    makeOneSymbolAllIndex('jm1812')