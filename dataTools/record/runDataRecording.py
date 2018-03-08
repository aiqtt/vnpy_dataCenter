# encoding: UTF-8

import multiprocessing
from datetime import datetime, time
from time import sleep

from vnpy.event import EventEngine2
from vnpy.trader.app import dataRecorder
from vnpy.trader.gateway import ctpGateway
from vnpy.trader.vtEngine import MainEngine, LogEngine
from vnpy.trader.vtEvent import EVENT_LOG
from vnpy.trader.vtFunction import sendRecordErrorEmail

#监控写文件的时间格式
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# 创建日志引擎
le = LogEngine()
le.setLogLevel(le.LEVEL_INFO)
le.addConsoleHandler()
#----------------------------------------------------------------------
def runChildProcess():
    """子进程运行函数"""

    le.info(u'启动行情记录运行子进程')
    
    ee = EventEngine2()
    le.info(u'事件引擎创建成功')
    
    me = MainEngine(ee, le)
    me.addGateway(ctpGateway)
    me.addApp(dataRecorder)
    le.info(u'主引擎创建成功')

    ee.register(EVENT_LOG, le.processLogEvent)
    le.info(u'注册日志事件监听')

    me.connect('CTP', dataRecorder.appName)
    le.info(u'连接CTP接口')

    while True:
        #向文件写时间
        write_time()
        sleep(5)

#----------------------------------------------------------------------
def runParentProcess():
    """父进程运行函数"""
    le.info(u'启动行情记录守护父进程')
    
    DAY_START = time(8, 57)         # 日盘启动和停止时间
    #中午不能停止子进程
    DAY_END = time(15, 18)
    NIGHT_START = time(20, 57)      # 夜盘启动和停止时间
    NIGHT_END = time(2, 33)
    
    p = None        # 子进程句柄

    while True:
        currentTime = datetime.now().time()
        recording = False

        # 判断当前处于的时间段
        #下午3点后，凌晨2点半，停子进程
        if ((currentTime >= DAY_START and currentTime <= DAY_END) or
            (currentTime >= NIGHT_START) or
            (currentTime <= NIGHT_END)):
            recording = True
            
        # 过滤周末时间段：周六全天，周五夜盘，周日日盘
        if ((datetime.today().weekday() == 6) or 
            (datetime.today().weekday() == 5 and currentTime > NIGHT_END) or 
            (datetime.today().weekday() == 0 and currentTime < DAY_START)):
            recording = False

        # 记录时间则需要启动子进程
        if recording and p is None:
            le.info(u'启动子进程')
            write_empty()
            p = multiprocessing.Process(target=runChildProcess)
            p.start()
            le.info(u'子进程启动成功')

        # 非记录时间则退出子进程
        if not recording and p is not None:
            le.info(u'关闭子进程')
            p.terminate()
            p.join()
            p = None
            write_empty()
            le.info(u'子进程关闭成功')

        sleep(5)
        if recording and p is not None:
            #只在子进程运行期间监控
            read_time()


#----------------------------------------------------------------------
def write_time():
    #向文件写时间
    fileName = 'watch_time.txt'
    with open(fileName, 'w') as f:
        timeStr = datetime.now().strftime(TIME_FORMAT)
        f.write(timeStr)
        f.close()

#----------------------------------------------------------------------
def write_empty():
    #向文件写时间
    fileName = 'watch_time.txt'
    with open(fileName, 'w') as f:
        f.write("")
        f.close()

#----------------------------------------------------------------------
def read_time():
    fileName = 'watch_time.txt'
    with open(fileName, 'r') as f:
        timeStr = f.read()
        f.close()
        if timeStr !=  None and timeStr != '':
            timeRead = datetime.strptime(timeStr, TIME_FORMAT)
            #计算相差的分钟数
            timeWatch = (datetime.now() - timeRead).seconds
            if timeWatch > 300:
                sendRecordErrorEmail()

#----------------------------------------------------------------------
if __name__ == '__main__':
    runParentProcess()
