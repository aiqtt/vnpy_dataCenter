# encoding: UTF-8

"""
包含一些开发中常用的函数
"""

import os
import decimal
import json
from datetime import datetime, time
import platform
from email.mime.text import MIMEText
import smtplib

MAX_NUMBER = 10000000000000
MAX_DECIMAL = 4


#----------------------------------------------------------------------
def safeUnicode(value):
    """检查接口数据潜在的错误，保证转化为的字符串正确"""
    # 检查是数字接近0时会出现的浮点数上限
    if type(value) is int or type(value) is float:
        if value > MAX_NUMBER:
            value = 0
    
    # 检查防止小数点位过多
    if type(value) is float:
        d = decimal.Decimal(str(value))
        if abs(d.as_tuple().exponent) > MAX_DECIMAL:
            value = round(value, ndigits=MAX_DECIMAL)
    
    return unicode(value)


#----------------------------------------------------------------------
def todayDate():
    """获取当前本机电脑时间的日期"""
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

# ----------------------------------------------------------------------
# 返回字符串形式的日期，如20171229
def todayDateStr():
    now_time = datetime.now()
    today_str = ''
    today_str += str(now_time.year)
    today_str += str(now_time.month)
    today_str += str(now_time.day)

    return today_str

# 图标路径
iconPathDict = {}

path = os.path.abspath(os.path.dirname(__file__))
for root, subdirs, files in os.walk(path):
    for fileName in files:
        if '.ico' in fileName:
            iconPathDict[fileName] = os.path.join(root, fileName)

#----------------------------------------------------------------------
def loadIconPath(iconName):
    """加载程序图标路径"""   
    global iconPathDict
    return iconPathDict.get(iconName, '')    
    


#----------------------------------------------------------------------
def getTempPath(name):
    """获取存放临时文件的路径"""
    tempPath = os.path.join(os.getcwd(), 'temp')
    if not os.path.exists(tempPath):
        os.makedirs(tempPath)
        
    path = os.path.join(tempPath, name)
    return path


# JSON配置文件路径
jsonPathDict = {}

#----------------------------------------------------------------------
def getJsonPath(name, moduleFile):
    """
    获取JSON配置文件的路径：
    1. 优先从当前工作目录查找JSON文件
    2. 若无法找到则前往模块所在目录查找
    3.再找上级目录
    """
    currentFolder = os.getcwd()
    currentJsonPath = os.path.join(currentFolder, name)
    if os.path.isfile(currentJsonPath):
        jsonPathDict[name] = currentJsonPath
        return currentJsonPath
    
    moduleFolder = os.path.abspath(os.path.dirname(moduleFile))
    moduleJsonPath = os.path.join(moduleFolder, '.', name)
    if os.path.isfile(moduleJsonPath):
        jsonPathDict[name] = moduleJsonPath
        return moduleJsonPath

    currentParentPath = os.path.dirname(moduleFolder)
    jsonPath = os.path.join(currentParentPath, '.', name)
    if os.path.isfile(jsonPath):
        jsonPathDict[name] = jsonPath
        return jsonPath

#----------------------------------------------------------------------
def convertFloatMaxValue(f):
    """
    处理ctp接口返回的数据(主要是各种价格)有1.7976931348623157e+308的问题
    """
    float1 = float(f)
    if float1 == float('1.7976931348623157e+308'):
        return 0.0
    else:
        return float1


# ----------------------------------------------------------------------
def generateTickId(tickSymbol):
    """
    根据tick的Symbol生成对应的连续数字id
    """
    pass

def getPlatform():
    return platform.system()

def sendRecordErrorEmail():
    subject = "[aiqtt]data record maybe occur  exception"
    content = "数据记录进程可能发生了异常，请速速前去检查."
    send_email(subject, content)

def send_email(subject, content):
    # 发送方邮箱
    msg_from = "1323826903@qq.com"
    # 填入发送方邮箱的授权码
    passwd = "ggswtqjnhdgljjjj"
    # 收件人邮箱
    msg_to = "250401364@qq.com"

    msg = MIMEText(content)
    msg['Subject'] = subject
    msg['From'] = msg_from
    msg['To'] = msg_to

    try:
        # 邮件服务器及端口号
        s = smtplib.SMTP_SSL("smtp.qq.com", 465)
        s.login(msg_from, passwd)
        s.sendmail(msg_from, msg_to, msg.as_string())
        print "semd a error email"
    except s.SMTPException, e:
        print "send email error"
    finally:
        s.quit()

#------------------------------------------------
#累计自然时间的分钟数,传入参数是bar的datetime的time
#用于计算6,7,8,9分钟的bar
def grandMinutes(bar_datetime):
    bar_time = time(bar_datetime.hour, bar_datetime.minute)
    bar_hour = bar_time.hour
    grand_mins = 0
    if bar_hour == 9:
        #从9点开始计算
        grand_mins = bar_time.minute
    elif bar_hour == 10 and bar_time <= time(10, 15):
        grand_mins = 60 + bar_time.minute
    elif bar_hour == 10 and bar_time >= time(10, 30):
        #因10:15到10:30休市要减掉15分钟
        grand_mins = 45 + bar_time.minute
    elif bar_hour == 11 or bar_hour == 13:
        #中午不间断
        grand_mins = 105 + bar_time.minute
    elif bar_hour == 14:
        grand_mins = 165 + bar_time.minute
    elif bar_hour == 21:
        #夜盘开盘重新开始累计
        grand_mins = bar_time.minute
    elif bar_hour == 22:
        grand_mins = 60 + bar_time.minute
    elif bar_hour == 23:
        grand_mins = 120 + bar_time.minute
    elif 0 <= bar_hour <= 2:
        #0-2点的夜盘继续累计
        grand_mins = 180 + bar_hour * 60 + bar_time.minute

    return grand_mins

#------------------------------------------------
#期权使用
#累计自然时间的分钟数,传入参数是bar的datetime的time
#用于计算6,7,8,9分钟的bar
def op_grandMinutes(bar_datetime):
    bar_time = time(bar_datetime.hour, bar_datetime.minute)
    bar_hour = bar_datetime.hour
    grand_mins = 0
    if bar_hour == 9:
        #从9:30点开始计算
        grand_mins = bar_time.minute - 30
    elif bar_hour == 10:
        grand_mins = 30 + bar_time.minute
    elif bar_hour == 11:
        grand_mins = 90 + bar_time.minute
    elif bar_hour == 13 or bar_hour == 14:
        grand_mins = 120 + bar_time.minute

    return grand_mins

if __name__ == '__main__':
    print convertPriceLenght()