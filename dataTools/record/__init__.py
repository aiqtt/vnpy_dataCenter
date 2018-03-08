# encoding: UTF-8

"""

1.历史数据目录格式说明：
tick_bar            #数据根目录
|__rb                   #品种名称（多个）
    |__1min             #1min-bar目录
        |__rb1801.txt   #bar文件（多个）
    |__3min             #3min-bar目录
        |__rb1801.txt
    |__5min
        |__rb1801.txt
    |__15min
        |__rb1801.txt
    |__30min
        |__rb1801.txt
    |__60min
        |__rb1801.txt
    |__day
        |__rb1801.txt
    |__tick             #转换格式后的历史tick数据
        |__rb1801.txt
    |__tick_history     #历史tick数据
        |__rb1801.txt

"""