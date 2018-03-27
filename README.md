# vnpy_dataCenter

## 简介
- 本程序基于开源项目vnpy的基础上做了一定改动，增强了期货市场实时行情记录的功能，和bar转换的功能
- 安装和使用方式参考vnpy

## 修改项
### 期货市场数据记录
- DataRecord模块不再使用配置文件读取合约，直接对所有合约进行保存
- 增加tick保存到txt文件，每合约每天保存一份文件
### tick数据转换为bar数据
- 修改了BarManager，可以生成任意分钟bar
- BarManager分为两个，DataRecord中实时的，和txt文件转换的FileBarManager
- 历史tick文件的转换，需要预先创建规定的目录结构



