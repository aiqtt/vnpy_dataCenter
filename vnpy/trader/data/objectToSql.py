# encoding: UTF-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')




from datetime import datetime

SQL_TABLENAME_STOP_ORDER = "t_stop_order"    ##停止单

def getInsertSql(tableName,obj,accountId):
    if tableName == SQL_TABLENAME_STOP_ORDER:
        ##stop order
        timenow = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        sql = 'insert into t_stop_order(accountId,direction,price,symbol,status, volume,offset,createTime,updateTime) ' \
              "values('%s','%s',%f,'%s','%s','%s','%s','%s','%s')"\
              %(accountId,obj.direction,obj.price,obj.vtSymbol,obj.status,obj.volume,obj.offset,timenow,timenow)

        return sql

def getDeleteSql(tableName,obj,accountId):
    if tableName == SQL_TABLENAME_STOP_ORDER:
        ##stop order

        sql = "delete from t_stop_order where accountId ='%s' and symbol='%' and direction='%s' "\
              %(accountId,obj.vtSymbol, obj.direction)

        return sql

def getSelectSql(tableName,obj,accountId, ret_type):
    if tableName == SQL_TABLENAME_STOP_ORDER:
        if ret_type == "all":
            sql = "select * from t_stop_order where accountId ='%s'  "\
                %(accountId)

            return  sql




if __name__ == '__main__':
    getInsertSql(SQL_TABLENAME_STOP_ORDER,None,"")
