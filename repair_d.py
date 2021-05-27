# -*- coding: utf-8 -*-
"""
function：check data and extract miss it

@author：lezhipeng

email: 2399809590@qq.com

date:2021/5/24
"""

import pandas as pd
import os
import pymysql
import numpy as np
from pyhive import hive
from impala.dbapi import connect
import logging
import datetime as dt


sql_bigdata = """
SELECT id FROM  ylyh__view_cn.ods__log where e = "{table}" and 
dt = '{date}' and server='{server_id}'
"""
sql_ods = """ select id from {table_}
where substring(log_time,1,10)='{date}'"""
#between a and b
sql_ods_e = """ select * from {table_} where substring(log_time,1,10)= '{date}' and log_server={server_id} and id in {sub_id}"""

# yx_log5636_backup
db = pymysql.connect(host="172.16.16.132", user="maxwell", port=3336,
                         password="maxwell",db="yx_log22940_backup",
                         charset="utf8")
cursor = db.cursor()
conn = connect(host='172.16.18.27', port=21050)
path_base = os.path.abspath(".")

def main(table,sql_ods_e,date,server_id,sub_id,data_num):
    if data_num <= 0:
        print('服务端数据 <= 大数据端')
        return '无需补充'
    elif data_num == 1:
        sub_id = '({})'.format(sub_id[0])
    sql_ods_e1 = sql_ods_e.format(table_=table,date=date,server_id=server_id,sub_id=sub_id)
    data_ods_e1 = pd.read_sql(sql_ods_e1,db)
    data_ods_e1['id'] = data_ods_e1.id.astype('str')
    data_ods_e1['log_time'] = data_ods_e1.log_time.astype('str')
    data_ods_e1.log_type.fillna(9999999, inplace=True)
    data_ods_e1['log_type'] = data_ods_e1.log_type.astype('int64')
    data_ods_e1.log_tag.fillna(9999999,inplace=True)
    data_ods_e1['log_tag'] =  data_ods_e1.log_tag.astype('int64')
    data_ods_e1['log_name'] = data_ods_e1.log_name.astype('str')
    data_ods_e1.log_server.fillna(9999999,inplace=True)
    data_ods_e1['log_server'] = data_ods_e1.log_server.astype('int64')
    data_ods_e1.log_level.fillna(9999999,inplace=True)
    data_ods_e1['log_level'] = data_ods_e1.log_level.astype('int64')
    data_ods_e1['log_previous'] = data_ods_e1.log_previous.astype('str')
    data_ods_e1['log_now'] = data_ods_e1.log_now.astype('str')
    data_ods_e1['log_relate'] = data_ods_e1.log_relate.astype('str')
    data_ods_e1['log_channel'] = data_ods_e1.log_channel.astype('str')
    data_ods_e1['f1'] = data_ods_e1.f1.astype('str')
    data_ods_e1['f2'] = data_ods_e1.f2.astype('str')
    data_ods_e1['f3'] = data_ods_e1.f3.astype('str')
    data_ods_e1['f4'] = data_ods_e1.f4.astype('str')
    data_ods_e1['f5'] = data_ods_e1.f5.astype('str')
    data_ods_e1['f6'] = data_ods_e1.f6.astype('str')
    data_ods_e1['f7'] = data_ods_e1.f7.astype('str')
    data_ods_e1.f8.fillna(9999999,inplace=True)
    data_ods_e1['f8'] = data_ods_e1.f8.astype('int64')
    data_ods_e1.log_data.fillna(9999999,inplace=True)
    data_ods_e1['log_data'] = data_ods_e1.log_data.astype('int64')
    data_ods_e1.log_result.fillna(9999999,inplace=True)
    data_ods_e1['log_result'] = data_ods_e1.log_result.astype('int64')
    data_ods_e1['server'] = str(server_id)
    return data_ods_e1
def load_data(data_ods_e1,date,table,data_num):
    path = os.path.join(path_base, 'res_data/add_{}.parquet'.format(data_num))
    data_ods_e1.to_parquet(path, compression='snappy',index=None)
    # path = '/data/jupyter/lzp/Jasonle/repair_data/res_data/add_{}.csv'.format(data_num)
    # data_ods_e1.to_csv(path,index=None,header=None)
    load_sql = "load data local inpath '{path}' into table game_data_mining.ods__log1 partition(v='cn',dt='{date}',e='{table}')".format(path=path,date=date,table=table)
    load_sql1 = """hive -e "{load_sql}" """.format(load_sql=load_sql)
    os.system(load_sql1)
    return 'load sucessful!'

def run(data):
    for item in data.values:
        table = item[1]
        date = item[0]
        server_id= item[2]
        sql_ods1 = sql_ods.format(table_=table,date=date)
        sql_bigdata1 = sql_bigdata.format(table=table,date=date,server_id=server_id)
        data_ods = pd.read_sql(sql_ods1,db)
        data_bigdata = pd.read_sql(sql_bigdata1,conn)
        sub_id = tuple(set(data_ods.id.astype('int64').values)- set(data_bigdata.id.astype('int64').values))
        data_num = len(sub_id)
        data_ods_e1 = main(table,sql_ods_e,date,server_id,sub_id,data_num)
        if len(data_ods_e1) == 0:
            print(item)
            continue
        else:

            load_data(data_ods_e1,date,table,data_num)
            sql = """refresh game_data_mining.ods__log1 partition(v = 'cn',e='{table}',dt='{date}')""".format(table=table,date=date)
            cur = conn.cursor()
            cur.execute(sql)
            cur.close()
            print('更新完成')
            logger.info('更新完成:{date},表名:{table},补数{data_num}'.format(date=date,table=table,data_num=data_num))
            # logger.info(table,len(sub_id))
            print(date,table,len(sub_id))
            data_bigdata1 = pd.read_sql(sql_bigdata1, conn)
            num.append(len(data_bigdata1))
    # data['大数据补数后确认'] = num
    logger.info('确认完成')

    return '校验并提取完成!'

if __name__ == '__main__':
    num = []
    data = pd.read_excel('服22940.xlsx')
    logging.basicConfig(level=logging.INFO,
                        filename='/data/game_data_mining/lzp/projects/repair_data/log/repair_{server_id}.log'.
                        format(server_id = '服22940'), format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    # database='yx_log22940_backup'
    run(data)
    # data1.to_excel('服5636_补数后确认.xlsx')

