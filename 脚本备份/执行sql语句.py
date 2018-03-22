#!/usr/bin/env python
#coding:utf8

import MySQLdb as mdb


def Write_file(shuzi):
    with open("/tmp/zhangqidong/mid_date.txt","a+") as f:
        f.writelines(shuzi+"\n")

def Excute_sql(sql_command):
    con = None
    try:
        # 连接 mysql 的方法： connect('ip','user','password','dbname')
        con = mdb.connect('localhost', 'jtyl', 'aoshujtyl666', 'jtyl')
        # 所有的查询，都在连接 con 的一个模块 cursor 上面运行的
        cur = con.cursor()
        num_task = {}
        # 执行一个查询
        cur.execute(sql_command)
        results = cur.fetchall()
        for row in results:
            fname = str(row[0])
            lname = str(row[1])
            num_task[fname] = lname
        #   print "%s\t%s" % (fname, lname)
            #Write_file(fname + "\t" + lname)
        Write_file(str(num_task))
        #Write_file(num_task.keys() + "\t" + num_task.values())
    finally:
        if con:
            con.close()


if __name__ == "__main__":


    sql_comand2 = str().split('[')[1].split(']')[0]
    sql2 = "select d.level, count(*) as count from (select p.id,pa.level from player_attr as pa, player as p where p.id=pa.player_id and p.username in (" + sql_comand2 + ")) as d group by d.level;"
    Excute_sql(sql2)









####################################################################
