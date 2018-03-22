#!/usr/bin/env python
# coding:utf-8

"""
流程：
1、先创建一个临时数据库（test），将需要和过来的数据库导入进去。

需求1：            ok
合服过程中出现同名的玩家名称，将被合过来的玩家名称追加_2       player，tong
出现帮派同名，两个帮派都加后缀，然后给该帮派帮主发改名卡        tong

需求2：   --黄鑫，      需求已解决
money_tree_reward
假设 把game2合到game1, 只保留game1
找出game1和game2的 money_tree_reward  id最大那条
把game2那条的pool字段加到game1那条的pool字段
info字段[{game1的内容1}]  和 [{game2 的内容1}]合并成 [{game1的内容},{game2的内容}]
money_tree 表的话可以直接合并
tong_battle 开头的表 合区的时候全部清空

wealth 表的id已经做成全服唯一了，可以直接合--品龙  前面是几个服的话，需要手动，  以后的新服不会出现冲突
mysql -uroot -p'Jtyl2017!@#$'

CREATE TABLE `wealth1` (
  `id` varchar(40) NOT NULL,
  `player_id` varchar(20) NOT NULL,
  `wealth_id` int(4) NOT NULL,
  `gold` int(11) NOT NULL DEFAULT '0' COMMENT '获取金币数量',
  `time_id` int(4) NOT NULL DEFAULT '0' COMMENT '活动的时间段id',
  `create_time` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

#需要改成对应的serverID    3

INSERT INTO wealth1 SELECT CONCAT( 31,0,(@rownum := @rownum + 1)) as id, t1.player_id, t1.wealth_id, t1.gold, t1.time_id, t1.create_time FROM wealth  t1, (select @rownum := 0) r;
对比wealth和wealth1的记录数是否相等，
SELECT COUNT(*) FROM wealth;
SELECT COUNT(*) FROM wealth1;

DROP TABLE wealth;
RENAME TABLE wealth1 TO wealth;
如果相等的话，删除wealth表，   重命名wealth1为wealth表

竞技场清数据
delete from career_battle;


问题：
1、
"player_fashion",需要删除无效数据，在本库中对比player表，如果没有这个player_id，就删除
player_fashion表的player_id有重复，在两个库分别执行DELETE FROM player_fashion WHERE player_id IN(SELECT p.player_id FROM (SELECT player_id FROM player_fashion WHERE player_id NOT IN(SELECT id FROM player)) AS p);
2、发送的邮件中文乱码。
添加mdb.connect(charset='utf8')
3、帮派表中出现同名的帮主也要加_2
4、letters_player出现player_id为空,需要先清除掉        DELETE FROM letters_player WHERE player_id=''
5、title_fight表会展示机器战力，有的机器人的id一样门导致报错。    需要清掉r-开头的player_id
6、游戏内服务器等级不对，需要修改后台的开服时间

#用“_”连接两列数据             player表和tong表
UPDATE str(row[0]) SET `name`= CONCAT_WS('_',`name`,server_id) WHERE id IN(
SELECT id FROM(
SELECT id FROM str(row[0])
WHERE `name` IN(
select `name`
from str(row[0])
group by `name`
having count(*)>1) AND server_id=2) AS temp)
"""

import re
import time
import MySQLdb as mdb
import sys
reload(sys)
sys.setdefaultencoding('utf8')


def Excute_sql(sql_command, db1, db2, id1, id2):
    con = None
    try:
        # 连接 mysql 的方法： connect('ip','user','password','dbname',charset=)
        con = mdb.connect(host='localhost', user='root', passwd='Jtyl2017!@#$', db=db1, charset='utf8mb4')
        # 所有的查询，都在连接 con 的一个模块 cursor 上面运行的
        cur = con.cursor()

        # 先执行去除无效的player_id    --需要在合并player表之前执行，因为合并后就无法区分无效ID（这个无效可能存在于另一个表）故拿到循环外面先执行
        for disable in ["player_fashion"]:
            sql_brace1 = "DELETE FROM " + db1 + "." + str(
                disable) + " WHERE player_id IN(SELECT p.player_id FROM (SELECT player_id FROM " + db1 + "." + str(
                disable) + " WHERE player_id NOT IN(SELECT id FROM " + db1 + ".player)) AS p);"
            cur.execute(sql_brace1)
            con.commit()

            sql_brace2 = "DELETE FROM " + db2 + "." + str(
                disable) + " WHERE player_id IN(SELECT p.player_id FROM (SELECT player_id FROM " + db2 + "." + str(
                disable) + " WHERE player_id NOT IN(SELECT id FROM " + db2 + ".player)) AS p);"
            cur.execute(sql_brace2)
            con.commit()

        # 执行一个查询
        cur.execute(sql_command)
        results = cur.fetchall()
        for row in results:
            print str(row[0])

            #删除副表中player_id 为空的记录
            if str(row[0]) in ["player_newbie","guaji"]:
                sql_del_null1 = "DELETE FROM " + db2 + "." + str(row[0]) +" WHERE player_id=''"
                cur.execute(sql_del_null1)
                con.commit()

            if str(row[0]) in ["sequence","server_config"]:
                continue

            if str(row[0]) in ["money_tree_reward"]:

                # 获取主区最大id值
                sql_max_id = "SELECT id FROM " + str(db1) + "." + str(row[0]) + " ORDER BY CAST(id AS SIGNED INTEGER) DESC LIMIT 1"
                cur.execute(sql_max_id)
                max_id = str(cur.fetchall()).split("'")[1].split("'")[0]
                #print max_id

                #获取最大id值的info
                sql_max_info1 = "SELECT info FROM " + str(db1) + "." + str(row[0]) + " ORDER BY CAST(id AS SIGNED INTEGER) DESC LIMIT 1"
                cur.execute(sql_max_info1)
                max_info1 = str(cur.fetchall()).split('[')[1].split(']')[0].decode('unicode_escape')            #还有根据实际情况，如果该值为空，则再以（分割会报错
                #print max_info1
                # 获取最大id值的pool
                sql_max_pool1 = "SELECT pool FROM " + str(db1) + "." + str(row[0]) + " ORDER BY CAST(id AS SIGNED INTEGER) DESC LIMIT 1"
                cur.execute(sql_max_pool1)
                max_pool1 = str(cur.fetchall()).split('(')[2].split(')')[0].split('.')[0]
                #print max_pool1

                # 获取最大id值的info
                sql_max_info2 = "SELECT info FROM " + str(db2) + "." + str(row[0]) + " ORDER BY CAST(id AS SIGNED INTEGER) DESC LIMIT 1"
                cur.execute(sql_max_info2)
                max_info2 = str(cur.fetchall()).split('(')[2].split(')')[0].split('[')[1].split(']')[0].decode('unicode_escape')
                #print max_info2           #print max_info2.decode('utf-8')   无法解决，还是输出相应汉字的utf-16编码

                # 获取最大id值的pool
                sql_max_pool2 = "SELECT pool FROM " + str(db2) + "." + str(row[0]) + " ORDER BY CAST(id AS SIGNED INTEGER) DESC LIMIT 1"
                cur.execute(sql_max_pool2)
                max_pool2 = str(cur.fetchall()).split('(')[2].split(')')[0].split('.')[0]
                #print max_pool2

                sum_max_info=[]
                sum_max_pool=[]


                if int(max_pool2) == int(0):
                    continue
                else:
                    sum_max_pool = int(max_pool1) + int(max_pool2)
                    if int(max_pool1) == int(0):
                        sum_max_info.append(max_info2)
                    else:
                        sum_max_info.append(max_info1)
                        sum_max_info.append(max_info2)
                        #sum_max_info = [max_info1,max_info2]


                #print str(sum_max_info).decode('unicode_escape').replace("u","").replace("'","")            #.replace("[","\[").replace("]","\]").replace("{","\{").replace("}","\}")
                tree = str(sum_max_info).decode('unicode_escape').replace("u","").replace("'","")           #.replace("[","\[").replace("]","\]").replace("{","\{").replace("}","\}")
                print str(sum_max_pool)
                sql_update_money = "UPDATE " + str(db1) + "." + str(row[0]) + " SET pool=%s ,info=%s WHERE id=%s"
                print sql_update_money
                pool_tree = (str(sum_max_pool), tree, max_id)
                cur.execute(sql_update_money,pool_tree )                        #重要
                con.commit()

                continue

                #清空主表和副表  ----清空主表，然后不合并
            if "rank" in str(row[0]) or "tong_battle" in str(row[0]) or str(row[0]) in ["sell_item_price", "arena", "player_rename","career_battle"]:
                cur.execute("truncate table " + db1 + "." + str(row[0]))
                con.commit()
                continue

            # 副表id值加20000000，然后合并           如果为第一次和服就是10000000
            if str(row[0]) in ["auction_item","master_relation", "auction_item_log", "yunying_time"]:
                cur.execute("UPDATE " + db2 + "." + row[0] + " SET id=id+20000000")
                con.commit()

            # 副表rid值加20000000，然后合并
            if str(row[0]) in ["master_daily_target", "master_graduate_target"]:
                cur.execute("UPDATE " + db2 + "." + row[0] + " SET rid=rid+20000000")
                con.commit()

            if str(row[0]) in ["player"]:  # 先合并，然后找出重复的玩家名称，然后将合过来的重复玩家名称加上后缀
                sql_rename = "UPDATE " + str(row[
                                                 0]) + " SET `name`= CONCAT_WS('_',`name`,server_id) WHERE id IN(SELECT id FROM(SELECT id FROM " + str(
                    row[0]) + " WHERE `name` IN(select `name`from " + str(
                    row[0]) + " group by `name` having count(*)>1) AND server_id=" + str(id2) + ") AS temp)"
                cur.execute(
                    "INSERT INTO " + db1 + "." + str(row[0]) + " SELECT * FROM " + db2 + "." + str(row[0]))  # 合并操作
                con.commit()
                # ||查找合并表之后重名的玩家名称---需要在添加后缀之前操作
                global rename_account
                sql_rename_account = "SELECT id from " + db1 + "." + str(
                    row[0]) + " WHERE `name` IN(SELECT `name` from " + db1 + "." + str(
                    row[0]) + " GROUP BY `name` HAVING COUNT(*)>1) AND server_id=" + str(id2)
                cur.execute(sql_rename_account)
                rename_account = cur.fetchall()

                cur.execute(sql_rename)  # 重复玩家名称添加后缀
                continue

            # 1、写邮件
            # 2、群发给帮主                   没有同名的帮主也发了
            if str(row[0]) in ["letters"]:
                t = time.time()
                nowTime = lambda: int(round(t * 1000))
                #print nowTime

                letterID = (nowTime())
                sql_email1 = "insert into " + db1 + "." + "letters(id,letter_type,created_at,title,content,item_type,attach,level_limit,start_time,end_time,player_ids,sender) values(" + str(
                    letterID) + ",1,now(),'帮派改名卡','很抱歉，合服过程中发现同名帮派，为了保障合服的顺利进行，我们将你们两个帮派名中增加数字号码，为此我们为您赠上【帮派改名卡】。',1111,'[{\"am\":1,\"spId\":352107}]',0,now(),DATE_ADD(NOW(),INTERVAL 7 DAY),'',0);"
                print sql_email1
                cur.execute(sql_email1)
                con.commit()

                #cur.execute("SELECT owner_id FROM " + db1 + ".tong")
                global bangzhus
                cur.execute("SELECT owner_id FROM " + str(db1) + ".tong WHERE `name` IN(SELECT `name` FROM " + str(db2) + ".tong)")       #同名帮派的帮主id--主表
                bangzhus = cur.fetchall()
                for bz in bangzhus:                                                                                                       #同名帮派的帮主id--主表   --发邮件
                    print str(bz[0])
                    sql_email2 = "insert into " + db1 + "." + "letters_player(player_id,letter_id,status,expire) values(" + str(
                        bz[0]) + "," + str(letterID) + ",0,DATE_ADD(NOW(),INTERVAL 1 MONTH))"
                    print sql_email2
                    cur.execute(sql_email2)
                    con.commit()
                for db in [db1,db2]:
                    sql_del_null1 = "DELETE FROM " + db + "." + "letters_player WHERE player_id=''"
                    cur.execute(sql_del_null1)
                    con.commit()
                    sql_del_null2 = "DELETE FROM " + db + "." + "letters_last_check WHERE player_id=''"
                    cur.execute(sql_del_null2)
                    con.commit()


                ###############################################
                t2 = time.time()
                nowTime2 = lambda: int(round(t2 * 1000))
                #print nowTime2

                letterID2 = (nowTime2()) + 1
                sql_email3 = "insert into " + db2 + "." + "letters(id,letter_type,created_at,title,content,item_type,attach,level_limit,start_time,end_time,player_ids,sender) values(" + str(
                    letterID2) + ",1,now(),'帮派改名卡','很抱歉，合服过程中发现同名帮派，为了保障合服的顺利进行，我们将你们两个帮派名中增加数字号码，为此我们为您赠上【帮派改名卡】。',1111,'[{\"am\":1,\"spId\":352107}]',0,now(),DATE_ADD(NOW(),INTERVAL 7 DAY),'',0);"
                print sql_email3
                cur.execute(sql_email3)
                con.commit()

                #cur.execute("SELECT owner_id FROM " + db2 + ".tong")
                global bangzhus2
                cur.execute("SELECT owner_id FROM " + str(db2) + ".tong WHERE `name` IN(SELECT `name` FROM " + str(db1) + ".tong)")               #同名帮派的帮主id--副表
                bangzhus2 = cur.fetchall()
                for bz2 in bangzhus2:                                                                                                             #同名帮派的帮主id--副表   --发邮件
                    print str(bz2[0])
                    sql_email4 = "insert into " + db2 + "." + "letters_player(player_id,letter_id,status,expire) values(" + str(
                        bz2[0]) + "," + str(letterID2) + ",0,DATE_ADD(NOW(),INTERVAL 1 MONTH))"
                    print sql_email4
                    cur.execute(sql_email4)
                    con.commit()

                sql_del_null2 = "DELETE FROM " + db2 + "." + "letters_player WHERE player_id=''"
                cur.execute(sql_del_null2)
                con.commit()

            ##备注：第一次合并两表的时候需要，第二次合并第三个表的时候就需要注释掉，因为会导致主表又加上_1    其结果会导致后面的变成_1_1_1

            if str(row[0]) in ["tong"]:  #同名帮派-副表
                            #     cur.execute("SELECT owner_id FROM " + str(db1) + "." + str(row[0]) + " WHERE `name` IN(SELECT `name` FROM " + str(db2) + ".tong)")          #主表同名帮派的帮主id
                            #     rename_list1 = cur.fetchall()
                            #
                            #     cur.execute("SELECT owner_id FROM " + str(db2) + "." + str(row[0]) + " WHERE `name` IN(SELECT `name` FROM " + str(db1) + ".tong)")          #副表同名帮派的帮主id
                            #     rename_list2 = cur.fetchall()

                # 同名帮派-副表
                for rename2 in bangzhus2:
                    #print str(rename2[0])
                    sql_rename2 = "UPDATE " + str(db2) + "." + str(row[0]) + " SET `name`= CONCAT_WS('_',`name`," + str(id2) + ") WHERE owner_id =" + str(rename2[0])
                    #sql_rename2 = "UPDATE " + str(db2) + "." + str(row[0]) +" SET `name`= CONCAT_WS('_',`name`,"+ str(id2) +") WHERE `name` IN(SELECT `name` FROM(SELECT `name` FROM " + str(db2) + "." + str(row[0]) +" WHERE `name` IN(SELECT `name` FROM " + str(db1) + "." + str(row[0]) +" )) AS temp1)"
                    print sql_rename2
                    cur.execute(sql_rename2)
                    con.commit()

                # 同名帮派-主表
                for rename in bangzhus:
                    #####副表已经提交修改了，同帮派名字已经加上_1，   故这个查找找不到同名的。所以先把同名帮派保存到一个列表
                    sql_rename3 = "UPDATE " + str(db1) + "." + str(row[0]) + " SET `name`= CONCAT_WS('_',`name`," + str(id1) + ") WHERE owner_id =" + str(rename[0])
                    #sql_rename3 = "UPDATE " + str(db1) + "." + str(row[0]) + " SET `name`= CONCAT_WS('_',`name`," + str(id1) + ") WHERE `name` IN(SELECT `name` FROM(SELECT `name` FROM " + str(db1) + "." + str(row[0]) + " WHERE `name` IN(SELECT `name` FROM " + str(db2) + "." + str(row[0]) + " )) AS temp2)"
                    print sql_rename3
                    cur.execute(sql_rename3)
                    con.commit()

                #同名帮主-副表
                sql_rename5 = "UPDATE " + str(db2) +"."+str(row[0])+" SET owner_name= CONCAT_WS('_',owner_name,"+ str(id2) +") WHERE owner_name IN(SELECT owner_name FROM(SELECT owner_name FROM " + str(db2) +"." + str(row[0]) +" WHERE owner_name IN(SELECT owner_name FROM " + str(db1) + "." + str(row[0]) +")) AS temp3)"
                print sql_rename5
                cur.execute(sql_rename5)
                con.commit()

                cur.execute("INSERT INTO " + db1 + "." + str(row[0]) + " SELECT * FROM " + db2 + "." + str(row[0]))  # 合并
                con.commit()

                continue

            #机器人的player_id相同，导致主键重复
            if str(row[0]) in ["title_fight"]:
                for db in [db1,db2]:
                    sql_del_null3 = "DELETE FROM " + db + "." + "title_fight WHERE player_id LIKE 'r%'"
                    cur.execute(sql_del_null3)
                    con.commit()
                    sql_del_null4 = "DELETE FROM " + db + "." + "title_fight WHERE player_id LIKE 'r%'"
                    cur.execute(sql_del_null4)
                    con.commit()

            cur.execute("INSERT INTO " + db1 + "." + str(row[0]) + " SELECT * FROM " + db2 + "." + str(row[0]))
            con.commit()

    finally:
        if con:
            con.close()


if __name__ == "__main__":
    sql = "show tables;"
    masterdb = 'jtyl1'  # 主数据库名称              #即将jtyl2合并到jtyl1
    masterid = 1
    tempdb = 'jtyl3'  # 被合并的数据库名称
    tempid = 3
    Excute_sql(sql, masterdb, tempdb, masterid, tempid)




"""
            # #主副表加一个主键server_id，主表值默认为1，副表默认值为2
            # add_column_name = "server_id"
            # if str(row[0]) in ["user"]:          #,"user"表，已处理，如上。可直接合并
            #     continue

                # 主表server_id设置为1
                # cur.execute("ALTER TABLE db1." + str(row[0]) + " PRIMARY KEY;")       #删除主键
                # cur.execute("ALTER TABLE db1." + str(row[0]) + " ADD COLUMN " + add_column_name + " NOT NULL DEFAULT '1' AFTER username;")      #新增str(row[0])列，放置在主键username列后面
                # cur.execute("ALTER TABLE db1." + str(row[0]) + " ADD PRIMARY KEY(username," + add_column_name + ");")                           #设置复合主键
                # #副表server_id设置为2
                # cur.execute("ALTER TABLE db2." + str(row[0]) + " PRIMARY KEY;")       #删除主键
                # cur.execute("ALTER TABLE db2." + str(row[0]) + " ADD COLUMN " + add_column_name + " NOT NULL DEFAULT '2' AFTER username;")      #新增str(row[0])列，放置在主键username列后面
                # cur.execute("ALTER TABLE db2." + str(row[0]) + " ADD PRIMARY KEY(username," + add_column_name + ");")                           #设置复合主键


            #保留主表，清空副表
"""

