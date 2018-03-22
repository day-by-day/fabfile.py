#!/usr/bin/env python
#coding:utf8


def Write_excel(timer_str):
    f = xlwt.Workbook(encoding = 'utf-8')  # 创建工作簿

    '''
    创建第一个sheet:
    sheet1
    '''
    fi = open("/tmp/zhangqidong/mid_date.txt", "r")
    date = fi.readlines()


    fi2 = open("/tmp/zhangqidong/ItemBuy.txt","r")
    date2 = fi2.readlines()

    fi3 = open("/tmp/zhangqidong/ZuanshiUse.txt","r")
    date3 = fi3.readlines()

    sheet1 = f.add_sheet(u'sheet1', cell_overwrite_ok=True)  # 创建sheet
    column0 = [u'新增玩家数', u'老玩家数', u'次日留存玩家数', u'3日留存玩家数', u'7日留存玩家数', u'新增付费玩家数', u'老付费玩家数', u'收入']
    # column1 = [u'定义', u'当日新增加的玩家账户数量', u'当日新增的玩家中次日还有登录游戏的玩家账户数量', u'首次付费发生在当日的玩家账户数量', u'当日之前有付费并且当日有付>费的玩家账户数量',u'当日总充值的金额']
    # 生成第一列
    for j in range(0, len(column0)):
        sheet1.write(j, 0, column0[j], Set_style('Times New Roman', 220, True))
    # 生成第二列
    for i in range(0, len(column0)):
        sheet1.write(i, 1, date[i].strip('\n'))

    sheet2 = f.add_sheet(u'玩家首次付费等级分布', cell_overwrite_ok=True)
    sheet2.write(0, 0, u'首次付费玩家等级', Set_style('Times New Roman', 220, True))
    sheet2.write(0, 1, u'数量', Set_style('Times New Roman', 220, True))
    player_class2 = eval(date[8])
    i = 1
    for key in player_class2:
        sheet2.write(i, 0, key)
        sheet2.write(i, 1, player_class2[key])
        i += 1

    sheet3 = f.add_sheet(u'首次付费金额分布', cell_overwrite_ok=True)
    sheet3.write(0, 0, u'首次付费金额分布(元)', Set_style('Times New Roman', 220, True))
    sheet3.write(0, 1, u'数量', Set_style('Times New Roman', 220, True))
    player_class3 = eval(date[9])
    i = 1
    for key in player_class3:
        sheet3.write(i, 0, key)
        sheet3.write(i, 1, player_class3[key])
        i += 1

    sheet4 = f.add_sheet(u'付费金额分布', cell_overwrite_ok=True)
    sheet4.write(0, 0, u'付费金额分布(元)', Set_style('Times New Roman', 220, True))
    sheet4.write(0, 1, u'数量', Set_style('Times New Roman', 220, True))
    player_class4 = eval(date[10])
    i = 1
    for key in player_class4:
        sheet4.write(i, 0, key)
        sheet4.write(i, 1, player_class4[key])
        i += 1

    sheet5 = f.add_sheet(u'新增玩家等级分布', cell_overwrite_ok=True)
    sheet5.write(0, 0, u'新增玩家等级分布', Set_style('Times New Roman', 220, True))
    sheet5.write(0, 1, u'数量', Set_style('Times New Roman', 220, True))

    player_class5 = eval(date[11])
    i = 1
    for key in player_class5:
        sheet5.write(i, 0, key)
        sheet5.write(i, 1, player_class5[key])
        i += 1

    # sheet8 = f.add_sheet(u'新增玩家主线任务分布', cell_overwrite_ok=True)
    # sheet6.write(0, 0, u'人数', Set_style('Times New Roman', 220, True))
    # sheet6.write(0, 1, u'任务ID', Set_style('Times New Roman', 220, True))
    # player_class6 = date[12:]
    # i = 1
    # for key in player_class6:
    #     sheet6.write(i, 0, key.split('\t')[0])
    #     sheet6.write(i, 1, key.split('\t')[1])
    #     i += 1

    sheet6 = f.add_sheet(u'元宝消耗记录', cell_overwrite_ok=True)
    i = 0
    for line in date2:
        sheet6.write(i, 0, line.split('\t')[0])
        sheet6.write(i, 1, line.split('\t')[1])
        sheet6.write(i, 2, line.split('\t')[2])
        sheet6.write(i, 3, line.split('\t')[3])
        sheet6.write(i, 4, line.split('\t')[4])
        sheet6.write(i, 5, line.split('\t')[5])
        i += 1

    sheet7 = f.add_sheet(u'金币消耗记录', cell_overwrite_ok=True)
    i = 0
    for line in date3:
        sheet7.write(i, 0, line.split('\t')[0])
        sheet7.write(i, 1, line.split('\t')[1])
        sheet7.write(i, 2, line.split('\t')[2])
        sheet7.write(i, 3, line.split('\t')[3])
        sheet7.write(i, 4, line.split('\t')[4])
        i += 1
    fi.close()
    fi2.close()
    fi3.close()

    f.save('/tmp/zhangqidong/' + timer_str.strftime("%Y-%m-%d") + '.xlsx')  # 保存文件
