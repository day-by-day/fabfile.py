#!/usr/bin/env python
#coding:utf8

import os

#获取cwd目录下所有文件，包括子目录下文件（显示绝对路径）;将与log_name同名的文件追加到一个文件中
def Read_file(log_name,cwd):
    with open("/tmp/zhangqidong/"+log_name.split('.')[0]+".txt", "w") as f:
        f.writelines('')
    #列出目录下所有的文件（包括子目录），显示绝对路径
    all_file = []
    root_file = []
    #for root, dirs, files in os.walk('/tmp/jtyl'):
    for root, dirs, files in os.walk(cwd):
        for file in files:
            root_file = os.path.join(root, file)
            all_file.append(root_file)
    #将和需求文件的文件名相同的文件内容追加到一个文件中
    for fi in all_file:
        if os.path.basename(fi) == log_name:
        #if os.path.basename(fi) == 'CreateRole.log':
            CreateRole_file = open(fi,"r")
            lines = CreateRole_file.readlines()
            with open("/tmp/zhangqidong/"+log_name.split('.')[0]+".txt","a+") as f:
                f.writelines(lines)
            CreateRole_file.close()
    #CreateRole = open("/tmp/zhangqidong/CreateRole.txt","r")
    CreateRole = open("/tmp/zhangqidong/"+log_name.split('.')[0]+".txt", "r")
    file_txt = CreateRole.readlines()
    return file_txt
