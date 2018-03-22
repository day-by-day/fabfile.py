#!/usr/bin/env python
#coding:utf8

import xlwt


def Set_style(name, height, bold=False):
    style = xlwt.XFStyle()  # 初始化样式

    font = xlwt.Font()  # 为样式创建字体
    font.name = name  # 'Times New Roman'
    font.bold = bold
    font.color_index = 4
    font.height = height

    borders= xlwt.Borders()
    borders.left= 6
    borders.right= 6
    borders.top= 6
    borders.bottom= 6

    style.font = font
    # style.borders = borders

    return style





#实例
sheet1.write(j, 0, column0[j], Set_style('Times New Roman', 220, True))