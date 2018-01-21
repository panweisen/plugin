#!/usr/bin/python
#-*- coding:utf-8 -*-
"""
Created on Wed Jan.03 2018

@author: Wilson

@function:
   Picus plugin
"""
__author__ = 'Wilson'
import sys
import xlrd
import time
import random
from picus_db_manage import PicusDBManage


if __name__=='__main__':
	db_manage = PicusDBManage()
	workbook = xlrd.open_workbook('jingbai_for_demo_v3.xls', on_demand = True)
	sheet = workbook.sheet_by_index(0)
	max_row_num = sheet.nrows
	max_col_num = sheet.ncols
	# print max_row_num
	# print max_col_num
	round_cnt = 0
	raw_cnt = 0
	raw_data_list = []
	# brand = 'jingbai'
	while True:
		for rx in range(1, max_row_num):
			raw_data_list[:] = []
			for cx in range(1, max_col_num):
				cell_data = sheet.cell(rx, cx).value
				if cell_data == 'undefined':
					raw_data_list.append(float(0))
				else:
					raw_data_list.append(float(cell_data))
			raw_cnt = raw_cnt + 1
			# print raw_data_list
			db_manage.insert_raw_data(raw_data_list)
			print("Insert {0}th data to influxdb, Round {1}".format(raw_cnt, round_cnt))
		round_cnt = round_cnt + 1
		raw_cnt = 0
	print "done"