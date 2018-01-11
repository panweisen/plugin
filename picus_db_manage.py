#!/usr/bin/python
#-*- coding:utf-8 -*-
"""
Created on Wed Nov.20 2017

@author: Wilson

@function:
   Picus database management
"""
__author__ = 'Wilson'

from influxdb import InfluxDBClient
import time
import random

class PicusDBManage(object):
    def __init__(self):
		super(PicusDBManage, self).__init__()
		hostname = 'localhost'
		port_num = 8086
		db_user_name = 'root'
		db_password = 'root'
		database_name = 'picus_db'
		#setup the connection with influx db
		self.client = InfluxDBClient(hostname, port_num, db_user_name, db_user_name, database_name)
    
    #insert the new data to raw_data
    def insert_raw_data(self, raw_data_list):
		measurement = "raw_data"
		host_name = "13.56.155.255"
		region_value = "us_west"
		brand_value = raw_data_list[0]
		air_out_temp_value = raw_data_list[1]
		air_out_temp_changing_rate_value = raw_data_list[2]
		base_powder_temp_value = raw_data_list[3]
		base_powder_temp_changing_rate_value = raw_data_list[4]
		air_in_temp_1_value = raw_data_list[5]
		air_in_temp_1_changing_rate_value = raw_data_list[6]
		slurry_temp_value = raw_data_list[7]
		slurry_temp_changing_rate_value = raw_data_list[8]
		tower_top_negative_pressure_value = raw_data_list[9]
		tower_top_negative_pressure_changing_rate_value = raw_data_list[10]
		aging_tank_flow_value = raw_data_list[11]
		second_input_air_temp_value = raw_data_list[12]
		slurry_pipeline_lower_layer_pressure_value = raw_data_list[13]
		out_air_motor_freq_value = raw_data_list[14]
		second_air_motor_freq_value = raw_data_list[15]
		high_pressure_pump_freq_value = raw_data_list[16]
		gas_flow_value = raw_data_list[17]
		ambient_humidity_value = raw_data_list[18]
		json_body = [
			    {
			        "measurement": measurement,
			        "tags": {
			            "host": host_name,
			            "region": region_value
			        },
			        "fields": {
			            "air_out_temp": air_out_temp_value,
			            "air_out_temp_changing_rate": air_out_temp_changing_rate_value,
			            "base_powder_temp": base_powder_temp_value,
			            "base_powder_temp_changing_rate": base_powder_temp_changing_rate_value,
			            "air_in_temp_1": air_in_temp_1_value,
			            "air_in_temp_1_changing_rate": air_in_temp_1_changing_rate_value,
			            "slurry_temp": slurry_temp_value,
			            "slurry_temp_changing_rate": slurry_temp_changing_rate_value,
			            "tower_top_negative_pressure": tower_top_negative_pressure_value,
			            "tower_top_negative_pressure_changing_rate": tower_top_negative_pressure_changing_rate_value,
			            "aging_tank_flow": aging_tank_flow_value,
			            "second_input_air_temp": second_input_air_temp_value,
			            "slurry_pipeline_lower_layer_pressure":slurry_pipeline_lower_layer_pressure_value,
			            "out_air_motor_freq": out_air_motor_freq_value,
			            "second_air_motor_freq": second_air_motor_freq_value,
			            "high_pressure_pump_freq": high_pressure_pump_freq_value,
			            "gas_flow": gas_flow_value,
			            "ambient_humidity": ambient_humidity_value
			        }
			    }
			]
		self.client.write_points(json_body)
		# print("Write data: {0},{1}".format(air_out_temp_value, base_powder_temp_value))
		time.sleep(1)

	#query all data from raw_data measurement
    def query_raw_data(self):
    	query = 'select * from raw_data;'
    	print("Querying data: " + query)
    	result = self.client.query(query)
    	#print result
    	return result

    #insert the new data to picus_db
    def insert_energy_consumption_data(self, table_name, field_a_name, field_a_value, field_b_name, field_b_value):
		measurement = table_name
		print measurement
		host_name = "13.56.155.255"
		region_value = "us_west"
		json_body = [
			    {
			        "measurement": table_name,
			        "tags": {
			            "host": host_name,
			            "region": region_value
			        },
			        "fields": {
			            field_a_name: field_a_value,
			            field_b_name: field_b_value
			        }
			    }
			]
		self.client.write_points(json_body)
		print("Write data: {0},{1}".format(field_a_value, field_b_value))
		time.sleep(1)

	#query all data from raw_data measurement
    def query_energy_consumption_data(self):
    	query = 'select * from air_out_temp_m;'
    	print("Querying data: " + query)
    	result = self.client.query(query)
    	return result

#Test raw data insert and query
if __name__=='__main__':
	db_manage = PicusDBManage()
	for i in range(0,1):
		raw_data_list = []
		for j in range(0,20):
			number = random.random()
			raw_data_list.append(number)
		print raw_data_list
		db_manage.insert_raw_data(raw_data_list)
	query_result = db_manage.query_raw_data()
	print query_result

#Test energy consumption data insert and query
# if __name__=='__main__':
# 	db_manage = PicusDBManage()
# 	table_name = "air_out_temp_m"
# 	field_a_name = "air_out_temp"
# 	field_a_value = random.random()
# 	field_b_name = "energy_consumption"
# 	field_b_value = random.random()
# 	db_manage.insert_energy_consumption_data(table_name, field_a_name, field_a_value, field_b_name, field_b_value)
# 	query_result = db_manage.query_energy_consumption_data()
# 	print query_result