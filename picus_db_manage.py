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
		host_name = "127.0.0.1"
		region_value = "us_west"
		# brand_value = raw_data_list[0]
		m_value = raw_data_list[0]
		air_out_temp_value = raw_data_list[1]
		base_powder_temp_value = raw_data_list[2]
		air_in_temp_1_value = raw_data_list[3]
		slurry_temp_value = raw_data_list[4]
		tower_top_negative_pressure_value = raw_data_list[5]
		aging_tank_flow_value = raw_data_list[6]
		second_input_air_temp_value = raw_data_list[7]
		slurry_pipeline_lower_layer_pressure_value = raw_data_list[8]
		out_air_motor_freq_value = raw_data_list[9]
		second_air_motor_freq_value = raw_data_list[10]
		high_pressure_pump_freq_value = raw_data_list[11]
		gas_flow_value = raw_data_list[12]

		p_m_value = raw_data_list[13]
		p_air_out_temp_value = raw_data_list[14]
		p_base_powder_temp_value = raw_data_list[15]
		p_air_in_temp_1_value = raw_data_list[16]
		p_slurry_temp_value = raw_data_list[17]
		p_tower_top_negative_pressure_value = raw_data_list[18]
		p_aging_tank_flow_value = raw_data_list[19]
		p_second_input_air_temp_value = raw_data_list[20]
		p_slurry_pipeline_lower_layer_pressure_value = raw_data_list[21]
		p_out_air_motor_freq_value = raw_data_list[22]
		p_second_air_motor_freq_value = raw_data_list[23]
		p_high_pressure_pump_freq_value = raw_data_list[24]
		p_gas_flow_value = raw_data_list[25]

		json_body = [
			    {
			        "measurement": measurement,
			        "tags": {
			            "host": host_name,
			            "region": region_value
			        },
			        "fields": {
			        	"m" : m_value,
			            "air_out_temp": air_out_temp_value,
			            "base_powder_temp": base_powder_temp_value,
			            "air_in_temp_1": air_in_temp_1_value,
			            "slurry_temp": slurry_temp_value,
			            "tower_top_negative_pressure": tower_top_negative_pressure_value,
			            "aging_tank_flow": aging_tank_flow_value,
			            "second_input_air_temp": second_input_air_temp_value,
			            "slurry_pipeline_lower_layer_pressure":slurry_pipeline_lower_layer_pressure_value,
			            "out_air_motor_freq": out_air_motor_freq_value,
			            "second_air_motor_freq": second_air_motor_freq_value,
			            "high_pressure_pump_freq": high_pressure_pump_freq_value,
			            "gas_flow": gas_flow_value,
			            "p_m": p_m_value,
			            "p_air_out_temp": p_air_out_temp_value,
			            "p_base_powder_temp": p_base_powder_temp_value,
			            "p_air_in_temp_1": p_air_in_temp_1_value,
			            "p_slurry_temp": p_slurry_temp_value,
			            "p_tower_top_negative_pressure": p_tower_top_negative_pressure_value,
			            "p_aging_tank_flow": p_aging_tank_flow_value,
			            "p_second_input_air_temp": p_second_input_air_temp_value,
			            "p_slurry_pipeline_lower_layer_pressure":p_slurry_pipeline_lower_layer_pressure_value,
			            "p_out_air_motor_freq": p_out_air_motor_freq_value,
			            "p_second_air_motor_freq": p_second_air_motor_freq_value,
			            "p_high_pressure_pump_freq": p_high_pressure_pump_freq_value,
			            "p_gas_flow": p_gas_flow_value,
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
		host_name = "127.0.0.1"
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