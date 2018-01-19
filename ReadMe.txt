1. Please install xlrd and xlwt
sudo pip install xlrd
sudo pip install xlwt

2. Setup the database

> create database "picus_db"
> show databases
name: databases
---------------
name
_internal
picus_db

> use picus_db

> show measurements


> INSERT raw_data,host=13.56.155.255,region=us_west air_out_temp=61.8,air_out_temp_changing_rate=89.4,base_powder_temp=81.2,base_powder_temp_changing_rate=0.65,air_in_temp_1=89.54,air_in_temp_1_changing_rate=65.2,slurry_temp=56.2,slurry_temp_changing_rate=0.45,tower_top_negative_pressure=91.3,tower_top_negative_pressure_changing_rate=0.85,aging_tank_flow=0.34,second_input_air_temp=0.98,slurry_pipeline_lower_layer_pressure=45.6,out_air_motor_freq=0.98,second_air_motor_freq=89.5,high_pressure_pump_freq=23.6,gas_flow=23.8,ambient_humidity=89.3,jingbai_recommend_gas_1=34.1,jingbai_recommend_gas_2=23.2,TBO_recommend_gas=34.8

3. Run the code

> python main.py








