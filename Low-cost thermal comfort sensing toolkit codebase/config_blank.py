#########################################################################################################################
#																														#
#	TCRS configuration file																								#
#	allows all the variables that need configuring for the TCSR script to be done										# 																						#
#																														#
#	Version history																										#
#	0.1 - 08 NOV 2018 	-	Initial named working version.																#
#	0.1.1 - 09 SEP 2018 -	added database size checking																#
#	0.1.2 - 12 SEP 2018 -	added TSL2561 Lux sensor and Gravity SEN02032 decibel meter. Thread ADS1115 name changed	#
#							to MDREVP																					#
#	0.1.3 - 19 NOV 18 	-	included i2c addresses, database names and tables for daily email, button pins, module id's	#
#	0.1.4 - 23 NOV 18 	-	added ADC (SEN0232 and MDREVP) channel selects												#
#	0.1.5 - 16 JAN 19 	-	added enable for emailing minor and major errors, plus minor and major error summarys for   #
#                           daily emails (used with TCSR 0.8.9)												            #
#	0.1.6 - 18 JUN 19	-	TCSR0.9 added in PMV calculation module and sensor offsets. Appropriate values added here	#
#	0.1.7 - 19 JUN 19	-	TCSR0.10 added in outdoor temperature get, mean and running mean outdoor temp calculation,	# 
# 							and Occupant temperature callculation. Appropriate values added here						#
#																														#
#	Known issues:																										#
#																														#
#	Work to do:																											#
#																														#
#########################################################################################################################

## Credentials
# 'email_usr' Username for email server
# 'email_psw' password for email server
# 'email_add' Address of email server
# 'email_prt' Port for email server
# 'email_frm' Address to send emails from
# 'email_to' Address list for emails to be sent too
# 'db_usr' Username for MySQL database
# 'db_psw' Password for MySQL database
# 'db_db' name of database
# 'db_tbl' table used in the database
# 'db_host' Address of database
# 'weatherAPIcurrent_api_key' api key for darksky.net

## Setup script variables
# 'sampleTime' How frequently each of the sensors is read. Note that if this is changed from 60s, the systemd config file should be changed to match
# 'mysqlTime' How frequently data is sent to the MySQL server, note sensor reads are queued until they are sent to MySQL server. Note that if this is changed from 120s, the systemd config file should be changed to match
# 'node_id' Identifying number of the node
# 'loc_lat' latitude of location for web api current weather call
# 'loc_long' longditude of location for web api current weather call
# 'weatherAPIcurrent_freq' frequency of web api current weather call


# 'module_MDREVP' Air speed sensor
# 'module_SHT75' Air temp and humidity sensor
# 'module_MAX31865' ADC for Radiant temp sensor
# 'module_AMG8833' Low resolution Panasonic thermal imaging camera
# 'module_Lepton' High resolution Flir thermal imaging camera
# 'module_SCD30' Requires module_BME280. CO2, air temp and humidity sensor
# 'module_SGP30' VOC sensor
# 'module_BME280' Air pressure, temp and humidity sensor
# 'module_buttons' Voting buttons
# 'module_PMS5003' Particulate sensor
# 'module_TSL2561' Lux sensor
# 'module_SEN0232' Decibel sensor
# 'module_PMV' PMV calculation module
# 'module_weatherAPIcurrent' Current weather get module, and calculate mean and running mean temperatures
# 'module_OccT' Occupant temperature calculation module

## PMV and OccT calculation variables
# 'Running_mean_full_id' full sensor id for the running mean outdoor temperature
# 'PMV_ta_module' Which temperature sensor to use for PMV calculation; SHT75, SCD30, BME280
# 'PMV_rh_module' Which humidity sensor to use for PMV calculation; SHT75, SCD30, BME280
# 'PMV_met' Metabolic rate value to use in PMV calculation; default is 1.1
# 'PMV_clo' Clothing value to use in PMV calculation; default is 0.5

# 'log_file' Location where the log file is saved to
# 'error_file' Location where the daily error and restart count is stored

## Debugging options
# 'Print_loop_count' Prints to terminal sensor read and MySQL counts
# 'email_major_loop_count' Emails out to email_to list each time the sensor has been read email_major_loop_value times 
# 'email_major_loop_value'
# 'email_daily' Emails the email_to list at midnight to confirm the node is alive 
# 'check_db_size' Requires 'email_daily' and 'run_mysql'. Re-analyses the database and adds it's size to the daily email
# 'run_mysql' Enables or disables sending of the data to the MySQL server
# 'print_positions' Prints to the terminal the position of each thread through it's loop
# 'print_queue_data' Prints to the terminal data being added to the queue
# 'print_errors' Prints to the terminal any errors that are caught
# 'email_errors' Emails the email_to list any minor errors that are caught
# 'email_errors_major' Emails the email_to list any minor errors that are caught
# 'email_summary_errors' Adds a summary of the previous days minor error count to the daily email if enabled
# 'email_summary_errors_major' Adds a summary of the previous days minor error count to the daily email if enabled
# 'fake_BME280' Enables a fake BME280 sensor with a constant pressure of 1000mbar, to allow testing of the SCD30 sensor. Data is not added to the queue

## Sensor offsets
# 'SHT75_T_offset' SHT75 temperature offset. default is 0.0
# 'SHT75_H_offset' SHT75 humidity offset. default is 0.0
# 'MAX31865_offset' MAX31865 offset. default is 0.0
# 'SCD30_C_offset' SCD30 CO2 offset. default is 0.0
# 'SCD30_T_offset' SCD30 temperature offset. default is 0.0
# 'SCD30_H_offset' SCD30 humidity offset. default is 0.0
# 'SGP30_offset' SGP30 offset. default is 0.0
# 'BME280_T_offset' BME280 temperature offset. default is 0.0
# 'BME280_H_offset' BME280 humidity offset. default is 0.0
# 'BME280_P_offset' BME280 pressure offset. default is 0.0

##Other sensor configurations
# 'SCD30_measurement_interval' Sets the interval that the SCD30 sensor is read
# 'SCD30_initial_pressure' Sets the initial pressure used for in the SCD30 sensor for compensation, before the BME280 can be read
# 'TSL2561_gain' Set the gain of the TSL2561 sensor. A value of 0 is low gain mode, and a value of 1 is high gain / 16x mode.
# 'TSL2561_integration_time' Set the TSL2561 integration time of the sensor.  A value 0 is 13.7ms, 1 is 101ms, 2 is 402ms, and 3 is manual mode.
# 'button_pin1_value' value logged for button 1
# 'button_pin2_value' value logged for button 2
# 'button_pin3_value' value logged for button 3
# 'button_pin4_value' value logged for button 4
# 'button_pin5_value' value logged for button 5

# 'MDREVP_ch_A' channel selects for MDREVP. for differential mode, the order used in code mean A is compared to B, and C is compared to D. in single mode, A-D are used in the order they come up in the code (i.e. the first channel used in the code will be A, the last will be D).
# 'MDREVP_ch_B'
# 'MDREVP_ch_C'
# 'MDREVP_ch_D'
# 'SEN0232_ch_A' channel selects for SEN0232. for differential mode, the order used in code mean A is compared to B, and C is compared to D. in single mode, A-D are used in the order they come up in the code (i.e. the first channel used in the code will be A, the last will be D).
# 'SEN0232_ch_B'
# 'SEN0232_ch_C'
# 'SEN0232_ch_D'

## Address configurations
# 'MDREVP_i2c_add' Address for MDREVP Air speed sensor 
# 'SHT75_i2c_add' # NOT IN USE # Address for SHT75 Air temp and humidity sensor
# 'MAX31865_i2c_add' # NOT IN USE # Address for MAX31865 ADC for Radiant temp sensor
# 'AMG8833_i2c_add' # NOT IN USE # Address for AMG8833 Low resolution Panasonic thermal imaging camera
# 'Lepton_i2c_add' # NOT IN USE # Address for Flir Lepton High resolution Flir thermal imaging camera
# 'SCD30_i2c_add' # NOT IN USE # Address for SCD30 CO", Temp and Humidity sensor
# 'SGP30_i2c_add' # NOT IN USE # Address for SGP30 VOC sensor
# 'BME280_i2c_add' # NOT IN USE # Address for BME280 Air pressure, temp and humidity sensor
# 'buttons_i2c_add' # NOT IN USE # Address for Voting buttons
# 'PMS5003_i2c_add' # NOT IN USE # Address for PMS5003 Particulate sensor
# 'TSL2561_i2c_add' # NOT IN USE # Address for TSL2561 Lux sensor
# 'SEN0232_i2c_add' Address for SEN0232 Decibel sensor

## Pin configurations
# 'MDREVP_pin_SCL' MDREVP SCL pin
# 'MDREVP_pin_SDA' MDREVP SDA pin
# 'SHT75_pin_data' SHT75 data pin
# 'SHT75_pin_SCK' SHT75 CLK pin
# 'MAX31865_pin_CS' MAX31865 CS pin
# 'MAX31865_pin_DI' MAX31865 DI pin
# 'MAX31865_pin_DO' MAX31865 DO pin
# 'MAX31865_pin_CLK' MAX31865 CLK pin
# 'AMG883_pin_SCL' AMG883 SCL pin
# 'AMG883_pin_SDA' AMG883 SDA pin
# 'SCD30_pin_SCL' SCD30 SCL pin
# 'SCD30_pin_SDA' SCD30 SDA pin
# 'SGP30_pin_SCL' SGP30 SCL pin
# 'SGP30_pin_SDA' SGP30 SDA pin
# 'BME280_pin_SCL' BME280 SCL pin
# 'BME280_pin_SDA' BME280 SDA pin
# 'button_pin1' button 1 pin
# 'button_pin2' button 2 pin
# 'button_pin3' button 3 pin
# 'button_pin4' button 4 pin
# 'button_pin5' button 5 pin
# 'TSL2561_pin_SCL' TSL2561 SCL pin
# 'TSL2561_pin_SDA' TSL2561 SDA pin
# 'SEN0232_pin_SCL' SEN0232 SCL pin
# 'SEN0232_pin_SDA' SEN0232 SDA pin

config = {
	'email_usr' : '',
	'email_psw' : '',
	'email_add' : '',
	'email_prt' : '',
	'email_frm' : '',
	'email_to' : '',
	'db_usr' : '',
	'db_psw' : '',
	'db_db' : '',
	'db_tbl' : '',	
	'db_host' : '',
	'weatherAPIcurrent_api_key' : '',

	'sampleTime' : '60',
	'mysqlTime' : '120',
	'node_id' : '',
	'loc_lat' : '',
	'loc_long' : '',
	'weatherAPIcurrent_freq' : '',

	'module_MDREVP' : 'False',
	'module_SHT75' : 'False',
	'module_MAX31865' : 'False',
	'module_AMG8833' : 'False',
	'module_Lepton' : 'False',
	'module_SCD30' : 'False',
	'module_SGP30' : 'False',
	'module_BME280' : 'False',
	'module_buttons' : 'False',
	'module_PMS5003' : 'False',
	'module_TSL2561' : 'False',
	'module_SEN0232' : 'False',
	'module_PMV' : 'False',
	'module_weatherAPIcurrent' : 'False',
	'module_OccT' : 'False',
	
	'Running_mean_full_id' : '',
	'PMV_ta_module' : '',
	'PMV_rh_module' : '',
	'PMV_met' :  '1.1',
	'PMV_clo' : '0.5',

	'log_file' : '/home/pi/Documents/TCSR/log.txt',
    'error_file' : '/home/pi/Documents/TCSR/error.tmp',

	'Print_loop_count' : 'False',
	'email_major_loop_count' : 'False',
	'email_major_loop_value' : '1000',
	'email_daily' : 'False',
	'check_db_size' : 'False',
	'run_mysql' : 'False',
	'print_positions' : 'False',
	'print_queue_data' : 'False',
	'print_errors' : 'False',
	'email_errors' : 'False',
	'email_errors_major' : 'False',
	'email_summary_errors' : 'False',
	'email_summary_errors_major' : 'False',
	'fake_BME280' : 'False',
	
	'SHT75_T_offset' : '0.0',
	'SHT75_H_offset' : '0.0',
	'MAX31865_offset' : '0.0',
	'SCD30_C_offset' : '0.0',
	'SCD30_T_offset' : '0.0',
	'SCD30_H_offset' : '0.0',
	'SGP30_offset' : '0.0',
	'BME280_T_offset' : '0.0',
	'BME280_H_offset' : '0.0',
	'BME280_P_offset' : '0.0',

	'SCD30_measurement_interval' : '60',
	'SCD30_initial_pressure' : '1000',
	'TSL2561_gain' : '0',
	'TSL2561_integration_time' : '0',
	'button_pin1_value' : '0',
	'button_pin2_value' : '1',
	'button_pin3_value' : '2',
	'button_pin4_value' : '3',
	'button_pin5_value' : '4',
	
	'MDREVP_ch_A' : '0',
	'MDREVP_ch_B' : '1',
	'MDREVP_ch_C' : '2',
	'MDREVP_ch_D' : '3',
	'SEN0232_ch_A' : '0',
	'SEN0232_ch_B' : '1',
	'SEN0232_ch_C' : '',
	'SEN0232_ch_D' : '',

	'MDREVP_i2c_add' : '0x49',
	'SHT75_i2c_add' : '',
	'MAX31865_i2c_add' : '',
	'AMG8833_i2c_add' : '',
	'Lepton_i2c_add' : '',
	'SCD30_i2c_add' : '',
	'SGP30_i2c_add' : '',
	'BME280_i2c_add' : '',
	'buttons_i2c_add' : '',
	'PMS5003_i2c_add' : '',
	'TSL2561_i2c_add' : '',
	'SEN0232_i2c_add' : '0x48',

	'MDREVP_pin_SCL' : '3',
	'MDREVP_pin_SDA' : '2',
	'SHT75_pin_data' : '18',
	'SHT75_pin_SCK' : '23',
	'MAX31865_pin_CS' : '8',
	'MAX31865_pin_DI' : '9',
	'MAX31865_pin_DO' : '10',
	'MAX31865_pin_CLK' : '11',
	'AMG883_pin_SCL' : '3',
	'AMG883_pin_SDA' : '2',
	'SCD30_pin_SCL' : '3',
	'SCD30_pin_SDA' : '2',
	'SGP30_pin_SCL' : '3',
	'SGP30_pin_SDA' : '2',
	'BME280_pin_SCL' : '3',
	'BME280_pin_SDA' : '2',
	'button_pin1' : '26',
	'button_pin2' : '19',
	'button_pin3' : '13',
	'button_pin4' : '6',
	'button_pin5' : '5',
	'TSL2561_pin_SCL' : '3',
	'TSL2561_pin_SDA' : '2',
	'SEN0232_pin_SCL' : '3',
	'SEN0232_pin_SDA' : '2'
}
