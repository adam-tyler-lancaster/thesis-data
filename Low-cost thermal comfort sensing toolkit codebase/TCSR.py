#!/usr/bin/python3
#########################################################################################################################
#																														#
#	Thermal Comfort Sensor Rig (TCSR)																					#
#																														#
#	Version history																										#
#	0.1 - 05 SEP 2018 	-	Initial named working version.																#
#	0.2	-18 SEP 2018 	-	Merged P0 and P1 scripts into a single script with switch-able modules (switch-able mysql	#
#							for debugging). Merged in P0 feature of MySQL thread and the use of queues. Added 			#
#							calculation of standard deviation to the air speed sensor. Added SCD30, SGP30 and BME280 	#
#							sensors. 																					#
#	0.2.1 - 25 SEP 2018	- 	Fixed calling of the late time data was saved in the SGP30 module, to check if data is 		#
#							older than seven days																		#
#	0.3	- 25 SEP 2018	-	Added button voting functionality															#
#	0.4 - 26 SEP 2018	-	Added switch-able print statements for program position, errors and queue data, for 		#
#							debugging. Also added try/except blocks to all in loop I/O calls. Also added logging of 	#
#							errors to a log file																		#
#	0.5 - 09 OCT 2018	-	Switched SCD30 to using AT_SCD30 library													#
#	0.5.1 - 10 OCT 2018 -	added better error catching and reporting for the SDC30. Also added try/catch to SDC30		#
#							pressure offset set command																	#
#	0.5.2 - 18 OCT 2018 -	moved credentials to a external file to obfuscate. Added switch for loop printing. Added 	#
#							functionality to email errors, and email out every major loop (1000), and a switch to 		#
#							enable or disable it. Added a check to see if the timer or mysql threads have got stuck 	#
#							and prints/emails out if it detects it has													#
#	0.5.2.1 - 18 OCT 2018 -	corrected a bug that heartbeart thread didn't sleep											#
#	0.5.2.2 - 18 OCT 2018 -	corrected a bug that the error emails were compiled but not actually sent					#
#	0.5.2.3 - 20 OCT 2018 - added in a function for email as server was dropping connection if just connecting in setup	#
#	0.5.2.4 - 22 OCT 2018 -	Added option for a daily email instead of a nth loop one									#
#	0.5.2.5 - 22 OCT 2018 -	Fixed issue with the emailing errors from MySQl thread that was crashing it					#
#	0.5.2.6 - 23 OCT 2018 -	Added reporting of DB size to daily email													#
#	0.6	-	07 NOV 2018 -	Added PMS5003 sensor, and fake BME280 sensor for testing SCD30								#
#	0.6.1 - 07 NOV 2018 -	Added check to SCD30 sensor for NaN data													#
#	0.7 -	08 NOV 2018 -	Changed credential file to a general configuration file and moved all configurable settings	#
#							to it for easier GIT synchronisation														#
#	0.7.1 -	08 NOV 2018 -	minor corrections to position print in Lepton thread										#
#	0.7.2 -	09 NOV 2018 -	made getting database size optional separate from the daily email. Before calling for the	#
#							size of the database, the script now requests the database be analysed to get the indexes 	#
#							updated																						#
#	0.8 -	12 NOV 2018 -	Added TSL2561 Lux sensor and Gravity SEN02032 decibel meter. Thread ADS1115 name changed to	#
#							MDREVP, as there is now a second ADS1115 device connected for the SEN0232. Corrected a bug	#
#							that if the script had never run before, and as such there was nothing stored for the SGP30	#
#							base values in the database, it crashed														#
#	0.8.1 -	12 NOV 2018 -	Corrected ADC configuration error due to difference in prototype board wiring				#
#	0.8.2 - 14 NOV 2018 -	added i2c addresses, database names and tables for daily email, all pins to config. Removed #
#							adding error data such as '9999'. Fixed an error in heartbeat where the analyse error email #
#							and print functions called a none existent variable. Added try except to email sending.		#
#							Corrected bad mysql call in SGP30.															#
#	0.8.3 - 21 NOV 2018 -	Error pulling pins in from config for MAX31865. Added address select to BME280.	Error 		#
#							adding time to SHT75 data. Started breaking out functions; first one is a check it it's a 	#
#							number function that is used to check for bad data from the sensors. Fixed SGP30 checking 	#
#							db for base values at start.																#
#	0.8.4 - 22 NOV 2018 -	Mysql commits were clashing, so made thread-safe by establishing separate connections for 	#
#							each thread.; new logic to test for nan wasn't quite right.									#
#	0.8.5 - 23 NOV 2018 -	SEN0232 was using MDREVP I2C address. Also added MDREVP and SEN0232 ADC channel selects to 	#
#							config 																						#
#	0.8.6 - 26 NOV 2018 -	Previous changes had broken the daily email and the heartbeat. Modified to kill the script 	#
#							if there is a problem allowing systemd to restart. Opportunity has been taken to start 		#
#							simplifying code, so a general error function has been created, which heartbeat uses to 	#
#							report errors. Added a brutal and ugly kill switch to heartbeat								#
#	0.8.7 - 10 DEC 2018 -	Script still randomly stopping, so added in utilisation of systemd watchdog.			 	#
#	0.8.8 - 10 DEC 2018 -	Wasn't submitting correct signal to systemd watchdog									 	#
#	0.8.9 - 16 JAN 2019 -	Split errors into minor and major, allowed emailing of both to be enabled or disabled       #
#                           separately. Also added ability to enable adding major and minor error daily summaries to    #
#                           daily emails. error_report function now used throughout		   							 	#
#	0.8.10- 17 JAN 2019 -	Fixed an error that error counts were not global in helper function. Made logging of errors #
# 							thread safe with a new thread.       														#
#	0.8.11- 23 JAN 2019 -	Fixed an error with restarts not counting													#
#	0.8.12- 31 JAN 2019 -	When running as root, modules were not importing. added sys path. Added LED output to 		#
#							button press. Now indicates device is booting up using the status LED (flashing), which 	#
#							goes off once boot is complete and the loops have begun 									#
#	0.8.13- 28 FEB 2019 -	added a default value to variables in SGP30 as occasionally they were referenced before 	#
# 							assignment 																					#
#	0.8.14- 04 JUN 2019 -	ADS1115 library and how it works has been updated by Adafruit. Similarly SGP30 library and 	#
# 							how it works has been updated by AdafruitCode altered to use the new libraries				#
#	0.8.14-	13 JUN 2019	-	SEN0232 seems to be having semi regular I/O issues, which are causing script to restart. 	#
#							Try block simplified, and extra pass statements removed.									#
#	0.9 -	18 JUN 2019 - 	Added PMV calculation module. Added check for BME280 or fake BME 280 for SCD30. Added 		#
#							offsets to all appropriate values (set in config file). Added a few more comments			#
#	0.10 -	19 JUN 2019 -	Added module to pull current outdoor temperature from darksky.net and log it to database. 	#
# 							Added module to calculate Adaptive max and min operative temp, as well as current actual 	#
# 							operative temp. Also parameters used in PMV calculation are loggedto database. PMV module 	#
# 							also logs PPD value to database.															#
#																														#
#	Known issues:																										#
#	-	Systemd requires watchdog time-out to be set in config file, so currently if the sample time is changed, the 	#
#		watchdog timer must be changed in the config file to match														#
#																														#
#	Work to do:																											#
#	-	More Comments required																							#
#	-	Look into thread priority's. mysql thread doesn't need to be as high											#
#	-	encrypt mysql and connections to it																				#
#		https://www.symantec.com/connect/articles/secure-mysql-database-design											#
#		http://thinkdiff.net/mysql/encrypt-mysql-data-using-aes-techniques/												#
#	-	Add try to sensor initialisations																				#
#	-	Add else to try blocks to avoid catching extra errors															#
#	-	Break a lot of the repeated code into functions																	#
#	-	split errors into major and minor errors, and include a summary of minor errors in daily email					#
#	-	Add LED pin to config																							#
#																														#
#########################################################################################################################

#########################################################################################################################
#########################################################################################################################
##																													   ##
##	Start of Set-up																									   ##
##																													   ##
#########################################################################################################################
#########################################################################################################################

import ast
import csv
import datetime
import json
import math
import os
import queue
import smtplib
## Import General modules
import sys
import time
from collections import deque
from email.mime.text import MIMEText
from pathlib import Path
from threading import Barrier, Event, Thread

## Switch on the Button Status LED (flash) to indicate the start of a boot
import RPi.GPIO as GPIO
import sdnotify
from config import config

sys.path.insert(0,'/home/pi/.local/lib/python3.5/site-packages')

GPIO.setmode(GPIO.BCM)
GPIO.setup(16,GPIO.OUT, initial=GPIO.HIGH)
p = GPIO.PWM(16, 1)
p.start(50)



# Setup script variables
sampleTime = int(config['sampleTime']) 				# How frequently each of the sensors is read
mysqlTime = int(config['mysqlTime']) 				# How frequently data is sent to the MySQL server, note sensor reads are queued until they are sent to MySQL server
node_id = config['node_id'] 						# Identifying number of the node
print("node id " + node_id)							# print the node id to the terminal
kill_switch = False

Heartbeat_name = "Heartbeat"						# Human readable name
Timer_name = "Timer"								# Human readable name
MySQL_name = "MySQL"								# Human readable name

module_MDREVP = ast.literal_eval(config['module_MDREVP']) 	# Enabled externally by config file. Air speed sensor
MDREVP_name = "MDREVP"								# Human readable name
MDREVP_id = "000"									# MDREVP sensor id. default is 000
module_SHT75 = ast.literal_eval(config['module_SHT75']) 		# Enabled externally by config file. Air temp and humidity sensor
SHT75_name = "SHT75"								# Human readable name
SHT75_T_id = "001"									# SHT75 temperature sensor id. default is 001
SHT75_T_offset = float(config['SHT75_T_offset'])	# SHT75 temperature offset. default is 0.0
SHT75_H_id = "002"									# SHT75 humidity sensor id. default is 002
SHT75_H_offset = float(config['SHT75_H_offset'])	# SHT75 humidity offset. default is 0.0
module_MAX31865 = ast.literal_eval(config['module_MAX31865']) 	# Enabled externally by config file. ADC for Radiant temp sensor
MAX31865_name = "MAX31865"							# Human readable name
MAX31865_id = "003"									# MAX31865 sensor id. default is 003
MAX31865_offset = float(config['MAX31865_offset'])	# MAX31865 offset. default is 0.0
module_AMG8833 = ast.literal_eval(config['module_AMG8833']) 	# Enabled externally by config file. Low resolution Panasonic thermal imaging camera
AMG8833_name ="AMG8833"								# Human readable name
AMG8833_id = "004"									# AMG8833 sensor id. default is 004
module_Lepton = ast.literal_eval(config['module_Lepton']) 		# Enabled externally by config file. High resolution Flir thermal imaging camera
Lepton_name = "Lepton"								# Human readable name
Lepton_id = "005"									# Lepton sensor id. default is 005
module_SCD30 = ast.literal_eval(config['module_SCD30']) 		# Enabled externally by config file. Requires module_BME280. CO2, air temp and humidity sensor
SCD30_name = "SCD30"								# Human readable name
SCD30_C_id = "006"									# SCD30 CO2 sensor id. default is 006
SCD30_C_offset = float(config['SCD30_C_offset'])	# SCD30 CO2 offset. default is 0.0
SCD30_T_id = "007"									# SCD30 temperature sensor id. default is 007
SCD30_T_offset = float(config['SCD30_T_offset'])	# SCD30 temperature offset. default is 0.0
SCD30_H_id = "008"									# SCD30 humidity sensor id. default is 008
SCD30_H_offset = float(config['SCD30_H_offset'])	# SCD30 humidity offset. default is 0.0
module_SGP30 = ast.literal_eval(config['module_SGP30']) 		# Enabled externally by config file. VOC sensor
SGP30_name = "SGP30"								# Human readable name
SGP30_id = "009"									# SGP30 sensor id. default is 009
SGP30_offset = float(config['SGP30_offset'])	# SGP30 offset. default is 0.0
module_BME280 = ast.literal_eval(config['module_BME280']) 		# Enabled externally by config file. Air pressure, temp and humidity sensor
BME280_name = "BME280"								# Human readable name
BME280_T_id = "010"									# BME280 temperature sensor id. default is 010
BME280_T_offset = float(config['BME280_T_offset'])	# BME280 temperature offset. default is 0.0
BME280_H_id = "011"									# BME280 humidity sensor id. default is 011
BME280_H_offset = float(config['BME280_H_offset'])	# BME280 humidity offset. default is 0.0
BME280_P_id = "012"									# BME280 pressure sensor id. default is 012
BME280_P_offset = float(config['BME280_P_offset'])	# BME280 pressure offset. default is 0.0
module_buttons = ast.literal_eval(config['module_buttons']) 	# Enabled externally by config file. Voting buttons
button_name = "button"								# Human readable name
button_id = "013"									# buttons sensor id. default is 013
module_PMS5003 = ast.literal_eval(config['module_PMS5003'])		# Enabled externally by config file. Particulate sensor
PMS5003_name = "PMS5003"							# Human readable name
PMS5003_id = "014"									# PMS5003 sensor id. default is 014
module_TSL2561 = ast.literal_eval(config['module_TSL2561'])		# Enabled externally by config file. Lux sensor
TSL2561_name = "TSL2561"							# Human readable name
TSL2561_B_id = "015"								# TSL2561 broadband sensor id. default is 015
TSL2561_I_id = "016"								# TSL2561 infrared sensor id. default is 016
TSL2561_L_id = "017"								# TSL2561 Lux sensor id. default is 017
module_SEN0232 = ast.literal_eval(config['module_SEN0232'])		# Enabled externally by config file. Decibel sensor
SEN0232_name = "SEN0232"							# Human readable name
SEN0232_id = "018"									# SEN0232 sensor id. default is 018
module_PMV = ast.literal_eval(config['module_PMV'])		# Enabled externally by config file. PMV Calculation
module_PMV = ast.literal_eval(config['module_PMV'])		# Enabled externally by config file. PMV Calculation
PMV_name = "PMV"									# Human readable name
PMV_id = "019"										# PMV calculation id. default is 019
PMV_ta_module = config['PMV_ta_module']				# PMV air temperature module
PMV_rh_module = config['PMV_rh_module']				# PMV humidity module
module_weatherAPIcurrent = ast.literal_eval(config['module_weatherAPIcurrent'])		# Enabled externally by config file. web scrape outdoor temp and calculate mean and running mean outdoor temperatures
loc_lat = config['loc_lat']							# latitude of location for web api outdoor temperature call
loc_long = config['loc_long']						# longditude of location for web api outdoor temperature call
weatherAPIcurrent_freq = config['weatherAPIcurrent_freq']							# frequency of web api outdoor temperature call
weatherAPIcurrent_api_key = config['weatherAPIcurrent_api_key']					# api key for web api outdoor temperature call
weatherAPIcurrent_name = "Outdoor Temp"					# Human readable name
weatherAPIcurrent_id = "020"							# Current Outdoor Temp id. default is 020
weatherAPI_mean_id = "021"								# Mean Outdoor Temp id. default is 021
weatherAPI_running_mean_id = "022"						# Exponentially weighted running Mean Outdoor Temp id. default is 022
module_OccT = ast.literal_eval(config['module_OccT'])	# Enabled externally by config file. Calculates occupant temperature parameters
OccT_name = "Occupant Temperature"						# Human readable name
OccT_id = "023"

#Debugging options
Print_loop_count = ast.literal_eval(config['Print_loop_count'])					    # Enabled externally by config file. Prints to terminal sensor read and MySQL counts		
email_major_loop_count = ast.literal_eval(config['email_major_loop_count'])	    	# Enabled externally by config file. Emails out to email_to list each time the sensor has been read email_major_loop_value times
email_major_loop_value = int(config['email_major_loop_value'])
email_daily = ast.literal_eval(config['email_daily'])							    # Enabled externally by config file. Emails the email_to list at midnight to confirm the node is alive, and reports (if MySQL enabled) the database size 
check_db_size = ast.literal_eval(config['check_db_size'])
run_mysql = ast.literal_eval(config['run_mysql'])								    # Enabled externally by config file. Enables or disables sending of the data to the MySQL server
print_positions = ast.literal_eval(config['print_positions'])				    	# Enabled externally by config file. Prints to the terminal the position of each thread through it's loop
print_queue_data = ast.literal_eval(config['print_queue_data'])				    	# Enabled externally by config file. Prints to the terminal data being added to the queue
print_errors = ast.literal_eval(config['print_errors'])						    	# Enabled externally by config file. Prints to the terminal any errors that are caught		
email_errors_major = ast.literal_eval(config['email_errors_major'])					# Enabled externally by config file. Emails the email_to list any minor errors that are caught
email_summary_errors = ast.literal_eval(config['email_summary_errors'])				# Enabled externally by config file. Adds a summary of the previous days minor error count to the daily email if enabled
email_summary_errors_major = ast.literal_eval(config['email_summary_errors_major']) # Enabled externally by config file. Adds a summary of the previous days minor error count to the daily email if enabled
email_errors = ast.literal_eval(config['email_errors'])							    # Enabled externally by config file. Emails the email_to list any errors that are caught
fake_BME280 = ast.literal_eval(config['fake_BME280'])							    # Enabled externally by config file. Enables a fake BME280 sensor with a constant pressure of 1000mbar, to allow testing of the SCD30 sensor. Data is not added to the queue

# setup the button pins
button_pin1 = int(config['button_pin1'])
button_pin1_value = int(config['button_pin1_value']) 
button_pin2 = int(config['button_pin2'])
button_pin2_value = int(config['button_pin2_value']) 
button_pin3 = int(config['button_pin3'])
button_pin3_value = int(config['button_pin3_value']) 
button_pin4 = int(config['button_pin4'])
button_pin4_value = int(config['button_pin4_value']) 
button_pin5 = int(config['button_pin5'])
button_pin5_value = int(config['button_pin5_value']) 

## Import optional modules
barrierCount = 0
# MDREVP (airspeed) setup
if module_MDREVP == True:
	barrierCount += 1
	import board
	import busio
	#from adafruit_ads1x15.differential import ADS1115
	import adafruit_ads1x15.ads1115 as ADS
	from adafruit_ads1x15.analog_in import AnalogIn
	import math
	import numpy as np
	print("module_MDREVP active as sensor id " + MDREVP_id)

# SHT75 (air temperature and humidity) setup
if module_SHT75 == True:
	barrierCount += 1
	from pi_sht1x import SHT1x
	print("module_SHT75 active with T as sensor id " + SHT75_T_id + ", H as sensor id " + SHT75_H_id)

# MAX31865 (radiant temperature) setup
if module_MAX31865 == True:
	barrierCount += 1
	import max31865 as max31865
	print("module_MAX31865 active as sensor id " + MAX31865_id)

# AMG8833 (thermal images) setup
if module_AMG8833 == True:
	barrierCount += 1
	import board
	import busio
	import adafruit_amg88xx
	print("module_AMG8833 active as sensor id " + AMG8833_id)

# Lepton (thermal images) setup
if module_Lepton == True:
	barrierCount += 1
	from pylepton.pylepton import Lepton
	import numpy as np
	from itertools import chain
	print("module_Lepton active as sensor id " + Lepton_id)

# SCD30 (CO2, air temperature and humidity) setup
if module_SCD30 == True: #requires module_BME280
	barrierCount += 1
	import board
	import busio
	import AT_SCD30
	b2 = Barrier(2)	# used to hold off the CO2 measurement untill the air pressure measurement has been taken
	import numpy as np
	print("module_SCD30 active with C as sensor id " + SCD30_C_id + ", T as sensor id " + SCD30_T_id + ", H as sensor id " + SCD30_H_id)

# SGP30 (VOC) setup
if module_SGP30 == True:
	barrierCount += 1
	import board
	import busio
	import adafruit_sgp30
	print("module_SGP30 active as sensor id " + SGP30_id)

# BME280 (air pressure, temperature and humidity) setup
if module_BME280 == True:
	barrierCount += 1
	import board
	import busio
	import adafruit_bme280
	print("module_BME280 active with T as sensor id " + BME280_T_id + ", H as sensor id " + BME280_H_id + ", P as sensor id " + BME280_P_id)

# Buttons setup
if module_buttons == True:
	import time
	print("module_buttons active as sensor id " + button_id)

# PMS5003 (particulates) setup
if module_PMS5003 == True:
	barrierCount += 1
	from PMS5003 import read_data
	print("module_PMS5003 active as sensor id " + PMS5003_id)

# TSL2561 (light level) setup
if module_TSL2561 == True:
	barrierCount += 1
	import board
	import busio
	import adafruit_tsl2561
	print("module_TSL2561 active with B as sensor id " + TSL2561_B_id + ", I as sensor id " + TSL2561_I_id + ", L as sensor id " + TSL2561_L_id)

# SEN02032 (noise level) setup
if module_SEN0232 == True:
	barrierCount += 1
	import board
	import busio
	#from adafruit_ads1x15.differential import ADS1115
	import adafruit_ads1x15.ads1115 as ADS
	from adafruit_ads1x15.analog_in import AnalogIn
	import math
	import numpy as np
	print("module_SEN0232 active as sensor id " + SEN0232_id)

# PMV calculation setup
if module_PMV == True:
	barrierCount += 1
	import comfort_models as PMV_OCCT
	PMV = PMV_OCCT.comfortModels()
	print("PMV active as sensor id " + PMV_id)

# Outdoor temperature web api fetch
if module_weatherAPIcurrent == True:
	from urllib.request import urlopen
	import json
	print("Current weather API active as sensor id " + weatherAPIcurrent_id)

# Occupant Temperature calculation setup
if module_OccT == True:
	barrierCount += 1
	import comfort_models as PMV_OCCT
	OCCT = PMV_OCCT.comfortModels()
	print("Occupant Temperature active as sensor id " + OccT_id)

# Fake BME280 setup
if fake_BME280 == True:
	barrierCount += 1
	print("fake_BME280 active")

## Setup the event controls
event = Event()								# event is one full sample period
halfTime = Event()							# halfTime is used to mark halfway through a sample period
event.set()
halfTime.set()
b1 = Barrier(barrierCount + 1)				# b1 Barrier is used to hold all sampling threads to synchronise the start of a sampling period
b3 = Barrier(barrierCount + 1)				# b3 Barrier is a second check to make sure everything is in sync after starting event
b4t_count = 1
b4r_count = 3
if module_PMV == True:						# if PMV calculation is done, setup barriers to wait for all measurements to be taken
	b4t_count += 1 
	b4r_count += 1
if module_OccT == True:						# if OccT calculation is done, setup barriers to wait for all measurements to be taken
	b4t_count += 1 
	b4r_count += 1
b4t = Barrier(b4t_count)						# barrier for temp measurement
b4r = Barrier(b4r_count)						# barrier for other measurements
q = queue.Queue()						# sensor data queue
ql = queue.Queue()						# error log count queue - used to update the file that logs the number of errors that day 
qe = queue.Queue()						# error error count queue - used to hold the actual error information
q_mysql = deque()						# backup sensor data queue

# Start the system d watchdog
n = sdnotify.SystemdNotifier()
n.notify("READY=1")

# Setup the database
if run_mysql == True:
	import MySQLdb

#########################################################################################################################
#########################################################################################################################
##																													   ##
##	Start of helper function definitions 																			   ##
##																													   ##
#########################################################################################################################
#########################################################################################################################

# Setup the email server if email is required
if (email_major_loop_count == True) or (email_errors == True) or (email_errors_major == True) or (email_daily == True):

	def sendEmail(emailMsg,emailSbj):
		server = smtplib.SMTP(config['email_add'],config['email_prt'])
		server.starttls()
		server.login(config['email_usr'],config['email_psw'])
		msg = MIMEText(str(emailMsg))
		msg['Subject'] = str(emailSbj)
		msg['From'] = config['email_frm']
		msg['To'] = config['email_to']
		server.sendmail(msg['To'],msg['From'],msg.as_string())
		server.quit()

# is_nan function is used to check if the value returned from a sensor is a value - used in SCD30
def is_nan(s):
    try:
        s = float(s)
        return not math.isnan(s)
    except ValueError:
        return True

# error_report function handles the reporting of errors.
def error_report(calling_thread,error,description,level):

	# email errors
	if ((email_errors == True) and (level == 0)) or ((email_errors_major == True) and (level == 1)): # if emailing out errors is enabled, then do it	
		try:
			sendEmail("{1} {3} node {4} @ {0}\n{2}\n".format(str(datetime.datetime.now().isoformat()),calling_thread,str(error),description,node_id),"{1} {2} node {3} @ {0}".format(str(datetime.datetime.now().isoformat()),calling_thread,description,node_id))
		except Exception as e:
			if print_errors == True:
				print(calling_thread + description + "email: "+ str(e)) # print the error
			qe.put("{1} {3} email node {4} @ {0}: {2}\n".format(str(datetime.datetime.now().isoformat()),calling_thread,str(error),description,node_id)) # write the error to the log file queue 
    #log minor errors to error count log file
	if(level == 0):
		ql.put("i")

    #log major errors to error count log file
	if(level == 1):
		ql.put("a")  
	
	# print errors
	if print_errors == True:
		print(calling_thread + description + ": " + str(error)) # print the error
	
	# log errors to file
	qe.put("{1} {3} node {4} @ {0}: {2}\n".format(str(datetime.datetime.now().isoformat()),calling_thread,str(error),description,node_id)) # write the error to the log file queue

#########################################################################################################################
#########################################################################################################################
##																													   ##
##	End of helper function definitions 																				   ##
##																													   ##
#########################################################################################################################
#########################################################################################################################

# make sure the correct combination of modules are enabled
if (module_SCD30 and (ast.literal_eval(config['module_BME280']) != True and ast.literal_eval(config['fake_BME280']) != True)):	# if SCD30 enabled and either BME280 or fake BME280 not enabled, kill the program as one is required
	kill_switch = True
	error_report("Startup","SCD30 requirements not met"," BME280 or fake BME280 not enabled, but SCD30 is. Killing script",0)
if ((PMV_ta_module == "SHT75" and module_SHT75 != True) or (PMV_ta_module == "SCD30" and module_SCD30 != True) or (PMV_ta_module == "BME280" and module_BME280 != True) or ((PMV_ta_module != "BME280") and (PMV_ta_module != "SCD30") and (PMV_ta_module != "SHT75"))) and (module_PMV or module_OccT):	# if required modules for PMV calculation not selected or enabled, kill the program
	kill_switch = True
	error_report("Startup","PMV/OccT requirements not met"," PMV or Occupant temperature module enabled but ta required modules either not enabled or not selected. Killing script",0)
if ((PMV_rh_module == "SHT75" and module_SHT75 != True) or (PMV_rh_module == "SCD30" and module_SCD30 != True) or (PMV_rh_module == "BME280" and module_BME280 !=  True) or ((PMV_rh_module != "BME280") and (PMV_rh_module != "SCD30") and (PMV_rh_module != "SHT75"))) and (module_PMV or module_OccT):	# if required modules not selected or enabled, kill the program
	kill_switch = True
	error_report("Startup","PMV/OccT requirements not met"," PMV or Occupant temperature module enabled but rh required modules either not enabled or not selected. Killing script",0)
if ((config['Running_mean_full_id'] == "") and module_OccT):	# if full sensor id for outdoor temperature not defined, kill the script
	kill_switch = True
	error_report("Startup"," PMV requirements not met","Occupant temperature module enabled but outdoor temperature required data not selected. Killing script",0)

# log the restart
error_report("main ","","restart",1)

#setup the error and restart counters and file
minor_error_count = 0
major_error_count = 0
restart_count = 0

error_file_exists = Path(config['error_file']) # if error counter file exists, read in the data from it
if error_file_exists.is_file():
	e = open(config['error_file'],"r")
	reader = csv.reader(e)
	
	for row in reader:
		minor_error_count = int(row[0])
		major_error_count = int(row[1])
		restart_count = int(row[2])
	e.close()
	print("error count log file found. Importing previous values (mi {0} ma {1} r {2})\n".format(minor_error_count,major_error_count,restart_count))
else:											# if it doesn't exist, write it out with 0's
	e = open(config['error_file'],"w")
	e.write('0,0,0')
	e.flush()
	e.close()
	print("error count log file not found. Creating a new one\n")


#########################################################################################################################
#########################################################################################################################
##																													   ##
##	End of Set-up																									   ##
##																													   ##
#########################################################################################################################
#########################################################################################################################

#########################################################################################################################
#########################################################################################################################
##																													   ##
##	Start of thread definitions																						   ##
##																													   ##
#########################################################################################################################
#########################################################################################################################

#########################################################################################################################
#																														#
#	Heartbeat thread - Calls out for help if the timer thread hasn't looped in a while 									#
#																														#
#########################################################################################################################
def heartBeat():
	db_heartbeat=MySQLdb.connect(host=config['db_host'],user=config['db_usr'],passwd=config['db_psw'],db=config['db_db'])
	global mysql_heartBeat_flag
	global samp_heartBeat_flag
	global kill_switch
	global minor_error_count
	global major_error_count
	global restart_count
	mysql_heartBeat_flag = True
	mysql_heartBeat_time = time.time()
	samp_heartBeat_flag = True
	samp_heartBeat_time = time.time()
	current_day = datetime.datetime.today().weekday()
	prev_day = current_day
	heartbeatLoop = 0
	while True:
		# send the daily email if required (also checks and reports the database size if that is required)
		x = 0
		if email_daily == True:
			if current_day != prev_day:
				x = 1
				if run_mysql == True:
					if check_db_size == True:
						x = 2
						c=db_heartbeat.cursor()
						select = """ANALYZE TABLE %s"""
						select = select % (config['db_tbl'])
						try:
							c.execute(select)
						except Exception as e:
							error_report(Heartbeat_name,e,"execute ANALYSE",0)
							x = 1 
						else:
							select = """SELECT ROUND(((data_length + index_length) / 1024 / 1024), 4) AS 'Size (MB)' FROM information_schema.TABLES WHERE table_schema = '%s' ORDER BY (data_length + index_length) DESC"""
							select = select % (config['db_db'])
							try:
								c.execute(select)
							except Exception as e:
								error_report(Heartbeat_name,e,"execute size",0)
								x = 1 
							else:
								heartbeat_mysql_result, = c.fetchall()
						db_heartbeat.close()

            # prepare the error summary details incase needed 
			email_summary_errors_msg = ""
			if (email_summary_errors_major == True) or (email_summary_errors == True):
				email_summary_errors_msg = email_summary_errors_msg + '\n\nError Summary for yesterday\n' + str(restart_count) + ' restarts\n'
			if email_summary_errors_major == True:
				email_summary_errors_msg = email_summary_errors_msg + str(major_error_count) + ' major errors\n'
			if email_summary_errors == True:
				email_summary_errors_msg = email_summary_errors_msg + str(minor_error_count) + ' minor errors\n'            
                
			# send the appropriate email
			if x == 1:
					try:
						sendEmail("Good Morning from node {0}, just letting you know I am still alive.{1}".format(node_id,email_summary_errors_msg),"node {1} @ {0} Good Morning".format(str(datetime.datetime.now().isoformat()),node_id))

					except Exception as e:
						error_report(Heartbeat_name,e,"email morning no size",0) 
					finally:
						minor_error_count = 0
						major_error_count = 0
						restart_count = 0
			elif x == 2:
					try:
						sendEmail("Good Morning from node {0}, just letting you know I am still alive.\n DB size in MB is {1}{2}".format(node_id,heartbeat_mysql_result[0],email_summary_errors_msg),"node {1} @ {0} Good Morning".format(str(datetime.datetime.now().isoformat()),node_id))
					except Exception as e:
						error_report(Heartbeat_name,e,"heartbeat email morning inc size",0) 
					finally:
						minor_error_count = 0
						major_error_count = 0
						restart_count = 0
			prev_day = current_day
			current_day = datetime.datetime.today().weekday()
			
		# Check if the Sampling and MySQL  heartbeat flags have been reset, and if they haven't for a while, raise a major flag
		if run_mysql == False:	# if mysql not being used, always set mysql heartbeat flag to true
			mysql_heartBeat_flag = True

		if samp_heartBeat_flag == True: # if the sample heartbeat flag has been set, the thread is still alive
			samp_heartBeat_time = time.time() #reset the time of last flag
			samp_heartBeat_flag = False #reset the flag itself
		
		if mysql_heartBeat_flag == True: # if the mysql heartbeat flag has been set, the thread is still alive
			mysql_heartBeat_time = time.time() #reset the time of last flag
			mysql_heartBeat_flag = False #reset the flag itself
		
		if (time.time() - samp_heartBeat_time < sampleTime * 2) and (time.time() - mysql_heartBeat_time < mysqlTime * 2):
			n.notify("WATCHDOG=1") # notify systemd that all is still well
		
		if time.time() - samp_heartBeat_time > sampleTime * 2: # if two sample times have passed since a flag was raised, email out then send the kill signal
				error_report(Heartbeat_name,"","sampling stopped",1)
				kill_switch = True
		
		if time.time() - mysql_heartBeat_time > mysqlTime * 2: # if two mysql times have passed since a flag was raised, email out then send the kill signal
				error_report(Heartbeat_name,"","mysql stopped",1)
				kill_switch = True
				
		heartbeatLoop += 1 # increment the heartbeat loop counter
		time.sleep(5) # and go to sleep

#########################################################################################################################
#																														#
#	MySQL thread - Empties the queued sensor readings and saves them to the database									#
#																														#
#########################################################################################################################
def mysql():
	mysql_loops = 0 # zero the counter providing a visual indication of how many loops we have been through
	global mysql_heartBeat_flag
	while True:
		mysql_heartBeat_flag = True
		if Print_loop_count == True:
			print("mysql " + str(mysql_loops)) # tell everyone what number loop this is
		mysql_loops = mysql_loops + 1 #increment the loop counter
		db_mysql=MySQLdb.connect(host=config['db_host'],user=config['db_usr'],passwd=config['db_psw'],db=config['db_db'])
		c=db_mysql.cursor() # setup the MySQL cursor
		# load any previously failed queries into the queue
		while q_mysql:
			q.put(q_mysql.popleft())
		# convert the queue to sql queries
		x = 0
		while (not q.empty()) and (x == 0):
			resp = q.get() # load the queue
			q_mysql.append(resp) # load the queries into the backup queue in-case the commit fails
			resp_time = json.loads(resp)['time'] # de-json the time component
			resp_data = {} # setup a blank data componant as we need to load re-json'd data back into it
			resp_data["data"] = json.loads(resp)['data'] # de-json the data component into a dictionary
			resp_data_json = json.dumps(resp_data) # re-json the data component (this is all because the database wants just the data component in json form, but to get it through the queue all the data is originally one json block
			resp_id = json.loads(resp)['id'] # de-json the id component
			
			# prepare the SQL query
			insert = """INSERT INTO %s(data_datetime,data_sensor_id,data_values) VALUES('%s','%s','%s')"""
			insert = insert % (config['db_tbl'],resp_time,resp_id,resp_data_json)
			try:
				# add the query to SQL buffer
				c.execute(insert)
			except Exception as e:
				bad_item = q_mysql.pop()
				error_report(MySQL_name,e + bad_item,"Execute",0)
				x = 1
			
		# try and execute the MySQL query. If it doesn't work save the items for next time and log the failure
		if x == 0:
			try:
				db_mysql.commit() # commit the MySQL queries
				q_mysql.clear() # because it was a success, clear the backup queue
			# if the commit wasn't a success
			except Exception as e:
				error_report(MySQL_name,e,"Commit",0)

		db_mysql.close()	
		
		# have a little sleep
		time.sleep(mysqlTime)
	

#########################################################################################################################
#																														#
#	Timer thread - keeps all the threads in sync																		#
#																														#
#########################################################################################################################
def timer():
	print("Barriers = " + str(barrierCount + 1))
	global samp_heartBeat_flag
	loop = 0
	while True:
		samp_heartBeat_flag = True 				# Set the heartbeat flag to make sure the script isn't restarted
		
		if print_positions == True:
			print("timer at b1")
		b1.wait()								# wait until all modules are at b1
		if print_positions == True:
			print("timer while start")
		event.set()								# start the sampling period
		if print_positions == True:
			print("timer at b3")
		b3.wait()								# wait until all the modules are at b3, and then release them at once
		if print_positions == True:
			print("timer event set")
		if Print_loop_count == True:			# print and email the loop count if required
			print("loop " + str(loop))
		if (email_major_loop_count == True) and (loop % email_major_loop_value == 0):	
			try:
				sendEmail('',"TCSR node {1} @ {0} loop {2}".format(str(datetime.datetime.now().isoformat()),node_id,loop))
			except Exception as e:
				if print_errors == True:
					print("timer email loop count "+ str(e)) # print the error
		if loop == 0:							# Turn off the button status LED to show boot complete
			p.stop()
			GPIO.output(16,GPIO.LOW)
		loop = loop + 1
		halfTime.set()							# start the half sampling period
		if print_positions == True:
			print("timer halftime set")
		time.sleep(sampleTime/2)				# sleep for half the sample period
		halfTime.clear()						# clear the half sampling period allowing all sensors that sample half way through the period to do so
		if print_positions == True:
			print("timer halftime clear")
		time.sleep(sampleTime/2)				# have another sleep
		event.clear()							# end the sampling period
		if print_positions == True:
			print("timer event clear")

#########################################################################################################################
#																														#
#	Error logging thread - writes errors to log file																	#
#																														#
#########################################################################################################################

def write_to_log():
	global minor_error_count
	global major_error_count
	global restart_count

	while True:									# loop forever, independent of timers and barriers
		if not qe.empty():						# if the queue of actual error information is not empty, write it to the file
			f = open(config['log_file'], 'a+')
		while not qe.empty():
			msg = qe.get()
			f.write(msg)
			f.flush()
		else:
			f.close()	
				
		while not ql.empty():					# if the queue of updates to the error counter is not empty, update the counter
			f = open(config['error_file'], 'w')
			msg = ql.get()
			if msg == "i":
				minor_error_count += 1
			if msg == "a":
				major_error_count += 1
			if msg == "r":
				restart_count += 1
			f.write(str(minor_error_count) + "," + str(major_error_count) + "," + str(restart_count))
			f.flush()
			f.close()
			
		time.sleep(0.1)							# have a little sleep

#########################################################################################################################
#																														#
#	AMG8833 thread - Reads the AMG8833 thermal imaging camera															#
#																														#
#########################################################################################################################
def read_AMG8833():
	# AMG8833 connection stuff
	pinSCL = int(config['AMG883_pin_SCL'])
	pinSDA = int(config['AMG883_pin_SDA'])
	i2c = busio.I2C(pinSCL, pinSDA)
	amg = adafruit_amg88xx.AMG88XX(i2c)
	# wait for AMG to boot
	time.sleep(0.1)
	while True:													# Setup a few things at the start of each sample period
		if print_positions == True:
			print("AMG8833 at b1")
		b1.wait()
		if print_positions == True:
			print("AMG8833 at b3")
		b3.wait()
		if print_positions == True:
			print("max while halftime start")
		# Setup the default error values
		data = {}
		data["id"] = "n" + node_id + "s" + AMG8833_id
		data_available = False
		x = 0
		y = 0
		while x == 0:
			if not halfTime.isSet():							# once half-time has passed try and grab the data. 
				norm_pix = []
				try:
					for row in amg.pixels: # read sensor
						norm_pix = norm_pix + row
					data["data"] = norm_pix
					data["time"] = str(datetime.datetime.now().isoformat())
					data_available = True
					x = 1
				except Exception as e:							# if there is an error grabbing the data, log an error
					error_report(AMG8833_name,e,"Read",0)
					if y <= 20:
						y += 1
					else:
						error_report(AMG8833_name,"","Too many tries",0)
						x = 1
			time.sleep(0.5)	
		if print_positions == True:
			print("max halftime clear")
		if print_queue_data == True:
			print("AMG8833 " + str(json.dumps(data)))
		if data_available == True:
			q.put(json.dumps(data))								# load the data into the data queue

#########################################################################################################################
#																														#
#	Lepton thread - Reads the Lepton thermal imaging camera																#
#																														#
#########################################################################################################################
def read_Lepton():
	while True:													# Setup a few things at the start of each sample period
		if print_positions == True:
			print("Lepton at b1")
		b1.wait()
		if print_positions == True:
			print("Lepton at b3")
		b3.wait()
		if print_positions == True:
			print("Lepton while halftime start")
		# Setup the default error values
		data = {}
		data["id"] = "n" + node_id + "s" + Lepton_id
		data_available = False
		x = 0
		y = 0 
		while x == 0:
			if not halfTime.isSet():							# once half-time has passed try and grab the data. 
				try:
					if print_positions == True:	
						print("Lepton reading data")
					with Lepton(spi_dev = "/dev/spidev0.1") as l:
						norm_pix,_ = l.capture() # read sensor
					if print_positions == True:	
						print("Lepton converting data")
					norm_pix = np.squeeze(norm_pix) # remove the extra dimension
					norm_pix = (norm_pix/100)-273.15 # convert to decimal degrees C
					norm_pix = np.around(norm_pix, decimals=2) # round to 2 dp
					norm_pix = list(chain.from_iterable(norm_pix)) #break from extra arrays
					if print_positions == True:	
						print("Lepton queueing data")
					data["data"] = norm_pix
					data["time"] = str(datetime.datetime.now().isoformat())
					data_available = True
					x = 1
				except Exception as e:							# if there is an error grabbing the data, log an error
					error_report(Lepton_name,e,"Read",0)
					if y <= 20:
						y += 1
					else:
						error_report(Lepton_name,"","Too many tries",0)
						x = 1
			time.sleep(0.5)	
		if print_positions == True:	
			print("lepton halftime clear")
		if print_queue_data == True:
			print("Lepton " + str(json.dumps(data)))
		if data_available == True:		
			q.put(json.dumps(data))								# load the data into the data queue

#########################################################################################################################
#																														#
#	MDREVP	- Reads the ADS1115 ADC that reads the Modern Devices Rev-P air speed sensor								#
#																														#
#########################################################################################################################
def read_MDREVP():
	global PMV_as_value
	i2c = busio.I2C(int(config['MDREVP_pin_SCL']), int(config['MDREVP_pin_SDA']))
	#adc = ADS1115(i2c, address=int(config['MDREVP_i2c_add'],16))
	adc = ADS.ADS1115(i2c, address=int(config['MDREVP_i2c_add'],16))
	while True:													# Setup a few things at the start of each sample period
		if print_positions == True:
			print("MDREVP at b1")
		b1.wait()
		if print_positions == True:
			print("MDREVP at b3")
		b3.wait()
		if print_positions == True:
			print("MDREVP cleared wait")
		data = {}
		data["id"] = "n" + node_id + "s" + MDREVP_id
		data_available = False
		airspeed = 0
		count = 0
		t = 0
		v = 0
		x = 0
		ADSflag = False
		if print_positions == True:
			print("MDREVP while start")
		while event.isSet():									# For the whole sample period read the data every 0.5 seconds and build up the average and standard deviation
			if print_positions == True:
				print("MDREVP while event start")
			try:
				t = (AnalogIn(adc, int(config['MDREVP_ch_A']), int(config['MDREVP_ch_B'])).voltage - 0.400)/0.0195 # read the air temp
				v = AnalogIn(adc, int(config['MDREVP_ch_C']), int(config['MDREVP_ch_D'])).voltage # read the air velocity
				if t > 0 and v > 1.3692:						# If value valid, convert air speed to m/s
					mph1 = v-1.3692
					mph2 = 3.038517*math.pow(t,0.115157)
					mph3 = (mph1/mph2)/0.087288
					mph = math.pow(mph3,3.009364)
					ms = mph*0.44704
					airspeed = airspeed + ms
					if count == 0:	# update the standard deviation
						DS1115_stddev = [ms]
					else:
						DS1115_stddev = np.append(DS1115_stddev,[ms])
					count = count + 1								# update the sample count for average at the end
			except Exception as e:								# if there is an error grabbing the data, log an error
				error_report(MDREVP_name,e,"Read",0)
			if not halfTime.isSet() and x == 0:					# at half time log the time, which is used for the average of the whole sample period
				if print_positions == True:
					print("MDREVP while event halftime")
				data["time"] = str(datetime.datetime.now().isoformat())
				x = 1
			time.sleep(0.5)										# have a little sleep before taking another sample
		if print_positions == True:
			print("MDREVP wait clear")
		if count == 0:											# do a little error checking to make sure the count and value is valid
			ADSflag = True
		if airspeed == 0:
			ADSflag = True
		if ADSflag == False:									# if count and value is valid, take the average and prepare the date to be loaded into the queue
			PMV_as_value = airspeed/count
			data["data"] = [PMV_as_value]
			data["data"].append(np.std(DS1115_stddev))
			data["data"].append(count)
			data_available = True
		if print_queue_data == True:
			print("MDREVP " + str(json.dumps(data)))
		if data_available == True:	
			q.put(json.dumps(data))								# load the data into the data queue
		if module_PMV or module_OccT:							# hold the module if the PMV or OccT calculation is being done so as not to overwrite the data
			if print_positions == True:
				print("MDREVP waiting for PMV/OccT calculation")
			b4r.wait()

#########################################################################################################################
#																														#
#	SHT75 thread - Reads the SHT75 Air temperature and Humidity sensor													#
#																														#
#########################################################################################################################
def read_SHT75():
	global PMV_ta_value
	global PMV_rh_value
	sensor = SHT1x(int(config['SHT75_pin_data']), int(config['SHT75_pin_SCK']),gpio_mode=GPIO.BCM)
	while True:
		if print_positions == True:								# Setup a few things at the start of each sample period
			print("SHT75 at b1")
		b1.wait()
		if print_positions == True:
			print("SHT75 at b3")
		b3.wait()
		if print_positions == True:
			print("SHT while halftime start")
		dataT = {}
		dataH = {}
		dataT["id"] = "n" + node_id + "s" + SHT75_T_id
		dataH["id"] = "n" + node_id + "s" + SHT75_H_id
		data_available = False
		x = 0
		y = 0
		while x == 0:
			if not halfTime.isSet():							# once half-time has passed try and grab the data and apply the calibration offsets. 
				try:
					tmpairTempG = sensor.read_temperature() + SHT75_T_offset
					dataT["data"] = [tmpairTempG]
					if (PMV_ta_module == "SHT75" and (module_PMV or module_OccT)):
						PMV_ta_value = tmpairTempG
					dataT["time"] = str(datetime.datetime.now().isoformat())
					tmpairTempH = sensor.read_humidity(tmpairTempG) + SHT75_H_offset
					dataH["data"] = [tmpairTempH]
					if (PMV_rh_module == "SHT75" and (module_PMV or module_OccT)):
						PMV_rh_value = tmpairTempH
					dataH["time"] = str(datetime.datetime.now().isoformat())
					x = 1
					data_available = True
				except Exception as e:							# if there is an error grabbing the data, log an error
					error_report(SHT75_name,e,"Read",0)
					if y <= 20:
						y += 1
					else:
						error_report(SHT75_name,"","Too many tries",0)
						x = 1
			time.sleep(0.5)
		if print_positions == True:
			print("SHT halftime clear")	
		if print_queue_data == True:
			print("SHT75 Temp " + str(json.dumps(dataT)))
			print("SHT75 Humid " + str(json.dumps(dataH)))
		if data_available == True:								# load the data into the data queue
			q.put(json.dumps(dataT))
			q.put(json.dumps(dataH))
		if module_PMV or module_OccT:							# hold the module if the PMV or OccT calculation is being done so as not to overwrite the data
			if (PMV_ta_module == "SHT75" or PMV_rh_module == "SHT75"):
				if print_positions == True:
					print("SHT75 waiting for PMV/OccT calculation")
				if PMV_ta_module == "SHT75":
					b4t.wait()
				if PMV_rh_module == "SHT75":
					b4r.wait()

#########################################################################################################################
#																														#
#	MAX3186 thread - reads the MAX3186 PT100 interface, which reads the PT100 radiant temperature sensor				#
#																														#
#########################################################################################################################
def read_MAX31865():
	global PMV_tr_value
	max = max31865.max31865(int(config['MAX31865_pin_CS']),int(config['MAX31865_pin_DI']),int(config['MAX31865_pin_DO']),int(config['MAX31865_pin_CLK']))
	while True:													# Setup a few things at the start of each sample period
		if print_positions == True:
			print("MAX31865 at b1")
		b1.wait()
		if print_positions == True:
			print("MAX31865 at b3")
		b3.wait()
		data = {}
		data["id"] = "n" + node_id + "s" + MAX31865_id
		data_available = False
		if print_positions == True:
			print("max while halftime start")
		x = 0
		y = 0
		while x == 0:
			if not halfTime.isSet():							# once half-time has passed try and grab the data and apply the calibration offsets. 
				try:
					PMV_tr_value = max.readTemp() + MAX31865_offset
					data["data"] = [PMV_tr_value]
					data["time"] = str(datetime.datetime.now().isoformat())
					data_available = True
					x = 1
				except Exception as e:							# if there is an error grabbing the data, log an error
					error_report(MAX31865_name,e,"Read",0)
					if y <= 20:
						y += 1
					else:
						error_report(MAX31865_name,"","Too many tries",0)
						x = 1
			time.sleep(0.5)
		if print_positions == True:
			print("max halftime clear")
		if print_queue_data == True:
			print("MAX31865 " + str(json.dumps(data)))
		if data_available == True:
			q.put(json.dumps(data))								# load the data into the data queue
		if module_PMV or module_OccT:							# hold the module if the PMV or OccT calculation is being done so as not to overwrite the data
			if print_positions == True:
				print("MAX31865 waiting for PMV/OccT calculation")
			b4r.wait()

#########################################################################################################################
#																														#
#	SCD30 thread - reads the SCD30 CO2, temperature and humidity sensor													#
#																														#
#########################################################################################################################
def read_SCD30():
	global PMV_ta_value
	global PMV_rh_value
	i2c = busio.I2C(int(config['SCD30_pin_SCL']), int(config['SCD30_pin_SDA']), frequency=100000)
	# Create library object on our I2C port
	scd30 = AT_SCD30.AT_SDC30_I2C(i2c)
	scd30.setMeasurementInterval(int(config['SCD30_measurement_interval']))
	#scd30.setAutoSelfCalibration(True)
	scd30.beginMeasuring(int(config['SCD30_initial_pressure']))
	while True:													# Setup a few things at the start of each sample period
		if print_positions == True:
			print("SCD30 at b1")
		b1.wait()
		if print_positions == True:
			print("SCD30 at b3")
		b3.wait()
		data_C = {}
		data_T = {}
		data_H = {}		
		data_C["id"] = "n" + node_id + "s" + SCD30_C_id
		data_T["id"] = "n" + node_id + "s" + SCD30_T_id
		data_H["id"] = "n" + node_id + "s" + SCD30_H_id
		data_available = False
		if print_positions == True:
			print("SCD30 while halftime start")
		x = 0
		y = 0
		z = 0
		while x == 0:
			if not halfTime.isSet():							# hold off the measurement until half-time has passed
				if print_positions == True:
					print("SCD30 b2 wait")
				b2.wait() # wait until BME280 or fake BME280 have recorded the air pressure
				if print_positions == True:
					print("SCD30 while halftime start")
				try:				
					scd30.beginMeasuring(int(round(BME280_p))) # set the air pressure offset
				except:
					x = 1
					z = 1
					error_report(SCD30_name,"","Failed to set pressure offset",0)	
				while z == 0:											# try upto 30 times to take a sample
					if y > 30:
						error_report(SCD30_name,"","Too many failed tries",0)	
						z = 1
					
					if z ==0:
						if print_errors == True:
							if y != 0:
								print("SCD30 try " + str(y))
								
						try:
							CO2_read = scd30.readMeasurement()			# take the measurement
						except Exception as e:							# if there is an error grabbing the data, log an error
							error_report(SCD30_name,e,"Read",0)	
							y += 1
						else:
							if (CO2_read[0].item() != False) and (is_nan(CO2_read[0].item())):
								z = 1
								data_available = True
							else:
								y += 1
								error_report(SCD30_name,"","Bad read",0)	
								
					time.sleep(sampleTime/30)							# have a little sleep inbetween read attempts
																		
				data_C["data"] = [CO2_read[0].item() + SCD30_C_offset]	# apply the calibration offsets and prepare the data to be loaded into the queue
				data_C["time"] = str(datetime.datetime.now().isoformat())
				SCD30tmpT = CO2_read[1].item() + SCD30_T_offset
				if (PMV_ta_module == "SCD30" and (module_PMV or module_OccT)):
					PMV_ta_value = SCD30tmpT
				data_T["data"] = [SCD30tmpT]
				data_T["time"] = str(datetime.datetime.now().isoformat())
				SCD30tmpH = CO2_read[2].item() + SCD30_H_offset
				if (PMV_rh_module == "SCD30" and (module_PMV or module_OccT)):
					PMV_rh_value = SCD30tmpH
				data_H["data"] = [SCD30tmpH]
				data_H["time"] = str(datetime.datetime.now().isoformat())
				data_C["data"].append(int(round(BME280_p)))
				if print_queue_data == True:
					print("SCD30_C " + str(json.dumps(data_C)))
					print("SCD30_T " + str(json.dumps(data_T)))
					print("SCD30_H " + str(json.dumps(data_H)))
				if data_available == True:								# load the data into the data queue
					q.put(json.dumps(data_C))
					q.put(json.dumps(data_T))
					q.put(json.dumps(data_H))
				x = 1
			time.sleep(0.5)	
		if print_positions == True:
			print("SCD30 halftime clear")
		if module_PMV or module_OccT:									# hold the module if the PMV or OccT calculation is being done so as not to overwrite the data
			if (PMV_ta_module == "SCD30" or PMV_rh_module == "SCD30"):
				if print_positions == True:
					print("SCD30 waiting for PMV/OccT calculation")
				if PMV_ta_module == "SCD30":
					b4t.wait()
				if PMV_rh_module == "SCD30":
					b4r.wait()

#########################################################################################################################
#																														#
#	SGP30 thread - reads the SGP30 VOC sensor																			#
#																														#
#########################################################################################################################
def read_SGP30():
	db_SGP30=MySQLdb.connect(host=config['db_host'],user=config['db_usr'],passwd=config['db_psw'],db=config['db_db'])
	i2c = busio.I2C(int(config['SGP30_pin_SCL']), int(config['SGP30_pin_SDA']))
	# Create library object on our I2C port
	sgp30 = adafruit_sgp30.Adafruit_SGP30(i2c)
	sgp30.iaq_init()
	
	c=db_SGP30.cursor()										# prepare the mysql query to pull the previous co2 equivalent and total voc bases
	SGP30_full_id = "n" + node_id + "s" + SGP30_id
	select = """SELECT data_values,data_datetime FROM %s WHERE data_sensor_id LIKE '%s' ORDER BY data_datetime DESC LIMIT 1"""
	select = select % (config['db_tbl'],SGP30_full_id)
	z = 0
	zcount = 0
	co2eq_base = 0
	tvoc_base = 0
	prov_base_age = 0
	while z == 0:											# try upto 10 time to pull the co2eq and tvoc bases from the database
		if zcount == 10:									# if database query fails ten times, set the bases and age to 0
			error_report(SGP30_name,"","Base fetch failed 10 times",0)			
			co2eq_base = 0
			tvoc_base = 0
			prov_base_age = 0
			z = 1
		if (db_SGP30) and (zcount < 10):					# try and query the database for the bases and ages
			try:
				c.execute(select)
				SGP30_mysql_result, = c.fetchall()
			except Exception as e:
				error_report(SGP30_name,e,"Base fetch failed",0)				
				zcount += 1
			else:
				prov_tvoc_base = json.loads(SGP30_mysql_result[0])['data'][2]
				prov_co2eq_base = json.loads(SGP30_mysql_result[0])['data'][1]
				prov_base_age = json.loads(SGP30_mysql_result[0])['data'][3]
				z = 1
		time.sleep(2)

	db_SGP30.close()
	time_init = time.time()
	if (prov_base_age < 43200) or ((datetime.datetime.now()-SGP30_mysql_result[1]).days >= 7): # if baseline age is less than 12 hours old, or hasn't been updated in 7 days set baselines to 0 and base age to now
		co2eq_base = 0
		tvoc_base = 0
		base_age = time.time()-time_init
	else:
		tvoc_base = prov_tvoc_base
		co2eq_base = prov_co2eq_base
		base_age = prov_base_age
		sgp30.set_iaq_baseline(co2eq_base, tvoc_base)
	init_base_age = base_age

	while True: 													# Setup a few things at the start of each sample period
		if print_positions == True:
			print("SGP30 at b1")
		b1.wait()
		if print_positions == True:
			print("SGP30 at b3")
		b3.wait()
		data = {}
		data_available = False
		data["id"] = SGP30_full_id
		if print_positions == True:
			print("SGP30 while halftime start")
		x = 0
		y = 0
		while x == 0:
			if not halfTime.isSet():							# once half-time has passed try and grab the data and apply the calibration offsets. 
				if base_age > 43200: # once baseline is older than 12 hours, grab it again each loop
					co2eq_base = sgp30.baseline_eCO2
					tvoc_base = sgp30.baseline_TVOC
				base_age = init_base_age + (time.time()-time_init)
				try:
					data["data"] = [sgp30.TVOC + SGP30_offset]
					data["data"].append(co2eq_base)
					data["data"].append(tvoc_base)
					data["data"].append(base_age)
					data["time"] = str(datetime.datetime.now().isoformat())
					data_available = True
					x = 1
				except Exception as e:							# if there is an error grabbing the data, log an error
					error_report(SGP30_name,e,"Read",0)					
					if y <= 20:
						y += 1
					else:
						error_report(SGP30_name,"","Too many tries",0)						
						x = 1
			time.sleep(0.5)	
		if print_positions == True:
			print("SGP30 halftime clear")
		if print_queue_data == True:
			print("SGP30 " + str(json.dumps(data)))
		if data_available == True:
			q.put(json.dumps(data))								# load the data into the data queue

#########################################################################################################################
#																														#
#	BME280 thread - reads the BME280 Air Temperature, Pressure and Humidity sensor										#
#																														#
#########################################################################################################################
def read_BME280():
	i2c = busio.I2C(int(config['BME280_pin_SCL']), int(config['BME280_pin_SDA']))
	bme280 = adafruit_bme280.Adafruit_BME280_I2C(i2c, address=int(config['BME280_i2c_add'],16))
	global BME280_p
	global PMV_ta_value
	global PMV_rh_value
	while True: 													# Setup a few things at the start of each sample period
		if print_positions == True:
			print("BME280 at b1")
		b1.wait()
		if print_positions == True:
			print("BME280 at b3")
		b3.wait()
		dataT = {}
		dataP = {}
		dataH = {}
		dataT["id"] = "n" + node_id + "s" + BME280_T_id
		dataP["id"] = "n" + node_id + "s" + BME280_P_id
		dataH["id"] = "n" + node_id + "s" + BME280_H_id
		data_available = False
		if print_positions == True:
			print("BME280 while halftime start")
		x = 0
		y = 0
		while x == 0:
			if not halfTime.isSet():							# once half-time has passed try and grab the data and apply the calibration offsets. 
				try:
					BME280tmpT = bme280.temperature + BME280_T_offset
					if (PMV_ta_module == "BME280" and module_PMV):
						PMV_ta_value = BME280tmpT
					dataT["data"] = [BME280tmpT]
					BME280_p = bme280.pressure + BME280_P_offset
					dataP["data"] = [BME280_p]
					BME280tmpH = bme280.humidity + BME280_H_offset
					if (PMV_rh_module == "BME280" and module_PMV):
						PMV_rh_value = BME280tmpH
					dataH["data"] = [BME280tmpH]
					dataT["time"] = str(datetime.datetime.now().isoformat())
					dataP["time"] = str(datetime.datetime.now().isoformat())
					dataH["time"] = str(datetime.datetime.now().isoformat())
					data_available = True
					x = 1
				except Exception as e:							# if there is an error grabbing the data, log an error
					error_report(BME280_name,e,"Read",0)					
					if y <= 20:
						y += 1
					else:
						error_report(BME280_name,"","Too many tries",0)							
						x = 1
			time.sleep(0.5)
		if module_BME280 == True:
			if print_positions == True:
				print("BME280 b2 wait")
			b2.wait()	# allow SCD30 to take it's sample
		if print_positions == True:
			print("BME280 halftime clear")
		if print_queue_data == True:
			print("BME280 Temp " + str(json.dumps(dataT)))
			print("BME280 Pressure " + str(json.dumps(dataP)))
			print("BME280 Humidity " + str(json.dumps(dataH)))
		if data_available == True:								# load the data into the data queue
			q.put(json.dumps(dataT))
			q.put(json.dumps(dataP))
			q.put(json.dumps(dataH))
		if module_PMV or module_OccT:							# hold the module if the PMV or OccT calculation is being done so as not to overwrite the data
			if (PMV_ta_module == "BME280" or PMV_rh_module == "BME280"):
				if print_positions == True:
					print("BME280 waiting for PMV/OccT calculation")
				if PMV_ta_module == "BME280":
					b4t.wait()
				if PMV_rh_module == "BME280":
					b4r.wait()
		
#########################################################################################################################
#																														#
#	fake BME280 thread - sets a fake pressure value for testing of the SCD30 sensor										#
#																														#
#########################################################################################################################
def read_fakeBME280():
	global BME280_p
	while True: 													# Setup a few things at the start of each sample period
		if print_positions == True:
			print("fakeBME280 at b1")
		b1.wait()
		if print_positions == True:
			print("fakeBME280 at b3")
		b3.wait()
		if print_positions == True:
			print("fakeBME280 while halftime start")
		x = 0
		while x == 0:
			if not halfTime.isSet():							# once half-time set an fixed value for pressure
				BME280_p = 1000
				x = 1
			time.sleep(0.5)
		if module_SCD30 == True:
			if print_positions == True:
				print("fakeBME280 b2 wait")
			b2.wait()	# allow SCD30 to take it's sample
		if print_positions == True:
			print("fakeBME280 halftime clear")

#########################################################################################################################
#																														#
#	PMS5003 thread - reads the PMS5003 particulate sensor																#
#																														#
#########################################################################################################################
def read_PMS5003():
	while True: 													# Setup a few things at the start of each sample period
		if print_positions == True:
			print("PMS5003 at b1")
		b1.wait()
		if print_positions == True:
			print("PMS5003 at b3")
		b3.wait()
		data = {}
		data["id"] = "n" + node_id + "s" + PMS5003_id
		data_available = False
		if print_positions == True:
			print("PMS5003 while halftime start")
		x = 0
		y = 0
		while x == 0:
			if not halfTime.isSet():							# once half-time has passed try and grab the data
				try:
					PMS_data = read_data()
					if not PMS_data:
						raise ValueError('no data')
					if PMS_data['errcode'] != '\0':
						raise ValueError('got error: {}'.format(PMS_data['errcode']))
					data["data"] = []
					for k in sorted(PMS_data['data'], key=lambda x: int(x)):
						v = PMS_data['data'][k]
						data["data"].append(v[1])
					data["time"] = str(datetime.datetime.now().isoformat())
					data_available = True
					x = 1
				except Exception as e:							# if there is an error grabbing the data, log an error
					error_report(PMS5003_name,e,"Read",0)						
					if y <= 20:
						y += 1
					else:
						error_report(PMS5003_name,"","Too many tries",0)						
						x = 1
			time.sleep(0.5)
		if print_positions == True:
			print("PMS5003 halftime clear")
		if print_queue_data == True:
			print("PMS5003 " + str(json.dumps(data)))
		if data_available == True:
			q.put(json.dumps(data))								# load the data into the data queue	

#########################################################################################################################
#																														#
#	SEN0232	- Reads the ADS1115 ADC that reads the Gravity SEN0232 Decibel sensor										#
#																														#
#########################################################################################################################
def read_SEN0232():
	i2c = busio.I2C(int(config['SEN0232_pin_SCL']), int(config['SEN0232_pin_SDA']))
	#adc = ADS1115(i2c, address=int(config['SEN0232_i2c_add'],16))
	adc = ADS.ADS1115(i2c, address=int(config['SEN0232_i2c_add'],16))
	while True:													# Setup a few things at the start of each sample period
		if print_positions == True:
			print("SEN0232 at b1")
		b1.wait()
		if print_positions == True:
			print("SEN0232 at b3")
		b3.wait()
		if print_positions == True:
			print("SEN0232 cleared wait")
		data = {}
		data["id"] = "n" + node_id + "s" + SEN0232_id
		data_available = False
		decibel = 0
		count = 0
		x = 0
		SENflag = False
		if print_positions == True:
			print("SEN0232 while start")
		while event.isSet():									# For the whole sample period read the data every 0.5 seconds and build up the average and standard deviation
			if print_positions == True:
				print("SEN0232 while event start")
			try:
				#d = adc[(int(config['SEN0232_ch_A']),int(config['SEN0232_ch_B']))].volts*-50
				d = AnalogIn(adc,int(config['SEN0232_ch_A']), int(config['SEN0232_ch_B'])).voltage*-50 # read the decibel level
			except Exception as e:								# if there is an error grabbing the data,
				error_report(SEN0232_name,e,"Read",0)				
			else:
				decibel = decibel + d
				if count == 0:	# update the standard deviation
					DS1115_stddev = [d]
				else:
					DS1115_stddev = np.append(DS1115_stddev,[d])
				count = count + 1								# update the sample count for average at the end
			if not halfTime.isSet() and x == 0:					# at half time log the time, which is used for the average of the whole sample period
				if print_positions == True:
					print("SEN0232 while event halftime")
				data["time"] = str(datetime.datetime.now().isoformat())
				x = 1
			time.sleep(0.5)										# have a little sleep before taking another sample
		if print_positions == True:
			print("SEN0232 wait clear")
		if count == 0:											# do a little error checking to make sure the count and value is valid
			SENflag = True
		if decibel == 0:
			SENflag = True
		if SENflag == False:									# if count and value is valid, take the average and prepare the date to be loaded into the queue
			data["data"] = [decibel/count]
			data["data"].append(np.std(DS1115_stddev))
			data["data"].append(count)
			data_available = True
		if print_queue_data == True:
			print("SEN0232 " + str(json.dumps(data)))
		if data_available == True:
			q.put(json.dumps(data))								# load the data into the data queue
	
#########################################################################################################################
#																														#
#	TSL2561 thread - reads the TSL2561 LUX sensor																			#
#																														#
#########################################################################################################################
def read_TSL2561():
	i2c = busio.I2C(int(config['TSL2561_pin_SCL']), int(config['TSL2561_pin_SDA']))
	# Create library object on our I2C port
	sensor = adafruit_tsl2561.TSL2561(i2c)
	sensor.gain = int(config['TSL2561_gain']) 												# A value of 0 is low gain mode, and a value of 1 is high gain / 16x mode.
	sensor.integration_time = int(config['TSL2561_integration_time']) 										# Get and set the integration time of the sensor.  A value 0 is 13.7ms, 1 is 101ms, 2 is 402ms, and 3 is manual mode.

	while True: 													# Setup a few things at the start of each sample period
		if print_positions == True:
			print("TSL2561 at b1")
		b1.wait()
		if print_positions == True:
			print("TSL2561 at b3")
		b3.wait()
		dataB = {}
		dataI = {}
		dataL = {}
		dataB["id"] = "n" + node_id + "s" + TSL2561_B_id
		dataI["id"] = "n" + node_id + "s" + TSL2561_I_id
		dataL["id"] = "n" + node_id + "s" + TSL2561_L_id
		data_available = False
		if print_positions == True:
			print("TSL2561 while halftime start")
		x = 0
		y = 0
		while x == 0:
			if not halfTime.isSet():							# once half-time has passed try and grab the data
				try:
					dataB["data"] = [sensor.broadband]
					dataI["data"] = [sensor.infrared]
					dataL["data"] = [sensor.lux]
					dataB["time"] = str(datetime.datetime.now().isoformat())
					dataI["time"] = str(datetime.datetime.now().isoformat())
					dataL["time"] = str(datetime.datetime.now().isoformat())
					data_available = True
					x = 1
				except Exception as e:							# if there is an error grabbing the data, log an error
					error_report(TSL2561_name,e,"Read",0)					
					if y <= 20:
						y += 1
					else:
						error_report(TSL2561_name,"","Too many tries",0)						
						x = 1
			time.sleep(0.5)	
		if print_positions == True:
			print("TSL2561 halftime clear")
		if print_queue_data == True:
			print("TSL2561 broadband " + str(json.dumps(dataB)))
			print("TSL2561 infrared " + str(json.dumps(dataI)))
			print("TSL2561 LUX " + str(json.dumps(dataL)))
		if data_available == True:								# load the data into the data queue
			q.put(json.dumps(dataB))
			q.put(json.dumps(dataI))
			q.put(json.dumps(dataL))

#########################################################################################################################
#																														#
#	PMV thread - generates a PMV value																					#
#																														#
#########################################################################################################################
def generate_PMV():

	data = {}
	data["id"] = "n" + node_id + "s" + PMV_id

	while True:
		if print_positions == True:
			print("PMV at b1")
		b1.wait()												# wait until every module is ready to start
		if print_positions == True:
			print("PMV at b3")
		b3.wait()												# wait until every module is ready to start and sample period has begun
		if print_positions == True:
			print("PMV at b4 temp")
		b4t.wait()												# wait until the PMV temperature sample has been taken
		if print_positions == True:
			print("PMV at b4 rest")
		b4r.wait()												# wait until the rest of the PMV samples have been taken
		if print_positions == True:
			print("PMV at calculation")
		pmv_value = PMV.comfPMV(PMV_ta_value, PMV_tr_value, PMV_as_value, PMV_rh_value, float(config['PMV_met']), float(config['PMV_clo']), 0) # calculate PMV value
		data["data"] = [pmv_value[0]]							# prepare the date to be loaded into the queue
		data["data"].append(PMV_ta_value)
		data["data"].append(PMV_tr_value)
		data["data"].append(PMV_as_value)
		data["data"].append(PMV_rh_value)
		data["data"].append(float(config['PMV_met']))
		data["data"].append(float(config['PMV_clo']))
		data["data"].append(0)
		data["data"].append(pmv_value[1])	# ppd value
		data["time"] = str(datetime.datetime.now().isoformat())	
		if print_positions == True:
			print("PMV calculation done")
		if print_queue_data == True:
			print("PMV " + str(json.dumps(data)))
		q.put(json.dumps(data))									# load the data into the data queue

#########################################################################################################################
#																														#
#	Weather API current thread - gets the current weather from Darksky's API											#
#																														#
#########################################################################################################################
def get_weatherAPIcurrent():
	weatherAPI_loops = 0 # zero the counter providing a visual indication of how many loops we have been through
	# Setup the arrays to queue data for the database
	dataW = {}
	dataW["id"] = "n" + node_id + "s" + weatherAPIcurrent_id
	dataM = {}
	dataM["id"] = "n" + node_id + "s" + weatherAPI_mean_id
	dataR = {}
	dataR["id"] = "n" + node_id + "s" + weatherAPI_running_mean_id
	url="https://api.darksky.net/forecast/"+weatherAPIcurrent_api_key+"/"+loc_lat+","+loc_long+"?units=si"

	# A few variables that are used
	weatherAPI_prev_run = ""
	today_mean_temp_value = 0
	today_mean_temp_count = 0
	prev_day_mean_temp_value = 0
	prev_day_mean_temp_count = 0

	while True:
		if Print_loop_count == True:
			print("weatherAPI " + str(weatherAPI_loops)) # tell everyone what number loop this is
		weatherAPI_loops = weatherAPI_loops + 1
		weatherAPI_new_day = 0
		prev_running_mean_value = 0.0
		prev_running_mean_days = 0.0
		prev_running_mean_confidence = 0.0
		running_mean_value = 0.0
		running_mean_days = 0.0
		running_mean_confidence = 0.0
		running_mean_valid = True
		running_mean_retry = False
		running_calc_used = 0

		# grab the weather data from the darksky API
		try:
			meteo=urlopen(url).read()
		except Exception as e:							# if there is an error grabbing the data, log an error
			error_report(weatherAPIcurrent_name,e," error grabbing the weather data",0)
		else:
			# If the data is available, prepare it to be pushed to the database
			meteo = meteo.decode('utf-8')
			weather = json.loads(meteo)
			if "temperature" in weather['currently']:
				dataW["data"] = [weather['currently']['temperature']]
				if "summary" in weather['currently']:
					dataW["data"].append(weather['currently']['summary'])
				else:
					dataW["data"].append("")
				if "nearestStormDistance" in weather['currently']:
					dataW["data"].append(weather['currently']['nearestStormDistance'])
				else:
					dataW["data"].append("")
				if "nearestStormBearing" in weather['currently']:
					dataW["data"].append(weather['currently']['nearestStormBearing'])
				else:
					dataW["data"].append("")
				if "precipIntensity" in weather['currently']:
					dataW["data"].append(weather['currently']['precipIntensity'])
				else:
					dataW["data"].append("")
				if "precipProbability" in weather['currently']:
					dataW["data"].append(weather['currently']['precipProbability'])
				else:
					dataW["data"].append("")
				if "apparentTemperature" in weather['currently']:
					dataW["data"].append(weather['currently']['apparentTemperature'])
				else:
					dataW["data"].append("")
				if "dewPoint" in weather['currently']:
					dataW["data"].append(weather['currently']['dewPoint'])
				else:
					dataW["data"].append("")
				if "humidity" in weather['currently']:
					dataW["data"].append(weather['currently']['humidity'])
				else:
					dataW["data"].append("")
				if "pressure" in weather['currently']:
					dataW["data"].append(weather['currently']['pressure'])
				else:
					dataW["data"].append("")
				if "windSpeed" in weather['currently']:
					dataW["data"].append(weather['currently']['windSpeed'])
				else:
					dataW["data"].append("")
				if "windGust" in weather['currently']:
					dataW["data"].append(weather['currently']['windGust'])
				else:
					dataW["data"].append("")
				if "windBearing" in weather['currently']:
					dataW["data"].append(weather['currently']['windBearing'])
				else:
					dataW["data"].append("")
				if "cloudCover" in weather['currently']:
					dataW["data"].append(weather['currently']['cloudCover'])
				else:
					dataW["data"].append("")
				if "uvIndex" in weather['currently']:
					dataW["data"].append(weather['currently']['uvIndex'])
				else:
					dataW["data"].append("")
				if "visibility" in weather['currently']:
					dataW["data"].append(weather['currently']['visibility'])
				else:
					dataW["data"].append("")
				if "ozone" in weather['currently']:
					dataW["data"].append(weather['currently']['ozone'])
				else:
					dataW["data"].append("")
				dataW["time"] = str(datetime.datetime.now().isoformat())	
				if print_positions == True:
					print("weatherAPIcurrent get done")
				if print_queue_data == True:
					print("weatherAPIcurrent " + str(json.dumps(dataW)))
				q.put(json.dumps(dataW))									# load the data into the data queue

				# generate the mean and exponentially weighted running means

				# save the previous mean temp value and count for exponentially weighted running mean calc
				prev_day_mean_temp_value = today_mean_temp_value
				prev_day_mean_temp_count = today_mean_temp_count

				#	if the first sample after midnight reset mean sum and count values to zero, and set flag for the previous days Exponentially weighted running mean to be calculated

				if datetime.datetime.now().strftime("%w") != weatherAPI_prev_run: # if midnight, reset the mean temp value and count
					today_mean_temp_value = 0.0
					today_mean_temp_count = 0.0
					weatherAPI_new_day = 1

				#	if current and previous day mean temp exists, use them, else go and check the database for them
				# 		else calculate the day mean temp and write it to a local variable and the database
				if today_mean_temp_count == 0:
					db_weatherAPI=MySQLdb.connect(host=config['db_host'],user=config['db_usr'],passwd=config['db_psw'],db=config['db_db'])
					c=db_weatherAPI.cursor()										# prepare the mysql query to pull the previous mean temperature data from the database
					select = """SELECT data_values,data_datetime FROM %s WHERE data_sensor_id LIKE '%s' AND data_datetime >= CURDATE() AND data_datetime < CURDATE() + INTERVAL 1 DAY ORDER BY data_datetime DESC LIMIT 1"""
					select = select % (config['db_tbl'],"n" + node_id + "s" + weatherAPI_mean_id)
					z = 0
					zcount = 0
					while z == 0:											# try upto 10 time to pull the previous mean outdoor temp data from the database
						if zcount == 10:									# if database query fails ten times, log an error
							error_report(weatherAPIcurrent_name,""," mean outdoor temperature fetch failed 10 times",0)			
							z = 1
						if (db_weatherAPI) and (zcount < 10):					# try and query the database for the current mean outdoor temperature data
							try:
								c.execute(select)
							except Exception as e:
								error_report(weatherAPIcurrent_name,e," mean outdoor temperature fetch failed",0)				
								zcount += 1
							else:
								if c.rowcount:
									weatherAPI_mysql_mean_temp_result, = c.fetchall()
									today_mean_temp_value = json.loads(weatherAPI_mysql_mean_temp_result[0])['data'][0] # grab the current mean outdoor temperature
									today_mean_temp_count = json.loads(weatherAPI_mysql_mean_temp_result[0])['data'][1] # grab the current mean outdoor temperature count
								else:
									error_report(weatherAPIcurrent_name,""," no mean outdoor temperature data found, resetting to zero",0)				
								z = 1
						time.sleep(2)
					

					select = """SELECT data_values,data_datetime FROM %s WHERE data_sensor_id LIKE '%s' AND data_datetime >= CURDATE() - INTERVAL 1 DAY AND data_datetime < CURDATE() ORDER BY data_datetime DESC LIMIT 1"""
					select = select % (config['db_tbl'],"n" + node_id + "s" + weatherAPI_mean_id)
					z = 0
					zcount = 0
					while z == 0:											# try upto 10 time to pull the previous mean outdoor temp data from the database
						if zcount == 10:									# if database query fails ten times, log an error
							error_report(weatherAPIcurrent_name,""," previous day mean outdoor temperature fetch failed 10 times",0)			
							z = 1
						if (db_weatherAPI) and (zcount < 10):					# try and query the database for the previous day mean outdoor temperature data
							try:
								c.execute(select)
							except Exception as e:
								error_report(weatherAPIcurrent_name,e," previous day mean outdoor temperature fetch failed",0)				
								zcount += 1
							else:
								if c.rowcount:
									weatherAPI_mysql_mean_temp_result, = c.fetchall()
									prev_day_mean_temp_value = json.loads(weatherAPI_mysql_mean_temp_result[0])['data'][0] # grab the previous day mean outdoor temperature
									prev_day_mean_temp_count = json.loads(weatherAPI_mysql_mean_temp_result[0])['data'][1] # grab the previous day mean outdoor temperature count
								else:
									error_report(weatherAPIcurrent_name,""," no previous day mean outdoor temperature data found, resetting to zero",0)				
								z = 1
						time.sleep(2)

					db_weatherAPI.close()

				today_mean_temp_value += weather['currently']['temperature']			# add the latest air temp reading to todays mean outdoor temp, then increment the count, then load them both back into the database
				today_mean_temp_count += 1
				dataM["data"] = [today_mean_temp_value]
				dataM["data"].append(today_mean_temp_count)
				dataM["time"] = str(datetime.datetime.now().isoformat())
				if print_queue_data == True:
					print("weatherAPIcurrent " + str(json.dumps(dataM)))	
				q.put(json.dumps(dataM))									# load the data into the data queue

				#	if midnight, check if a Exponentially weighted running mean is in the database
				#		if nothing in database use current mean outdoor temperature
				#		if value in database is less than seven days old use approximate calculation, using what data available and substituting where not
				#		if value in database is older than seven days, load it
				if weatherAPI_new_day == 1:
					db_weatherAPI=MySQLdb.connect(host=config['db_host'],user=config['db_usr'],passwd=config['db_psw'],db=config['db_db'])
					c=db_weatherAPI.cursor()										# prepare the mysql query to pull the previous days Exponentially weighted running mean from the database
					select = """SELECT data_values,data_datetime FROM %s WHERE data_sensor_id LIKE '%s' AND data_datetime >= CURDATE() - INTERVAL 1 DAY AND data_datetime < CURDATE() ORDER BY data_datetime DESC LIMIT 1"""
					select = select % (config['db_tbl'],"n" + node_id + "s" + weatherAPI_running_mean_id)
					z = 0
					zcount = 0
					while z == 0:											# try upto 10 time to pull the running mean data from the database
						if zcount == 10:									# if database query fails ten times, log an error
							error_report(weatherAPIcurrent_name,""," running mean fetch failed 10 times",0)			
							running_mean_valid = False
							running_mean_retry = True
							z = 1
						if (db_weatherAPI) and (zcount < 10):					# try and query the database for the running mean data
							try:
								c.execute(select)
							except Exception as e:
								error_report(weatherAPIcurrent_name,e," running mean fetch failed",0)				
								zcount += 1
							else:
								if c.rowcount:
									weatherAPI_mysql_running_mean_result, = c.fetchall()
									prev_running_mean_value = json.loads(weatherAPI_mysql_running_mean_result[0])['data'][0] # grab the current exponentially weighted running mean outdoor temperature
									prev_running_mean_days = json.loads(weatherAPI_mysql_running_mean_result[0])['data'][1] # grab the current exponentially weighted running mean outdoor temperature days count
									prev_running_mean_confidence = json.loads(weatherAPI_mysql_running_mean_result[0])['data'][3] # grab the current exponentially weighted running mean outdoor temperature confidence
								else:
									error_report(weatherAPIcurrent_name,""," no running mean outdoor temperature data found in database",0)				
								z = 1
						time.sleep(2)
					db_weatherAPI.close()


					if running_mean_valid == True:
						if prev_running_mean_days == 0:
							running_mean_value = today_mean_temp_value/today_mean_temp_count
							running_mean_confidence = 0.0
							running_calc_used = 1
						elif prev_running_mean_days <= 7:
							db_weatherAPI=MySQLdb.connect(host=config['db_host'],user=config['db_usr'],passwd=config['db_psw'],db=config['db_db'])
							c=db_weatherAPI.cursor()										# loop through pulling the previous days mean outdoor temperatures from the database
							i = 0
							alpha = 1.2
							prev_mean_value = 0
							prev_mean_confidence = 0.0
							while i < 7:
								select = """SELECT data_values,data_datetime FROM %s WHERE data_sensor_id LIKE '%s' AND data_datetime >= CURDATE() - INTERVAL %i DAY AND data_datetime < CURDATE() - INTERVAL %i DAY  ORDER BY data_datetime DESC LIMIT 1"""
								select = select % (config['db_tbl'],"n" + node_id + "s" + weatherAPI_mean_id,i+1,i+0)
								z = 0
								zcount = 0
								while z == 0:											# try upto 10 time to pull the currently selected days mean outdoor temperature data from the database
									if zcount == 10:									# if database query fails ten times, log an error
										running_mean_valid = False
										running_mean_retry = True
										error_report(weatherAPIcurrent_name,""," day " + i + " mean outdoor temperature fetch failed 10 times",0)			
										z = 1
									if (db_weatherAPI) and (zcount < 10):					# try and query the database for the selected days mean outdoor temperature data
										try:
											c.execute(select)
										except Exception as e:
											error_report(weatherAPIcurrent_name,e," day " + str(i) + " mean outdoor temperature fetch failed",0)				
											zcount += 1
										else:
											if i < 3:
												alpha = alpha - 0.2
											else:
												alpha = alpha - 0.1
											if c.rowcount:
												weatherAPI_mysql_daily_mean_result, = c.fetchall()
												prev_mean_value = (json.loads(weatherAPI_mysql_daily_mean_result[0])['data'][0]/json.loads(weatherAPI_mysql_daily_mean_result[0])['data'][1])
												prev_mean_confidence = (json.loads(weatherAPI_mysql_daily_mean_result[0])['data'][1] / 96)
											else:
												error_report(weatherAPIcurrent_name,""," day " + str(i) +  " no running mean outdoor temperature data found in database",0)				
											running_mean_value = running_mean_value + (prev_mean_value*alpha)
											running_mean_confidence = running_mean_confidence
											z = 1
									time.sleep(2)
									i += 1
							db_weatherAPI.close()
							running_mean_value = running_mean_value/3.8
							running_mean_confidence = running_mean_confidence/3.8
							running_calc_used = 2
						else:
							running_mean_value = ((1-0.8)*prev_running_mean_value) + (0.8*(prev_day_mean_temp_value/prev_day_mean_temp_count))
							running_mean_confidence = ((1-0.8)*prev_running_mean_confidence) + (0.8*(prev_day_mean_temp_count / 96))
							running_calc_used = 3
					
					if running_mean_valid == True:
						dataR["data"] = [running_mean_value]
						dataR["data"].append(prev_running_mean_days + 1)
						dataR["data"].append(running_calc_used)
						dataR["data"].append(running_mean_confidence)				# confidence value
						dataR["time"] = str(datetime.datetime.now().isoformat())	
						if print_queue_data == True:
							print("weatherAPIcurrent " + str(json.dumps(dataR)))
						q.put(json.dumps(dataR))									# load the data into the data queue
					else:
						error_report(weatherAPIcurrent_name,""," running mean not valid, therefore not queueing any data",0)				

					if running_mean_retry == False:
						# reset the last run variable
						weatherAPI_prev_run = datetime.datetime.now().strftime("%w")
			
			else:
				error_report(weatherAPIcurrent_name,"","incorrect data recived",0)
		
		time.sleep(int(weatherAPIcurrent_freq))						# sleep for the required sample time


#########################################################################################################################
#																														#
#	Occupant Temperature thread - calculates the adaptive thermal comfort max, min and mean indoor occupant temperature,#
# 	as well as the current value																						#
#																														#
#########################################################################################################################

def generate_OccT():	
	# this module should be run on a node that is inside a room for which Occupant Temperature values are to be calculated
	# mean outdoor temp should be calculatd by a node with an outdoor temp sensor.
	# this module  should be told which outdoor temperature mean to use

	#Occt_O_id Occupant temperature deets; Occt_simple, Occt_full, tComf_class1, tComfLower_class1, tComfUpper_class1, tComfLower_class2, tComfUpper_class2, tComfLower_class3, tComfUpper_class3

	data = {}
	data["id"] = "n" + node_id + "s" + OccT_id

	#	generate Occupant temperature deets

	while True:
		running_mean_valid = True

		if print_positions == True:
			print("OccT at b1")
		b1.wait()												# wait until every module is ready to start
		if print_positions == True:
			print("OccT at b3")
		b3.wait()												# wait until every module is ready to start and sample period has begun
		if print_positions == True:
			print("OccT at b4 temp")
		b4t.wait()												# wait until the PMV temperature sample has been taken
		if print_positions == True:
			print("OccT at b4 rest")
		b4r.wait()												# wait until the rest of the PMV samples have been taken
		if print_positions == True:
			print("OccT at calculation")

		db_OccT=MySQLdb.connect(host=config['db_host'],user=config['db_usr'],passwd=config['db_psw'],db=config['db_db'])
		c=db_OccT.cursor()										# prepare the mysql query to pull the running mean data from the database
		select = """SELECT data_values,data_datetime FROM %s WHERE data_sensor_id LIKE '%s' ORDER BY data_datetime DESC LIMIT 1"""
		select = select % (config['db_tbl'],config['Running_mean_full_id'])
		z = 0
		zcount = 0
		running_mean_temp = 0.0
		while z == 0:											# try upto 10 time to pull the previous running mean from the database
			if zcount == 10:									# if database query fails ten times, don't generate Occupant temperature, and log an error
				error_report(OccT_name,""," Running mean data fetch failed 10 times",0)		
				running_mean_valid = False	
				z = 1
			if (db_OccT) and (zcount < 10):					# try and query the database for the previous running mean
				try:
					c.execute(select)
				except Exception as e:
					error_report(OccT_name,e," Running mean data fetch failed",0)				
					zcount += 1
				else:
					if c.rowcount:
						Occt_mysql_result, = c.fetchall()
						running_mean_temp = json.loads(Occt_mysql_result[0])['data'][0] # grab the running mean temperature
						Tcomf_value_1 = OCCT.comfAdaptiveComfortEN15251(ta = PMV_ta_value, tr = PMV_tr_value, runningMean = running_mean_temp, vel = PMV_as_value, comfortClass = 1)
						Tcomf_value_2 = OCCT.comfAdaptiveComfortEN15251(ta = PMV_ta_value, tr = PMV_tr_value, runningMean = running_mean_temp, vel = PMV_as_value, comfortClass = 2)
						Tcomf_value_3 = OCCT.comfAdaptiveComfortEN15251(ta = PMV_ta_value, tr = PMV_tr_value, runningMean = running_mean_temp, vel = PMV_as_value, comfortClass = 3)
					else:
						error_report(OccT_name,""," incorrect data recived",0)
						running_mean_valid = False
					z = 1
			time.sleep(2)
		db_OccT.close()

		OccT_value_simple = ( 0.5 * PMV_ta_value ) + ( 0.5 * PMV_tr_value )
		OccT_value_full = ( ( PMV_ta_value * math.sqrt(10 * PMV_as_value) ) + PMV_tr_value ) / ( 1 + math.sqrt(10 * PMV_as_value) )

		if running_mean_valid == True:
			data["data"] = [OccT_value_full]							# prepare the date to be loaded into the queue
			data["data"].append(OccT_value_simple)
			data["data"].append(Tcomf_value_1[0])
			data["data"].append(Tcomf_value_1[2])
			data["data"].append(Tcomf_value_1[3])
			data["data"].append(Tcomf_value_2[0])
			data["data"].append(Tcomf_value_2[2])
			data["data"].append(Tcomf_value_2[3])
			data["data"].append(Tcomf_value_3[0])
			data["data"].append(Tcomf_value_3[2])
			data["data"].append(Tcomf_value_3[3])
			data["time"] = str(datetime.datetime.now().isoformat())	
			if print_positions == True:
				print("Occupant Temperature calculation done")
			if print_queue_data == True:
				print("Occt " + str(json.dumps(data)))
			q.put(json.dumps(data))									# load the data into the data queue

	

#########################################################################################################################
#																														#
#	Buttons - writes the appropriate value when a button is pressed														#
#																														#
#########################################################################################################################
if module_buttons == True:
	def button_handler(pin):
		time.sleep(.01)    # Wait a while for the pin to settle (debounce)
		
		# handle the press of a button. assign the appropriate value and turn the light on for a bit to show it has been pressed
		if pin == button_pin1:
			but_res = button_pin1_value
			GPIO.output(16,GPIO.HIGH)
		elif pin == button_pin2:
			but_res = button_pin2_value
			GPIO.output(16,GPIO.HIGH)
		elif pin == button_pin3:
			but_res = button_pin3_value
			GPIO.output(16,GPIO.HIGH)
		elif pin == button_pin4:
			but_res = button_pin4_value
			GPIO.output(16,GPIO.HIGH)
		elif pin == button_pin5:
			but_res = button_pin5_value
			GPIO.output(16,GPIO.HIGH)
		# prepare the data to be loaded into the queue
		data = {}
		data["data"] = [but_res]
		data["time"] = str(datetime.datetime.now().isoformat())
		data["id"] = "n" + node_id + "s" + button_id
		q.put(json.dumps(data))									# load the data into the data queue
		if print_queue_data == True:
			print("buttons data: " + str(json.dumps(data)))
		# turn the led off again after 2 seconds
		time.sleep(2)
		GPIO.output(16,GPIO.LOW)
		
	# setup all the GPIOs and add the event detection
	GPIO.setup(button_pin1, GPIO.IN, pull_up_down = GPIO.PUD_UP)
	GPIO.setup(button_pin2, GPIO.IN, pull_up_down = GPIO.PUD_UP)
	GPIO.setup(button_pin3, GPIO.IN, pull_up_down = GPIO.PUD_UP)
	GPIO.setup(button_pin4, GPIO.IN, pull_up_down = GPIO.PUD_UP)
	GPIO.setup(button_pin5, GPIO.IN, pull_up_down = GPIO.PUD_UP)
	GPIO.add_event_detect(button_pin1, GPIO.RISING,callback=button_handler,bouncetime=300)
	GPIO.add_event_detect(button_pin2, GPIO.RISING,callback=button_handler,bouncetime=300)
	GPIO.add_event_detect(button_pin3, GPIO.RISING,callback=button_handler,bouncetime=300)
	GPIO.add_event_detect(button_pin4, GPIO.RISING,callback=button_handler,bouncetime=300)
	GPIO.add_event_detect(button_pin5, GPIO.RISING,callback=button_handler,bouncetime=300)
	

#########################################################################################################################
#########################################################################################################################
##																													   ##
##	End of thread definitions																						   ##
##																													   ##
#########################################################################################################################
#########################################################################################################################

#########################################################################################################################
#########################################################################################################################
##																													   ##
##	Start of thread initiation																						   ##
##																													   ##
#########################################################################################################################
#########################################################################################################################

# Setup all threads
if run_mysql == True:
	t0=Thread(target=mysql)	
t1=Thread(target=timer)
if module_MDREVP== True:
	t2=Thread(target=read_MDREVP)
if module_SHT75 == True:
	t3=Thread(target=read_SHT75)
if module_MAX31865 == True:
	t4=Thread(target=read_MAX31865)
if module_AMG8833 == True:
	t5=Thread(target=read_AMG8833)
if module_Lepton == True:
	t6=Thread(target=read_Lepton)
if module_SCD30 == True:
	t7=Thread(target=read_SCD30)
if module_SGP30 == True:
	t8=Thread(target=read_SGP30)
if module_BME280 == True:
	t9=Thread(target=read_BME280)
if module_PMS5003 == True:
	t10=Thread(target=read_PMS5003)
if fake_BME280 == True:
	t11=Thread(target=read_fakeBME280)
if module_SEN0232 == True:
	t12=Thread(target=read_SEN0232)
if module_TSL2561 == True:
	t13=Thread(target=read_TSL2561)
if module_PMV == True:
	t14=Thread(target=generate_PMV)
if module_weatherAPIcurrent == True:
	t15=Thread(target=get_weatherAPIcurrent)
if module_OccT == True:
	t16=Thread(target=generate_OccT)
t17=Thread(target=heartBeat)
t18=Thread(target=write_to_log)
		
# Start all threads
if run_mysql == True:		
	t0.start()	
t1.start()
if module_MDREVP == True:
	t2.start()
if module_SHT75 == True:
	t3.start()
if module_MAX31865 == True:
	t4.start()
if module_AMG8833 == True:
	t5.start()
if module_Lepton == True:
	t6.start()
if module_SCD30 == True:
	t7.start()
if module_SGP30 == True:
	t8.start()
if module_BME280 == True:
	t9.start()
if module_PMS5003 == True:
	t10.start()
if fake_BME280 == True:
	t11.start()
if module_SEN0232 == True:
	t12.start()
if module_TSL2561 == True:
	t13.start()
if module_PMV == True:
	t14.start()
if module_weatherAPIcurrent == True:
	t15.start()
if module_OccT == True:
	t16.start()
t17.start()
t18.start()
while kill_switch == False: # sit and wait for the kill switch to be triggered
	time.sleep(10)
else:						# if the kill switch is triggered, make sure the error log queues are empty and then kill the script
	ql.put("r")
	while not ql.empty():
		time.sleep(0.1)
	while not qe.empty():
		time.sleep(0.1)
	os._exit(1)
print("after " + str(kill_switch))
# Join all threads		
if run_mysql == True:
	t0.join()	
t1.join()
if module_MDREVP == True:
	t2.join()
if module_SHT75 == True:
	t3.join()
if module_MAX31865 == True:
	t4.join()
if module_AMG8833 == True:
	t5.join()
if module_Lepton == True:
	t6.join()
if module_SCD30 == True:
	t7.join()
if module_SGP30 == True:
	t8.join()
if module_BME280 == True:
	t9.join()
if module_PMS5003 == True:
	t10.join()
if fake_BME280 == True:
	t11.join()
if module_SEN0232 == True:
	t12.join()
if module_TSL2561 == True:
	t13.join()
if module_PMV == True:
	t14.join()	
if module_weatherAPIcurrent == True:
	t15.join()
if module_OccT == True:
	t16.join()
t17.join()
t18.join()

#########################################################################################################################
#########################################################################################################################
##																													   ##
##	End of thread initiation																						   ##
##																													   ##
#########################################################################################################################
#########################################################################################################################

GPIO.cleanup()
f.close() 
print("The End")
