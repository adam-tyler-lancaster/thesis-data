#!/usr/bin/python3
#########################################################################################################################
#																														#
#	SCD30 I2C Driver																									#
#	This driver utilises the Adafruit busio and I2C drivers to communicate with the Sensiron SDC30 development board	# 																						#
#																														#
#	Version history																										#
#	0.1 - 09 OCT 18 	-	Initial named working version.																#
#	0.1.1 - 10 OCT 2018 -	cleaned up returning errors on read, to allow TCSR script to act on them					#
#	0.1.2 - 08 NOV 2018 -	Added a debug feature to write raw data to a log file										#
#																														#
#	Known issues:																										#
#																														#
#	Work to do:																											#
#	-	More Comments required																							#
#	-	Implement force recalibration, set temp offset, set altitude compensation functions								#
#																														#
#########################################################################################################################

from micropython import const
import time
import datetime
import struct
import numpy as np

_SDC30_ADDRESS = const(0x61) # Address of the SDC30 

# Registers for the various functions
_SDC30_REGISTER_CONTINUOUS_MEASUREMENT = const(0x0010)
_SDC30_REGISTER_STOP_PERIODIC_MEASUREMENT = const(0x0104)
_SDC30_REGISTER_SET_MEASUREMENT_INTERVAL = const(0x4600)
_SDC30_REGISTER_GET_DATA_READY = const(0x0202)
_SDC30_REGISTER_READ_MEASUREMENT = const(0x0300)
_SDC30_REGISTER_AUTOMATIC_SELF_CALIBRATION = const(0x5306)
_SDC30_REGISTER_SET_FORCED_RECALIBRATION_FACTOR = const(0x5204)
_SDC30_REGISTER_SET_TEMPERATURE_OFFSET = const(0x5403)
_SDC30_REGISTER_SET_ALTITUDE_COMPENSATION = const(0x5102)

# CRC calculation constants
_SDC30_CRC8_POLYNOMIAL   = const(0x31)
_SDC30_CRC8_INIT         = const(0xFF)

_i2c_max_tries = 10 # maximum number of times to try the i2c calls

# Debugging features
log_data = False # enables or disables logging of raw sensor data to a file

#setup the log file
if log_data == True:
	log_file = '/home/pi/Documents/TCSR/SDC30_data.txt'
	f = open(log_file,"a+")
	f.write("SDC30 @ {0} restart\n".format(str(datetime.datetime.now().isoformat())))
	f.flush()

class AT_SDC30:
	#def __init__(self):
	# function to begin measuring and set pressure offset (in mbar). To update pressure offset this function should just be called again.
	# min is 700mbar, max is 1200mbar. 0 deactivates pressure compensation
	def beginMeasuring(self, pressureOffset=0):
		if (pressureOffset < 700) or (pressureOffset > 1200) or (pressureOffset == 0):
			self._write_register_byte(_SDC30_REGISTER_CONTINUOUS_MEASUREMENT,int(pressureOffset))
    	
	# function to stop continuous measurement
	def stopMeasuring(self):
		self._write_register_byte(_SDC30_REGISTER_STOP_PERIODIC_MEASUREMENT,False)
	
 	# function to set measurement interval, default is 2 seconds. minimum is 2, maximum is 1800 seconds
	def setMeasurementInterval(self, interval=2):
		if (interval < 2) or (interval > 1800):
			self._write_register_byte(_SDC30_REGISTER_SET_MEASUREMENT_INTERVAL,interval)

	# function to enable or disable automatic self calibration 
	def setAutoSelfCalibration(self, enable):
		if enable == True:
			self._write_register_byte(_SDC30_REGISTER_AUTOMATIC_SELF_CALIBRATION,1) #Activate continuous ASC
		else:
			self._write_register_byte(_SDC30_REGISTER_AUTOMATIC_SELF_CALIBRATION,0) #Deactivate continuous ASC

	# function to check if data is available to read. Is called by the readMeasurement function. returns True or False
	def dataAvailable(self):
		response = self._read_register(_SDC30_REGISTER_GET_DATA_READY,2)[1]
		if response == 1:
			return (True)
		else:
			return (False)
	
	# Function to read measurements. Checks if data is available first, and returns False if not. Checks CRC's of data received, and returns False 
	# if not correct. if data is available and CRC's are correct, it returns a list of the three results, CO2 first, Temp second, Humidity third.
	def readMeasurement(self):
		if self.dataAvailable() == False:
			raise Exception('Data not available')
		else:
			try:
				response = self._read_register(_SDC30_REGISTER_READ_MEASUREMENT,18)
				i = 0
				j = 0
				read = {}
				while i < 18:
					if (self._generate_crc([response[i], response[i+1]]) != response[i+2]) and  (self._generate_crc([response[i+3], response[i+4]]) != response[i+5]):
						raise Exception('CRC check fail')
						i = 20
						break
					else:
						list = [response[i],response[i+1],response[i+3],response[i+4]]
						aa = bytearray(list)
						bb = struct.unpack('>f',aa)
						read[j] = bb[0]
						#print(bb[0])
						j += 1
						i += 6
				if log_data == True:
					f.write("{0},{1},{2},{3}\n".format(str(datetime.datetime.now().isoformat()),read[0],read[1],read[2]))
					f.flush()
				return (np.array([read[0],read[1],read[2]]))
			except:
				pass
			
class AT_SDC30_I2C(AT_SDC30):
	# Initialise driver for SDC30 connected over I2C
	def __init__(self, i2c, address=_SDC30_ADDRESS):
		import adafruit_bus_device.i2c_device as i2c_device
		self._i2c = i2c_device.I2CDevice(i2c, address)
		super().__init__()

	# low level function to actually read the required registers. Includes a try loop that has a specified number of goes at doing the i2c calls, and returns False if it still fails
	def _read_register(self, register, length):
		with self._i2c as i2c:
			cr, fr= divmod(register, 0x100)
			x = 0
			result = bytearray(length)
			while x <= _i2c_max_tries:
				try:
					i2c.write(bytes([cr,fr]))
					i2c.readinto(result)
					if log_data == True:
						f.write("{0},{1}\n".format(str(datetime.datetime.now().isoformat()),result))
						f.flush()
					x = _i2c_max_tries + 2
				except:
					time.sleep(0.5)
					x += 1
					pass
			#print("$%02X => %s" % (register, [hex(i) for i in result]))
			if x == _i2c_max_tries + 1:
				return (False)
			else:
				return result
	
	# low level function to write to the required registers. Includes a try loop that has ten goes at doing the i2c calls, and returns False if it still fails
	def _write_register_byte(self, register, value):
		with self._i2c as i2c:
			cr, fr= divmod(register, 0x100)
			if value != False:
				arr = [value >> 8, value & 0xFF]
				crc = self._generate_crc(arr)
				cv, fv= divmod(value, 0x100)
				packet = [cr,fr,cv,fv,crc]
			else:
				packet = [cr,fr]
			#print(bytes(packet))
			x = 0
			while x <= _i2c_max_tries:
				try:
					i2c.write(bytes(packet))
					x = _i2c_max_tries + 2
				except:
					time.sleep(0.5)
					x += 1
					pass
			if x == _i2c_max_tries + 1:
				return (False)
	
	# low level function that returns a CRC for the supplied bytes
	def _generate_crc(self, data):
		"""8-bit CRC algorithm for checking data"""
		crc = _SDC30_CRC8_INIT
		# calculates 8-Bit checksum with given polynomial
		for byte in data:
			crc ^= byte
			for _ in range(8):
				if crc & 0x80:
					crc = (crc << 1) ^ _SDC30_CRC8_POLYNOMIAL
				else:
					crc <<= 1
		return crc & 0xFF

