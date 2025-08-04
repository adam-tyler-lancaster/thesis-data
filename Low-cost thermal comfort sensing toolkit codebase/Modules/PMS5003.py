#! coding: utf-8
#########################################################################################################################
#																														#
#	PMS5003 Driver																										#
#	This driver utilises the pyserial library to allow the TCSR script to collect data from the Plantower PMS5003 		#
#	sensor. Hevily based on the PMS7003 reader for raspberry pi	project by dawncold 									#
#	(https://github.com/dawncold/raspberry_pms7003.git), with modifications to allow it to run on python 3				#
#																														#
#	Version history																										#
#	0.1 - 11 NOV 2018 	-	Initial named working version.																#
#																														#
#	Known issues:																										#
#																														#
#	Work to do:																											#
#	-	More Comments required																							#
#																														#
#########################################################################################################################

from __future__ import unicode_literals, print_function, division
import serial

SERIAL_DEVICE = '/dev/serial0'	# serial address
HEAD_FIRST = 0x42	# Expected first byte
HEAD_SECOND = 0x4d	# Expected second byte
DATA_LENGTH = 32	# Expected total number of bytes for read
BODY_LENGTH = DATA_LENGTH - 1 - 1
P_CF_PM10 = 2 # key for data PM1.0 concentration (CF=1, standard material, unit ug/m3)
P_CF_PM25 = 4 # key for data PM2.5 concentration (CF=1, standard material, unit ug/m3)
P_CF_PM100 = 6 # key for data PM10 concentration (CF=1, standard material, unit ug/m3)
P_C_PM10 = 8 # key for data PM1.0 concentration (atmospheric environment, unit ug/m3)
P_C_PM25 = 10 # key for data PM2.5 concentration (atmospheric environment, unit ug/m3)
P_C_PM100 = 12 # key for data PM10 concentration (atmospheric environment, unit ug/m3)
P_C_03 = 14 # key for data quantity of particles greater than 0.3 µm in diameter in 0.1 litres of air
P_C_05 = 16 # key for data quantity of particles greater than 0.5 µm in diameter in 0.1 litres of air 
P_C_10 = 18 # key for data quantity of particles greater than 1.0 µm in diameter in 0.1 litres of air 
P_C_25 = 20 # key for data quantity of particles greater than 2.5 µm in diameter in 0.1 litres of air 
P_C_50 = 22 # key for data quantity of particles greater than 5.0 µm in diameter in 0.1 litres of air 
P_C_100 = 24 # key for data quantity of particles greater than 10 µm in diameter in 0.1 litres of air 

# output formatting
DATA_DESC = [
	(P_CF_PM10, 'CF=1, PM1.0', 'μg/m3'), 
	(P_CF_PM25, 'CF=1, PM2.5', 'μg/m3'),
	(P_CF_PM100, 'CF=1, PM10', 'μg/m3'),
	(P_C_PM10, 'PM1.0', 'μg/m3'),
	(P_C_PM25, 'PM2.5', 'μg/m3'),
	(P_C_PM100, 'PM10', 'μg/m3'),
	(P_C_03, '0.1L, d>0.3μm', ''),
	(P_C_05, '0.1L, d>0.5μm', ''),
	(P_C_10, '0.1L, d>1μm', ''),
	(P_C_25, '0.1L, d>2.5μm', ''),
	(P_C_50, '0.1L, d>5.0μm', ''),
	(P_C_100, '0.1L, d>10μm', ''),
]

# function to grab the data block from the sensor
def get_frame(_serial):
	while True:
		# Read in the first byte, decode it as ISO unicode and check if it's HEAD_FIRST. If not restart while
		b = _serial.read().decode("ISO-8859-1")
		if b != chr(HEAD_FIRST):
			continue
		# Read in the second byte, decode it as ISO unicode and check if it's HEAD_SECOND. If not restart while
		b = _serial.read().decode("ISO-8859-1")
		if b != chr(HEAD_SECOND):
			continue
		# Read in the remaining bytes, decode them as ISO unicode and check if they are the correct length. If not restart while
		body = _serial.read(BODY_LENGTH).decode("ISO-8859-1")
		if len(body) != BODY_LENGTH:
			continue
		# If everything looks good, return the data block
		return body


# function to check the data block length
def get_frame_length(_frame):
	h8 = ord(_frame[0])
	l8 = ord(_frame[1])
	return int(h8 << 8 | l8)


# function to pull version and error code out of the data block
def get_version_and_error_code(_frame):
	return _frame[-4], _frame[-3]


# function to check if checksum is valid
def valid_frame_checksum(_frame):
	checksum = ord(_frame[-2]) << 8 | ord(_frame[-1])
	calculated_checksum = HEAD_FIRST + HEAD_SECOND
	for field in _frame[:-2]:
		calculated_checksum += ord(field)
	return checksum == calculated_checksum


# function to decode the data block
def decode_frame(_frame):
	data = {}
	for item in DATA_DESC:
		start, desc, unit = item
		value = int(ord(_frame[start]) << 8 | ord(_frame[start + 1]))
		data[str(start)] = (desc, value, unit)
	return data

# main function (externally called)
def read_data():
	# setup the serial connection
	ser = serial.Serial(port=SERIAL_DEVICE, baudrate=9600, bytesize=serial.EIGHTBITS, parity=serial.PARITY_NONE,stopbits=serial.STOPBITS_ONE)
    
	# try getting the data block
	try:
		frame = get_frame(ser)
	# if not raise the exception so the calling function can deal with it
	except:
		raise
	# if not an exception
	else:
		# if checksum is not valid, raise an exception
		if not valid_frame_checksum(frame):
			raise ValueError('frame checksum mismatch')
			return
		# decode data block
		data = {'data': decode_frame(frame)}
		# grab the version and any error code, then add it to the output
		version, error_code = get_version_and_error_code(frame)
		data['version'] = version
		data['errcode'] = error_code
		# send back the data
		return data
	finally:
		# close the serial connection
		ser.close()