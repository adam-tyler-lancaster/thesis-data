# TCSR- Thermal Comfort Sensing Rig

This project is developing a device to collect Thermal comfort data in an indoor office environment. it is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY. It currently is developed based on a Raspberry Pi, and has the ability to measure the following parameters: 
* Air Temperature - Sensiron SHT75 and SDC30 sensors, Adafruit BME280 sensor
* Air Speed - Modern Devices Rev P anemometer
* Humidity - Sensiron SHT75 and SDC30 sensors, Adafruit BME280 sensor
* Radiant Temperature - Adafruit MAX31865 ADC with a connected black bulb PT100 sensor
* Volatile Organic Compounds (VOC) - Adafruit SGP30 sensor
* CO2 - Sensiron SDC30 sensor
* Air pressure - Adafruit BME280 sensor
* Air particulate levels - Plantower PMS5003 sensor
* Decibel levels - Gravity sen0232 sensor (in development)
* Lux levels - Adafruit TSL2561 (in development)
* Thermal imaging - Flir Lepton and Panasonic AMG8833 cameras

The device can be mains or POE powered, and either connects via Ethernet or WiFi to log recorded data to a MySQL database

## Getting Started

* TCSR.py is the main script
* config.py is the configuration file for TCSR.py
* Modules folder contains modules developed as part of the project 
* Dump folder contains testing files and other bits and bobs that are not required for the main script

## Authors

* **Adam Tyler**

## License - To be completed

Creative Commons - BY-NC-SA - see repository LICENSE file