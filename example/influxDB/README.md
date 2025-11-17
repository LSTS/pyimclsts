# InfluxDB Integration 

So first, you need to access to an InfluxDB server. To perform tests on the behaviour of this system, I set up my own in my system. 

## Installation
To do so, you should follow the installation instructions for [InfluxDB3](https://docs.influxdata.com/influxdb3/core/get-started/)

## Execution 

After you have a server running you can use the `influxDBWrite.py` python script to write all of your logs information into it. 

Beforehand, you need to set up some information either in that file or in your system MACROs. 

URL: This is the gateway to your server (example). When you started your server, it most likely opened and resctrited an exclusive port for data parsing.  

TOKEN: For you to be able to connect to that port for either writing or reading, a TOKEN will be requested of you. The simplest path to get this is to simply run `influxdb3 create token --admin`. This will generate an admin token that you can either as a MACRO or supply directly either in the python script or in an auxiliary file. 

DATABASE: Correcly identify the name of your database. 

To run the script simply type from pyimclsts's root folder: 
`python3 -m example.influxDB.influxDBWrite`

To make things simpler, you can check if your data was correcly uploaded or not by checking InfluxDB web explorer. 

## InfluxDB3 

Start influxDB3 
`influxdb3`

Generate a token and check if it contains the data I sent to it yesterday


Also start the docker with influxdb3 xplorer

