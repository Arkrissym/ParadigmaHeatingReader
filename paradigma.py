#from distutils.log import debug
#from re import I
import socket
import sys
from itertools import takewhile
from datetime import datetime
from influxdb import InfluxDBClient
import time
import argparse



class Debugger(object):

    def __init__(self, verbose):
        self.verbose = verbose


    def debugData(self, data, i):
        if self.verbose > 0:
            filtered_characters = ' '.join(list(chr(s) if chr(s).isprintable() else '.' for s in data))
            d = ' '.join(["{:02x}".format(x) for x in data])
            offset = 0
            print(f'{i}:')
            while offset * 3 < len(d):
                print(f'{d[offset*3:(offset*3)+24]} {d[offset*3+24:offset*3+48]}\t{filtered_characters[offset*2:(offset*2) + 16]} {filtered_characters[offset*2 + 16 : offset*2 + 32]}')
                offset = offset + 16



CONNECT_MSG = bytes.fromhex('08 00 00 00 00 01 31 32 33 34') 


def extractSolarInfo(data):
    totalKwhOffset = data.find(b'\x12\x2c\x01\xbc\x00')
    totalKwh = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[totalKwhOffset+5:]))]).strip()
    if totalKwh[-3:] != 'kWh':
        return None, None, None

    currentPanelTempOffset = data.find(b'\x12\x2c\x01\x56\x00')
    currentPanelTemp = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[currentPanelTempOffset+5:]))]).strip()
    if currentPanelTemp[-2:] != '\xb0\x43':
        return None, None, None
    

    maxPanelTempOffset = data.find(b'\x12\x2c\x01\x78\x00')
    maxPanelTemp = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[maxPanelTempOffset+5:]))])
    if maxPanelTemp[-2:] != '\xb0\x43':
        return None, None, None

    totalKwh = int(totalKwh[:-3].strip())
    currentPanelTemp = float(currentPanelTemp[:-2].strip().replace(',','.'))
    maxPanelTemp = float(maxPanelTemp[:-2].strip().replace(',','.'))

    print(f'total kWh: {totalKwh}')
    print(f'panel Temp: {currentPanelTemp}')
    print(f'max Panel Temp: {maxPanelTemp}')

    return (totalKwh, currentPanelTemp, maxPanelTemp)


def extractWarmwasserInfo(data):
    warmWasserTempOffset = data.find(b'\x12\x2c\x01\x34\x00')
    warmWasserTemp = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[warmWasserTempOffset+5:]))]).strip()
    if warmWasserTemp[-2:] != '\xb0\x43':
        return None
 
    warmWasserTemp = float(warmWasserTemp[:-2].strip().replace(',','.'))
    print(f'Warmwasser Temp: {warmWasserTemp}')
    return warmWasserTemp



class HeatingConnector(object):
    def __init__(self, ip, port, influxhost, influxport, verbose):
        self.controller = (ip, port)
        self.influxhost = influxhost
        self.influxport = influxport
        self.token = None
        self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.warmWasserTemp = None
        self.maxTemp = None
        self.panelTemp = None
        self.totalKwh = None
        self.debugger = Debugger(verbose)

    def recv(self):
        data, _address = self.s.recvfrom(2048)
        self.token = data[:5]
        return data

    def send(self, data):
        message = self.token + data if self.token else data
        #print(message.hex())
        self.s.sendto(message, self.controller)

    
    def connect(self):
        self.s.sendto(CONNECT_MSG, self.controller)
        data = self.recv()
        assert len(data) == 7
        #assert data == bytes.fromhex('08 00 00 00 00 01 01')
        
        self.debugger.debugData(data, "CONNECT_1")

        data = self.recv()
        assert len(data) == 6
        self.debugger.debugData(data, "CONNECT_2")

        self.send(bytes.fromhex('f0 01 16 00 01 14 00 02'))
        data = self.recv()
        assert len(data) == 6
        self.debugger.debugData(data, "CONNECT_3")

        self.send(bytes.fromhex('f3 00 00 03 03 04 0e ff ff ff ff 74 56 01 00 56 72 41 b5'))


        data = self.recv()
        assert len(data) == 10
        assert data == bytes.fromhex('01 f7 00 f7 00 f7 03 00 00 00')
        self.debugger.debugData(data, "CONNECT_4")


        # is this now the last of the initialization of the connection?
        #message = data[:6] + bytes.fromhex('00 01 00 00 00')
        #self.s.sendto(message, self.controller)
        self.send(bytes.fromhex('f7 00 01 00 00 00'))

        data = self.recv()
        self.debugger.debugData(data, "CONNECT_5")
        #assert data[5:8] == bytes.fromhex('80 0a 00')


    def mainMenu(self):
        self.send(bytes.fromhex('00 16 00 ff 13 00 dd 00 00 00 83 1f 00 00'))
        # NOTE this additional message with slightly changed content IS NEEDED, otherwise it's not going to switch!
        data = self.recv()
        self.debugger.debugData(data, "MAIN_MENU_1")
        self.send(bytes.fromhex('00 02 00 1e ff ff ff ff 00 00 ff 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "MAIN_MENU_2")
        

    def warmwasser(self):
        self.send(bytes.fromhex('00 02 00 ff 7c 00 4a 00 00 00 ff 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "WARMWASSER_1")
        self.send(bytes.fromhex('00 14 00 20 ff ff ff ff 00 00 61 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "WARMWASSER_2")

        self.warmWasserTemp = extractWarmwasserInfo(data)

        counter = 0
        while True and counter < 4:
            self.send(bytes.fromhex('00 0b 00 ff ff ff ff ff 00 00 d0 1f 00 00'))
            data = self.recv()
            self.debugger.debugData(data, "WARMWASSER_LEAVING_1")
            if len(data) == 8:
                break
            counter += 1

        self.send(bytes.fromhex('00 02 00 ff 1c 00 db 00 00 00 ff 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "WARMWASSER_LEAVING_2")
        if len(data) == 55:
            self.send(bytes.fromhex('00 0b 00 09 1e 00 db 00 00 00 d0 1f 00 00'))
            data = self.recv()
            self.debugger.debugData(data, "WARMWASSER_LEAVING_3")


    def solar(self):
        self.send(bytes.fromhex('00 14 00 ff c8 00 4a 00 00 00 61 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "SOLAR_1")
        self.send(bytes.fromhex('00 02 00 1f c8 00 4a 00 00 00 ff 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "SOLAR_2")
        (self.totalKwh, self.panelTemp, self.maxTemp) = extractSolarInfo(data)

        counter = 0
        while True and counter < 4:
            self.send(bytes.fromhex('00 55 00 1f ff ff ff ff 00 00 62 1e 00 00'))
            data = self.recv()
            self.debugger.debugData(data, "SOLAR_LEAVING_1")
            if len(data) == 8:
                break
            counter += 1

        self.send(bytes.fromhex('00 02 00 ff 13 00 dd 00 00 00 ff 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "SOLAR_LEAVING_2")
        if len(data) == 125:
            self.send(bytes.fromhex('00 13 00 01 13 00 dd 00 00 00 8a 1f 00 00'))
            data = self.recv()
            self.debugger.debugData(data, "SOLAR_LEAVING_3")


    def closeAndSubmit(self):
        self.s.close()

        if self.influxhost is not None:
            client = InfluxDBClient(host=self.influxhost, port=self.influxport)
            data = []
            if self.maxTemp is not None:
                data.append(
                    {
                        "measurement": "\xb0\x43",
                        "tags": {
                            "entity_id": "Solarpanel Max",
                        },
                        "fields": {
                            "value": self.maxTemp,
                        },
                    }
                )
            if self.totalKwh is not None:
                data.append(
                    {
                        "measurement": "kWh",
                        "tags": {
                            "entity_id": "Solarpanel Total kwh",
                        },
                        "fields": {
                            "value": self.totalKwh,
                        },
                    }
                )
            if self.warmWasserTemp is not None:
                data.append(
                    {
                        "measurement": "\xb0\x43",
                        "tags": {
                            "entity_id": "Warmwassertank Temperature",
                        },
                        "fields": {
                            "value": self.warmWasserTemp,
                        },
                    }
                )
            if self.panelTemp is not None:
                data.append(
                    {
                        "measurement": "\xb0\x43",
                        "tags": {
                            "entity_id": "Solarpanel Temperature",
                        },
                        "fields": {
                            "value": self.panelTemp,
                        }
                    }
                )
            result = client.write_points(data, database='smarthome',
                                        time_precision='ms', batch_size=10000,
                                        protocol='json')
            print(f'Result of Writing to influxdb: {result}')
        else:
            print(f'not writing to influx')




def main(args):
    ip = args.host
    port = args.port

    while True:
        try:
            hc = HeatingConnector(ip, port, args.influxhost, args.influxport, args.verbosity)
            hc.connect()
            hc.mainMenu()
            hc.warmwasser()
            hc.solar()
            hc.closeAndSubmit()
        except:
            print(f'Error happened while requesting')
        time.sleep(600)

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbosity", help="output debug information of the messages received", default=0, action="count")
    parser.add_argument("-t", "--host", help="IP address of the heating control")
    parser.add_argument("-p", "--port",
                        type=int,
                        help="Port the Heating Controller is listening on (default: 3477)",
                        default=3477)
    parser.add_argument("--influxhost", help="Host of the InfluxDB the data to send to", default=None)
    parser.add_argument("--influxport", help="the port of the InfluxDB to send the data to", default=8086, type=int)


    args = parser.parse_args()

    if args.host is None:
        print('you need to at least provide a parameter for the ip of the heating controller')
    else:
        main(args)