import socket
import sys
from itertools import takewhile
from datetime import datetime
import time
import argparse
import paho.mqtt.client as mqtt


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
class HeatingConnector(object):
    def __init__(self, ip, port, verbose):
        self.controller = (ip, port)
        self.token = None
        self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.s.settimeout(10)
        self.data = {}
        self.debugger = Debugger(verbose)


    def recv(self):
        data, _address = self.s.recvfrom(2048)
        self.token = data[:5]
        return data


    def send(self, data):
        message = self.token + data if self.token else data
        #print(message.hex())
        self.s.sendto(message, self.controller)


    def extractMainMenuInfo(self, data):
        timeOffset = data.find(b'\x12\x2c\x01\x56\x00')
        self.data["time"] = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[timeOffset+5:]))]).strip()

        tempInOffset = data.find(b'\x12\x2c\x01\x94\x00')
        tempIn = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[tempInOffset+5:]))]).strip()
        if tempIn[-2:] == '\xb0\x43':
            self.data["temp-indoor"] = float(tempIn[:-2].strip().replace(',','.'))

        tempOutOffset = data.find(b'\x12\x2c\x01\xcb\x00')
        tempOut = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[tempOutOffset+5:]))]).strip()
        if tempOut[-2:] == '\xb0\x43':
            self.data["temp-outdoor"] = float(tempOut[:-2].strip().replace(',','.'))


    def extractSolarInfo(self, data):
        totalKwhOffset = data.find(b'\x12\x2c\x01\xbc\x00')
        totalKwh = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[totalKwhOffset+5:]))]).strip()
        if totalKwh[-3:] == 'kWh':
            self.data["total-kwh"] = int(totalKwh[:-3].strip())

        todayKwhOffset = data.find(b'\x12\x2c\x01\x9a\x00')
        todayKwh = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[todayKwhOffset+5:]))]).strip()
        if todayKwh[-3:] == 'kWh':
            self.data["today-kwh"] = int(todayKwh[:-3].strip())

        currentPanelTempOffset = data.find(b'\x12\x2c\x01\x56\x00')
        currentPanelTemp = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[currentPanelTempOffset+5:]))]).strip()
        if currentPanelTemp[-2:] == '\xb0\x43':
            self.data["current-panel-temp"] = float(currentPanelTemp[:-2].strip().replace(',','.'))

        maxPanelTempOffset = data.find(b'\x12\x2c\x01\x78\x00')
        maxPanelTemp = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[maxPanelTempOffset+5:]))])
        if maxPanelTemp[-2:] == '\xb0\x43':
            self.data["max-panel-temp"] = float(maxPanelTemp[:-2].strip().replace(',','.'))


    def extractWarmwasserInfo(self, data):
        warmWasserTempOffset = data.find(b'\x12\x2c\x01\x34\x00')
        warmWasserTemp = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[warmWasserTempOffset+5:]))]).strip()
        if warmWasserTemp[-2:] == '\xb0\x43':
            self.data["water-temp"] = float(warmWasserTemp[:-2].strip().replace(',','.'))

        warmWasserTargetTempOffset = data.find(b'\x12\x2c\x01\x56\x00')
        if warmWasserTargetTempOffset > -1:
            warmWasserTargetTemp = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[warmWasserTargetTempOffset+5:]))]).strip()
            if warmWasserTargetTemp[-2:] == '\xb0\x43':
                self.data["water-target-temp"] = float(warmWasserTargetTemp[:-2].strip().replace(',','.'))


    def extractKesselInfo(self, data):
#        kesselInOffset = data.find(b'\x12\x4b\x00\xa3\x00')
#        kesselIn = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[kesselInOffset+5:]))]).strip()
#        if kesselIn[-2:] != '\xb0\x43':
#            return None
#
#        kesselIn = float(kesselIn[:-2].strip().replace(',','.'))
#        print(f'KesselIn: {kesselIn}')
#
#        kesselOutOffset = data.find(b'\x12\x4b\x00\x4c\x00')
#        kesselOut = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[kesselOutOffset+5:]))]).strip()
#        if kesselOut[-2:] != '\xb0\x43':
#            return None
#
#        kesselOut = float(kesselOut[:-2].strip().replace(',','.'))
#        print(f'KesselOut: {kesselOut}')

        kesselStateOffset = data.find(b'\x12\xf0\x00\x65\x00')
        kesselState = ''.join([chr(x) for x in list(takewhile(lambda x: x != 0, data[kesselStateOffset+5:]))]).strip()
        self.data["boiler-state"] = kesselState


    def extractKesselRunInfo(self, data):
        kesselStartOffset = data.find(b'\x12\x2c\x01\x85\x00')
        kesselStart = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[kesselStartOffset+5:]))]).strip()
        self.data["boiler-start-count"] = int(kesselStart.strip())

        kesselHoursOffset = data.find(b'\x12\x2c\x01\x4c\x00')
        kesselHours = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[kesselHoursOffset+5:]))]).strip()
        if kesselHours[-2:] == ' h':
            self.data["boiler-runtime"] = int(kesselHours[:-1].strip())


    def connect(self):
        self.s.sendto(CONNECT_MSG, self.controller)
        data = self.recv()
        assert len(data) == 7
        #assert data == bytes.fromhex('08 00 00 00 00 01 01')
        # check if device is in use
        assert data != bytes.fromhex('08 00 00 00 00 01 fe')

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

        if len(data) > 16: # temperatures and time are sometimes shown early
            self.extractMainMenuInfo(data)


    def mainMenu(self):
        self.send(bytes.fromhex('00 16 00 ff 13 00 dd 00 00 00 83 1f 00 00'))
        # NOTE this additional message with slightly changed content IS NEEDED, otherwise it's not going to switch!
        data = self.recv()
        self.debugger.debugData(data, "MAIN_MENU_1")

        if len(data) > 16:
            self.extractMainMenuInfo(data)

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

        self.extractWarmwasserInfo(data)

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
        self.extractSolarInfo(data)

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


    def kessel(self):
        self.send(bytes.fromhex('00 02 00 ff ff ff ff ff 00 00 ff 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "KESSEL_1")
        self.send(bytes.fromhex('00 14 00 25 80 00 c0 00 00 00 61 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "KESSEL_2")
        self.extractKesselInfo(data)

        self.send(bytes.fromhex('00 02 00 ff ff ff ff ff 00 00 ff 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "KESSEL_INFO_1")
        self.send(bytes.fromhex('00 02 00 11 29 01 e6 00 00 00 ff 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "KESSEL_INFO_2")

        self.send(bytes.fromhex('00 02 ff ff ff ff ff 00 00 ff 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "KESSEL_INFO_3")
        self.send(bytes.fromhex('00 07 00 16 27 01 e2 00 00 00 ec 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "KESSEL_INFO_4")

        self.extractKesselRunInfo(data)

        self.send(bytes.fromhex('00 02 ff ff ff ff ff 00 00 ff 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "KESSEL_LEAVING_1")
        self.send(bytes.fromhex('00 02 00 08 17 01 0f 00 00 00 ff 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "KESSEL_LEAVING_2")


    def close(self):
        self.s.close()
        for key, value in self.data.items():
            print(f'{key}: {value}')


    def send_mqtt(self, client):
        for key, value in self.data.items():
            client.publish(f"paradigma/systacomfort/{key}", value)


def main(args):
    def on_disconnect(client, userdata, rc):
        client.reconnect()

    #mqttClient = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttClient = mqtt.Client("paradigma")

    if args.mqttuser is not None:
        mqttClient.username_pw_set(args.mqttuser, args.mqttpass)
    if args.mqtthost is not None:
        mqttClient.connect(args.mqtthost, port=args.mqttport)
        mqttClient.loop_start()

    while True:
        try:
            hc = HeatingConnector(args.host, args.port, args.verbosity)
            hc.connect()
            hc.mainMenu()
            hc.warmwasser()
            hc.solar()
            hc.kessel()
            hc.close()
            if mqttClient.is_connected():
                hc.send_mqtt(mqttClient)
        except Exception as e:
            print(f'Error happened while requesting: {e}')
        time.sleep(60)

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbosity", help="output debug information of the messages received", default=0, action="count")
    parser.add_argument("-t", "--host", help="IP address of the heating control")
    parser.add_argument("-p", "--port",
                        type=int,
                        help="Port the Heating Controller is listening on (default: 3477)",
                        default=3477)
    parser.add_argument("--mqtthost", help="Host of the mqtt broker the data to send to", default=None)
    parser.add_argument("--mqttport", help="the port of the mqtt broker to send the data to", default=1883, type=int)
    parser.add_argument("--mqttuser", help="username for the mqtt broker the data to send to", default=None)
    parser.add_argument("--mqttpass", help="password for the mqtt broker the data to send to", default=None)

    args = parser.parse_args()

    if args.host is None:
        print('you need to at least provide a parameter for the ip of the heating controller')
    else:
        main(args)
