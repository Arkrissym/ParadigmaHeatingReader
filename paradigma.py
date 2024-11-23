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


def extract_value(data:str, marker:str, unit:str = None, formatter = None):
    offset = data.find(marker)
    if offset < 0:
        return None
    value = "".join([chr(x) for x in list(takewhile(lambda x: x != 0, data[offset+len(marker):]))]).strip()

    if unit is not None:
        if value[-(len(unit)):] != unit:
            value = None
        else:
            value = value[:-(len(unit))].strip()

    if value is not None and formatter is not None:
        value = formatter(value)

    return value

def extract_temperature(data:str, marker:str):
    return extract_value(data, marker, '\xb0\x43', lambda x: x.replace(',', '.'))


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


    def _send(self, data):
        message = self.token + data if self.token else data
        #print(message.hex())
        self.s.sendto(message, self.controller)

    def send(self, data, send_intermediate=False):
        if send_intermediate:
            self._send(bytes.fromhex('00 02 00 ff ff ff ff ff 00 00 ff 1f 00 00'))
            self.debugger.debugData(self.recv(), "INTERMEDIATE")
        self._send(data)


    def extract_main_menu_info(self, data):
        self.data["time"] = extract_value(data, b'\x12\x2c\x01\x56\x00')

        self.data["temp-indoor"] = extract_temperature(data, b'\x12\x2c\x01\x94\x00')
        self.data["temp-outdoor"] = extract_temperature(data, b'\x12\x2c\x01\xcb\x00')


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
            self.extract_main_menu_info(data)


    def main_menu(self):
        self.send(bytes.fromhex('00 16 00 ff 13 00 dd 00 00 00 83 1f 00 00'))
        # NOTE this additional message with slightly changed content IS NEEDED, otherwise it's not going to switch!
        data = self.recv()
        self.debugger.debugData(data, "MAIN_MENU_1")

        if len(data) > 16:
            self.extract_main_menu_info(data)

        self.send(bytes.fromhex('00 02 00 1e ff ff ff ff 00 00 ff 1f 00 00'))
        data = self.recv()
        self.debugger.debugData(data, "MAIN_MENU_2")


    def return_to_main_menu(self):
        self.send(bytes.fromhex('00 02 00 08 17 01 0f 00 00 00 ff 1f 00 00'), True)
        data = self.recv()
        self.debugger.debugData(data, "RETURN_TO_MAIN_MENU")


    def water(self):
        self.send(bytes.fromhex('00 14 00 20 ff ff ff ff 00 00 61 1f 00 00'), True)
        data = self.recv()
        self.debugger.debugData(data, "WARMWASSER")

        self.data["water-temp"] = extract_temperature(data, b'\x12\x2c\x01\x34\x00')
        self.data["water-target-temp"] = extract_temperature(data, b'\x12\x2c\x01\x56\x00')

        self.return_to_main_menu()


    def solar(self):
        self.send(bytes.fromhex('00 02 00 1f c8 00 4a 00 00 00 ff 1f 00 00'), True)
        data = self.recv()
        self.debugger.debugData(data, "SOLAR")

        self.data["total-kwh"] = extract_value(data, b'\x12\x2c\x01\xbc\x00', "kWh")
        self.data["today-kwh"] = extract_value(data, b'\x12\x2c\x01\x9a\x00', "kWh")
        self.data["current-panel-temp"] = extract_temperature(data, b'\x12\x2c\x01\x56\x00')
        self.data["max-panel-temp"] = extract_temperature(data, b'\x12\x2c\x01\x78\x00')

        self.return_to_main_menu()


    def boiler(self):
        self.send(bytes.fromhex('00 14 00 25 80 00 c0 00 00 00 61 1f 00 00'), True)
        data = self.recv()
        self.debugger.debugData(data, "KESSEL")

        self.data["boiler-in-temp"] = extract_temperature(data, b'\x12\x4b\x00\xa3\x00')
        self.data["boiler-out-temp"] = extract_temperature(data, b'\x12\x4b\x00\x4c\x00')
        self.data["boiler-state"] = extract_value(data, b'\x12\xf0\x00\x65\x00')

        self.send(bytes.fromhex('00 02 00 11 29 01 e6 00 00 00 ff 1f 00 00'), True)
        data = self.recv()
        self.debugger.debugData(data, "KESSEL_INFO_1")

        self.send(bytes.fromhex('00 07 00 16 27 01 e2 00 00 00 ec 1f 00 00'), True)
        data = self.recv()
        self.debugger.debugData(data, "KESSEL_INFO_2")

        self.data["boiler-start-count"] = extract_value(data, b'\x12\x2c\x01\x85\x00')
        self.data["boiler-runtime"] = extract_value(data, b'\x12\x2c\x01\x4c\x00', " h")

        self.return_to_main_menu()


    def buffer(self):
        self.send(bytes.fromhex("00 14 00 22 22 01 53 00 00 00 61 1f 00 00"), True)
        data = self.recv()
        self.debugger.debugData(data, "BUFFER")

        self.data["buffer-top-temp"] = extract_temperature(data, b'\x12\x7d\x00\x4c\x00')
        self.data["buffer-bottom-temp"] = extract_temperature(data, b'\x12\x7d\x00\xa3\x00')

        self.return_to_main_menu()


    def error(self):
        self.send(bytes.fromhex("00 14 00 23 33 00 bb 00 00 00 61 1f 00 00"), True)
        data = self.recv()
        self.debugger.debugData(data, "ERROR_1")

        self.send(bytes.fromhex("00 07 00 15 80 00 9e 00 00 00 ec 1f 00 00"), True)
        data = self.recv()
        self.debugger.debugData(data, "ERROR_2")

        self.send(bytes.fromhex("00 07 00 15 80 00 9e 00 00 00 ec 1f 00 00"), True)
        data = self.recv()
        self.debugger.debugData(data, "ERROR_SENSOR")
        self.data["error-sensor"] = extract_value(data, b'\x12\x2c\x01\x86\x00')

        self.send(bytes.fromhex("00 0b 00 05 c6 00 e6 00 00 00 d6 1f 00 00"), True)
        data = self.recv()
        self.debugger.debugData(data, "ERROR_SOLAR")
        self.data["error-solar"] = extract_value(data, b'\x12\x2c\x01\x86\x00')

        self.send(bytes.fromhex("00 0b 00 05 c6 00 e6 00 00 00 d6 1f 00 00"), True)
        data = self.recv()
        self.debugger.debugData(data, "ERROR_WATER")
        self.data["error-water"] = extract_value(data, b'\x12\x2c\x01\x86\x00')

        self.send(bytes.fromhex("00 0b 00 05 c6 00 e6 00 00 00 d6 1f 00 00"), True)
        data = self.recv()
        self.debugger.debugData(data, "WARNING_WATER")
        self.data["warning-water"] = extract_value(data, b'\x12\x2c\x01\x86\x00')

        self.return_to_main_menu()


    def close(self):
        self.s.close()
        for key, value in self.data.items():
            if value is not None:
                print(f'{key}: {value}')


    def send_mqtt(self, client):
        for key, value in self.data.items():
            if value is not None:
                client.publish(f"paradigma/systacomfort/{key}", value)


def main(args):
    def on_disconnect(client, userdata, rc):
        client.reconnect()

    #mqttClient = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttClient = mqtt.Client("paradigma")
    mqttClient.on_disconnect = on_disconnect

    if args.mqttuser is not None:
        mqttClient.username_pw_set(args.mqttuser, args.mqttpass)
    if args.mqtthost is not None:
        mqttClient.connect(args.mqtthost, port=args.mqttport)
        mqttClient.loop_start()

    while True:
        try:
            hc = HeatingConnector(args.host, args.port, args.verbosity)
            hc.connect()
            hc.main_menu()
            hc.water()
            hc.solar()
            hc.boiler()
            hc.buffer()
            hc.error()
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
