#!/usr/bin/env python3
"""
Hitachi Inverter RS485 Modbus RTU Client with AWS IoT Core MQTT Publishing
Using Raspberry Pi UART + MAX485 Module
"""

import time
import json
import ssl
import logging
from pymodbus.client.sync import ModbusSerialClient
from paho.mqtt import client as mqtt_client

# ---------------- Logging ---------------- #
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------- Inverter Modbus Client ---------------- #
class HitachiInverterRS485:
    def __init__(self, port="/dev/serial0", baudrate=9600, unit_id=1):
        self.client = ModbusSerialClient(
            method="rtu",
            port=port,
            baudrate=baudrate,
            parity="N",
            stopbits=1,
            bytesize=8,
            timeout=1
        )
        self.unit_id = unit_id

    def connect(self):
        if self.client.connect():
            logger.info("✅ Connected to inverter via RS485")
            return True
        else:
            logger.error("❌ Failed to connect inverter")
            return False

    def disconnect(self):
        self.client.close()
        logger.info("🔌 Disconnected from inverter")

    def read_registers(self, address, count):
        try:
            result = self.client.read_holding_registers(address, count, unit=self.unit_id)
            if result.isError():
                logger.error(f"Modbus error: {result}")
                return None
            return result.registers
        except Exception as e:
            logger.error(f"Read error: {e}")
            return None

    def get_data(self):
        pv_data = {}
        registers_map = {
            'dc_voltage': (40001, 1, 0.1),
            'dc_current': (40002, 1, 0.01),
            'ac_voltage': (40010, 1, 0.1),
            'ac_current': (40011, 1, 0.01),
            'ac_power': (40012, 2, 1),
            'frequency': (40015, 1, 0.01),
        }
        for param, (address, count, scale) in registers_map.items():
            regs = self.read_registers(address-1, count)   # -1 because Modbus RTU offset
            if regs:
                if count == 1:
                    value = regs[0] * scale
                elif count == 2:
                    value = (regs[0] << 16 | regs[1]) * scale
                else:
                    value = regs
                pv_data[param] = value
            else:
                pv_data[param] = None
        return pv_data

# ---------------- AWS IoT Core MQTT ---------------- #
def create_mqtt_client(client_id, endpoint, cert_path, key_path, root_ca_path):
    client = mqtt_client.Client(client_id)
    client.tls_set(
        ca_certs=root_ca_path,
        certfile=cert_path,
        keyfile=key_path,
        tls_version=ssl.PROTOCOL_TLSv1_2,
    )
    client.connect(endpoint, 8883, 60)
    return client

# ---------------- Main ---------------- #
def main():
    # AWS IoT Config
    AWS_ENDPOINT = "a209vpgon02vvn-ats.iot.ap-south-1.amazonaws.com"
    CLIENT_ID = "MySolarThing"
    TOPIC = "solar_power_data/topic"
    CERT_PATH = "cert/certificate.pem.crt"
    KEY_PATH = "cert/private.pem.key"
    ROOT_CA_PATH = "cert/RootCA.pem"

    POLL_INTERVAL = 10

    inverter = HitachiInverterRS485(port="/dev/serial0", baudrate=9600, unit_id=1)
    if not inverter.connect():
        return

    mqttc = create_mqtt_client(CLIENT_ID, AWS_ENDPOINT, CERT_PATH, KEY_PATH, ROOT_CA_PATH)
    logger.info("✅ Connected to AWS IoT Core")

    try:
        while True:
            data = inverter.get_data()
            payload = json.dumps({"timestamp": time.time(), "inverter_data": data})

            print("\n--- Inverter Data ---")
            print(payload)

            mqttc.publish(TOPIC, payload, qos=1)
            logger.info(f"Published to topic {TOPIC}")

            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Stopping...")
    finally:
        inverter.disconnect()
        mqttc.disconnect()

if __name__ == "__main__":
    main()
