#!/usr/bin/env python3
"""
Hitachi Hiverter Si-1.1K-H3 Modbus TCP Client with AWS IoT Core MQTT Publishing
"""

import time
import json
import ssl
import logging
from pymodbus.client.sync import ModbusTcpClient
from paho.mqtt import client as mqtt_client

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HitachiInverterClient:
    def __init__(self, ip_address, port=502, unit_id=1):
        self.ip_address = ip_address
        self.port = port
        self.unit_id = unit_id
        self.client = None
        
    def connect(self):
        try:
            self.client = ModbusTcpClient(self.ip_address, port=self.port)
            connection = self.client.connect()
            if connection:
                logger.info(f"Connected to Hitachi inverter at {self.ip_address}:{self.port}")
                return True
            else:
                logger.error("Failed to connect to inverter")
                return False
        except Exception as e:
            logger.error(f"Connection error: {e}")
            return False
    
    def disconnect(self):
        if self.client:
            self.client.close()
            logger.info("Disconnected from inverter")
    
    def read_registers(self, address, count, function_code=3):
        try:
            if function_code == 3:
                result = self.client.read_holding_registers(address, count, unit=self.unit_id)
            elif function_code == 4:
                result = self.client.read_input_registers(address, count, unit=self.unit_id)
            else:
                logger.error("Unsupported function code")
                return None
                
            if result.isError():
                logger.error(f"Modbus error: {result}")
                return None
            return result.registers
        except Exception as e:
            logger.error(f"Read error: {e}")
            return None
    
    def get_pv_data(self):
        pv_data = {}
        registers_map = {
            'dc_voltage': (40001, 1, 0.1),
            'dc_current': (40002, 1, 0.01),
            'dc_power': (40003, 2, 1),
            'ac_voltage': (40010, 1, 0.1),
            'ac_current': (40011, 1, 0.01),
            'ac_power': (40012, 2, 1),
            'frequency': (40015, 1, 0.01),
            'energy_today': (40020, 2, 0.1),
            'energy_total': (40022, 2, 0.1),
            'temperature': (40030, 1, 0.1),
            'status': (40040, 1, 1),
        }
        
        for param, (address, count, scale) in registers_map.items():
            registers = self.read_registers(address - 1, count)
            if registers is not None:
                if count == 1:
                    value = registers[0] * scale
                elif count == 2:
                    value = (registers[0] << 16 | registers[1]) * scale
                else:
                    value = registers
                pv_data[param] = value
            else:
                pv_data[param] = None
        return pv_data
    
    def get_system_status(self):
        status_data = {}
        status_registers = {
            'operating_state': (40100, 1),
            'fault_code': (40101, 1),
            'warning_code': (40102, 1),
        }
        for param, (address, count) in status_registers.items():
            registers = self.read_registers(address - 1, count)
            if registers is not None:
                status_data[param] = registers[0] if count == 1 else registers
            else:
                status_data[param] = None
        return status_data
    
    def collect_all_data(self):
        return {
            'timestamp': time.time(),
            'pv_data': self.get_pv_data(),
            'system_status': self.get_system_status(),
        }

# ---------------- AWS IoT Core MQTT Setup ---------------- #

def create_mqtt_client(client_id, endpoint, cert_path, key_path, root_ca_path):
    client = mqtt_client.Client(client_id)
    client.tls_set(
        ca_certs=root_ca_path,
        certfile=cert_path,
        keyfile=key_path,
        tls_version=ssl.PROTOCOL_TLSv1_2,
    )
    client.tls_insecure_set(False)
    client.connect(endpoint, 8883, 60)
    return client

def main():
    # Inverter config
    INVERTER_IP = "192.168.1.100"
    
    # AWS IoT config
    AWS_ENDPOINT = "your-aws-iot-endpoint.amazonaws.com"
    CLIENT_ID = "HitachiInverterClient"
    TOPIC = "hitachi/inverter/data"
    CERT_PATH = "certs/deviceCert.pem.crt"
    KEY_PATH = "certs/private.pem.key"
    ROOT_CA_PATH = "certs/AmazonRootCA1.pem"
    
    # Polling
    POLL_INTERVAL = 10  # seconds
    
    inverter = HitachiInverterClient(INVERTER_IP)
    if not inverter.connect():
        logger.error("Failed to connect to inverter. Exiting.")
        return
    
    mqttc = create_mqtt_client(CLIENT_ID, AWS_ENDPOINT, CERT_PATH, KEY_PATH, ROOT_CA_PATH)
    logger.info("Connected to AWS IoT Core")
    
    try:
        while True:
            data = inverter.collect_all_data()
            payload = json.dumps(data)
            
            # Print locally
            print(f"\n--- Inverter Data ({time.strftime('%Y-%m-%d %H:%M:%S')}) ---")
            print(payload)
            
            # Publish to AWS IoT Core
            mqttc.publish(TOPIC, payload, qos=1)
            logger.info(f"Published data to AWS IoT topic {TOPIC}")
            
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        inverter.disconnect()
        mqttc.disconnect()

if __name__ == "__main__":
    main()
