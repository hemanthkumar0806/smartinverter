#!/usr/bin/env python3
"""
Hitachi Hiverter Si-1.1K-H3 Modbus TCP Client
With AWS IoT Core integration (MQTT publishing every 10s)
"""

import time
import json
import logging
from pymodbus.client.sync import ModbusTcpClient
from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder

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


def main():
    # Inverter config
    INVERTER_IP = "192.168.1.100"
    POLL_INTERVAL = 10  # seconds

    # AWS IoT config - replace with your endpoint & certs
    ENDPOINT = "YOUR_ENDPOINT_HERE-ats.iot.YOUR_REGION.amazonaws.com"
    CLIENT_ID = "hitachi_inverter_client"
    PATH_TO_CERT = "certs/device-certificate.pem.crt"
    PATH_TO_KEY = "certs/private.pem.key"
    PATH_TO_ROOT = "certs/AmazonRootCA1.pem"
    TOPIC = "inverter/data"

    # Setup AWS IoT MQTT connection
    event_loop_group = io.EventLoopGroup(1)
    host_resolver = io.DefaultHostResolver(event_loop_group)
    client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)

    mqtt_connection = mqtt_connection_builder.mtls_from_path(
        endpoint=ENDPOINT,
        cert_filepath=PATH_TO_CERT,
        pri_key_filepath=PATH_TO_KEY,
        client_bootstrap=client_bootstrap,
        ca_filepath=PATH_TO_ROOT,
        client_id=CLIENT_ID,
        clean_session=False,
        keep_alive_secs=30,
    )

    logger.info(f"Connecting to AWS IoT Core at {ENDPOINT}...")
    connect_future = mqtt_connection.connect()
    connect_future.result()
    logger.info("Connected to AWS IoT Core")

    # Create inverter client
    inverter = HitachiInverterClient(INVERTER_IP)
    
    if not inverter.connect():
        logger.error("Failed to connect to inverter. Exiting.")
        return
    
    try:
        while True:
            data = inverter.collect_all_data()

            # Publish to AWS IoT
            message_json = json.dumps(data)
            logger.info(f"Publishing data to {TOPIC}: {message_json}")
            mqtt_connection.publish(
                topic=TOPIC,
                payload=message_json,
                qos=mqtt.QoS.AT_LEAST_ONCE,
            )

            time.sleep(POLL_INTERVAL)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        inverter.disconnect()
        mqtt_connection.disconnect().result()
        logger.info("Disconnected from AWS IoT Core")


if __name__ == "__main__":
    main()
