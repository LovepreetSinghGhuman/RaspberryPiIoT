import paho.mqtt.client as mqtt
import json
import time
import random
from datetime import datetime

# MQTT Broker instellingen - kies één broker
BROKER_CONFIGS = {
    "mosquitto_local": {
        "broker": "localhost",
        "port": 1883,
        "username": None,
        "password": None
    },
    "mosquitto_public": {
        "broker": "test.mosquitto.org",
        "port": 1883,
        "username": None,
        "password": None
    },
    "hivemq": {
        "broker": "broker.hivemq.com",
        "port": 1883,
        "username": None,
        "password": None
    }
}

SELECTED_BROKER = "hivemq"  # Verander naar "mosquitto_local" of "mosquitto_public" indien gewenst

# Topic voor temperatuur data
TEMPERATURE_TOPIC = "Temp"

class TemperaturePublisher:
    def __init__(self):
        self.client = mqtt.Client()
        self.broker_config = BROKER_CONFIGS[SELECTED_BROKER]
        
        # Setup callbacks
        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish
        
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"Verbonden met MQTT Broker: {self.broker_config['broker']}")
        else:
            print(f"Verbinding mislukt. Return code: {rc}")
            
    def on_publish(self, client, userdata, mid):
        print(f"Bericht gepubliceerd met ID: {mid}")
        
    def connect(self):
        try:
            if self.broker_config["username"] and self.broker_config["password"]:
                self.client.username_pw_set(self.broker_config["username"], 
                                         self.broker_config["password"])
            
            self.client.connect(self.broker_config["broker"], 
                              self.broker_config["port"], 
                              60)
            self.client.loop_start()
            return True
        except Exception as e:
            print(f"Connectie fout: {e}")
            return False
            
    def publish_temperature(self):
        """Genereer en publiceer temperatuur data"""
        # Simuleer temperatuur data (in een echte setup zou dit van een sensor komen)
        temperature = round(random.uniform(18.0, 25.0), 2)
        humidity = round(random.uniform(40.0, 60.0), 2)
        
        payload = {
            "device_id": "RPiSmartHome",
            "temperature": temperature,
            "humidity": humidity,
            "timestamp": datetime.now().isoformat(),
            "unit": "celsius"
        }
        
        result = self.client.publish(TEMPERATURE_TOPIC, json.dumps(payload))
        
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"Temperatuur gepubliceerd: {temperature}°C, Vochtigheid: {humidity}%")
        else:
            print(f"Publicatie mislukt: {result.rc}")
            
    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()
        print("Verbinding verbroken")

async def send_telemetry(data):
    message = Message(json.dumps(data))
    message.message_id = uuid.uuid4()
    message.content_type = "application/json"
    message.content_encoding = "utf-8"
 
    await client.send_message(message)
    
async def send_temperature_to_iothub():
    telemetry_data = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "device_id": "RPiSmartHome",
        "temperature": random.randint(19, 31)
    }
    
    await send_telemetry(telemetry_data)
    
    
def main():
    publisher = TemperaturePublisher()
    
    if publisher.connect():
        try:
            print("Start met publiceren van temperatuur data...")
            print("Druk op Ctrl+C om te stoppen")
            
            while True:
                publisher.publish_temperature()
                time.sleep(5)  # Publiceer elke 5 seconden
                
        except KeyboardInterrupt:
            print("\nGestopt door gebruiker")
        finally:
            publisher.disconnect()
    else:
        print("Kon niet verbinden met MQTT broker")

if __name__ == "__main__":
    main()