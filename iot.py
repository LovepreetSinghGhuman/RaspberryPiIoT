import asyncio
import json
import uuid
from datetime import datetime, timezone
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import MethodResponse, Message

try:
    from gpiozero import CPUTemperature
    HAS_GPIOZERO = True
except ImportError:
    print("gpiozero niet geïnstalleerd. Gebruik: pip install gpiozero")
    HAS_GPIOZERO = False
    # Fallback voor testing zonder Raspberry Pi
    import random

CONNECTION_STRING = "lovepreetsingh=xxx.azure-devices.net;DeviceId=RPiSmartHome;SharedAccessKey=y6N8Q~Fc2wXbVp9KmLrStDzG4hJkM7nBq3RfTpWxYvZ"
client = None
device_twin_settings = {}

async def connect(connection_string):
    try:
        client = IoTHubDeviceClient.create_from_connection_string(connection_string)
        await client.connect()
        return client
    except Exception as e:
        raise e

async def disconnect():
    if client:
        await client.disconnect()

async def twin_patch_handler(patch):
    print(f"Device Twin update received: {patch}")
    if 'battery_level' in patch:
        new_battery_level = patch['battery_level']
        print(f"Battery level changed to: {new_battery_level}%")
        await update_reported_battery_level(new_battery_level)
        device_twin_settings['battery_level'] = new_battery_level
        print(f"Local settings updated: {device_twin_settings}")

async def update_reported_battery_level(level):
    try:
        await client.patch_twin_reported_properties({"battery_level": level})
        print(f"Reported battery level updated to: {level}%")
    except Exception as e:
        print(f"Error updating reported battery level: {e}")
        raise e

async def get_device_twin_settings():
    try:
        twin = await client.get_twin()
        print("Full device twin retrieved:")
        print(twin)
        if 'desired' in twin and 'properties' in twin['desired']:
            device_twin_settings.update(twin['desired']['properties'])
        elif 'properties' in twin and 'desired' in twin['properties']:
            device_twin_settings.update(twin['properties']['desired'])
        if 'reported' in twin and 'properties' in twin['reported']:
            reported_props = twin['reported']['properties']
            for key, value in reported_props.items():
                if key not in ['$metadata', '$version']:
                    device_twin_settings[f"reported_{key}"] = value
        elif 'properties' in twin and 'reported' in twin['properties']:
            reported_props = twin['properties']['reported']
            for key, value in reported_props.items():
                if key not in ['$metadata', '$version']:
                    device_twin_settings[f"reported_{key}"] = value
        print(f"Device twin settings loaded: {device_twin_settings}")
        return device_twin_settings
    except Exception as e:
        print(f"Error retrieving device twin: {e}")
        raise e

async def update_initial_battery_status():
    try:
        await client.patch_twin_reported_properties({"battery_level": 31})
        print("Initial battery status updated to 31%")
        device_twin_settings['battery_level'] = 31
        device_twin_settings['reported_battery_level'] = 31
    except Exception as e:
        raise e

async def method_request_handler(method_request):
    print(f"Method request received: {method_request.name}")
    if method_request.name == "reboot_device":
        print("Rebooting device...")
        response_payload = {"message": "Device reboot initiated"}
        method_response = MethodResponse.create_from_method_request(
            method_request, 200, response_payload
        )
        await client.send_method_response(method_response)
    elif method_request.name == "update_device":
        version = method_request.payload.get("version")
        print(f"Updating device to version {version}")
        response_payload = {"message": f"Update to version {version} initiated"}
        method_response = MethodResponse.create_from_method_request(
            method_request, 200, response_payload
        )
        await client.send_method_response(method_response)
    elif method_request.name == "change_battery_level":
        new_level = method_request.payload.get("battery_level")
        print(f"Changing battery level to: {new_level}%")
        await update_reported_battery_level(new_level)
        device_twin_settings['battery_level'] = new_level
        device_twin_settings['reported_battery_level'] = new_level
        response_payload = {"message": f"Battery level changed to {new_level}%"}
        method_response = MethodResponse.create_from_method_request(
            method_request, 200, response_payload
        )
        await client.send_method_response(method_response)
    else:
        print(f"Unknown method: {method_request.name}")
        response_payload = {"error": f"Unknown method: {method_request.name}"}
        method_response = MethodResponse.create_from_method_request(
            method_request, 400, response_payload
        )
        await client.send_method_response(method_response)

async def get_reporting_properties():
    try:
        twin = await client.get_twin()
        reporting_props = {}
        if 'reported' in twin and 'properties' in twin['reported']:
            for key, value in twin['reported']['properties'].items():
                if key not in ['$metadata', '$version']:
                    reporting_props[key] = value
        elif 'properties' in twin and 'reported' in twin['properties']:
            for key, value in twin['properties']['reported'].items():
                if key not in ['$metadata', '$version']:
                    reporting_props[key] = value
        return reporting_props
    except Exception as e:
        print(f"Error getting reporting properties: {e}")
        return {}

async def update_battery_status(level):
    try:
        await client.patch_twin_reported_properties({"battery_level": level})
        print(f"Battery status updated to {level}%")
        device_twin_settings['battery_level'] = level
        device_twin_settings['reported_battery_level'] = level
    except Exception as e:
        print(f"Error updating battery status: {e}")
        raise e

def get_actual_temperature():
    """
    Lees de werkelijke temperatuur van de Raspberry Pi CPU
    Gebruikt gpiozero voor echte temperatuurmeting
    """
    try:
        if HAS_GPIOZERO:
            # Lees de CPU temperatuur via gpiozero
            cpu = CPUTemperature()
            temperature = cpu.temperature
            print(f"CPU Temperatuur gemeten: {temperature}°C")
            return round(temperature, 2)
        else:
            # Fallback voor testing zonder gpiozero
            print("gpiozero niet beschikbaar - gebruik test data")
            return round(random.uniform(40.0, 60.0), 2)
            
    except Exception as e:
        print(f"Fout bij uitlezen temperatuur: {e}")
        # Fallback waarde bij error
        return 45.0

def get_actual_humidity():
    """
    Voor humidity zou je een echte sensor zoals DHT11/DHT22 kunnen gebruiken
    Dit is een placeholder voor toekomstige sensor integratie
    """
    try:
        humidity = 45.0  # Vaste waarde als placeholder
        
        print(f"Vochtigheid: {humidity}% (placeholder)")
        return humidity
        
    except Exception as e:
        print(f"Fout bij uitlezen vochtigheid: {e}")
        return 50.0

async def send_telemetry(data):
    """Verstuur telemetry data naar Azure IoT Hub"""
    try:
        message = Message(json.dumps(data))
        
        message.message_id = str(uuid.uuid4())
        message.content_type = "application/json"
        message.content_encoding = "utf-8"
        
        await client.send_message(message)
        print(f"Telemetry verzonden: {data}")
        
    except Exception as e:
        print(f"Fout bij verzenden telemetry: {e}")
        raise e

async def send_temperature_to_iothub():
    """Verzamel echte temperatuur data en verstuur naar IoT Hub"""
    try:
        actual_temperature = get_actual_temperature()
        actual_humidity = get_actual_humidity()
        
        telemetry_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "device_id": "RPiSmartHome",
            "temperature": actual_temperature,
            "humidity": actual_humidity,
            "battery_level": device_twin_settings.get('battery_level', 31),
            "sensor_type": "CPU" if HAS_GPIOZERO else "Simulated"
        }
        
        await send_telemetry(telemetry_data)
        
    except Exception as e:
        print(f"Fout bij verzamelen temperatuur data: {e}")

async def main():
    global client
    
    print("=== Raspberry Pi Temperatuur Monitor ===")
    print(f"gpiozero beschikbaar: {HAS_GPIOZERO}")
    
    if not HAS_GPIOZERO:
        print("WAARSCHUWING: gpiozero niet geïnstalleerd. Gebruik test data.")
        print("Installeer met: pip install gpiozero")
    
    client = await connect(CONNECTION_STRING)
    
    client.on_twin_desired_properties_patch_received = twin_patch_handler
    client.on_method_request_received = method_request_handler
    
    await get_device_twin_settings()
    
    reporting_properties = await get_reporting_properties()
    print(f"Initial reporting properties: {reporting_properties}")
    
    await update_battery_status(31)
    
    print("\n" + "="*50)
    print("Device is running and listening for twin updates and method calls...")
    print("Echte temperatuur data wordt elke 10 seconden verzonden...")
    print("Press Ctrl+C to stop.")
    print("="*50 + "\n")
    
    while True:
        await send_temperature_to_iothub()
        await asyncio.sleep(10)  # Wacht 10 seconden

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nScript interrupted by user")
        asyncio.run(disconnect())
    except Exception as e:
        print(f"An error occurred: {e}")
        asyncio.run(disconnect())