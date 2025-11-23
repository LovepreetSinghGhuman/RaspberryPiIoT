import asyncio
import json
import uuid
from datetime import datetime, timezone
from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import MethodResponse, Message
import RPi.GPIO as GPIO
import time
import os
import signal
import sys

# GPIO configuratie voor PIR sensor
PIR_PIN = 17
CONNECTION_STRING = "lovepreetsingh=xxx.azure-devices.net;DeviceId=PresenceDetector;SharedAccessKey=y6N8Q~Fc2wXbVp9KmLrStDzG4hJkM7nBq3RfTpWxYvZ"
# Globale variabelen
client = None
device_twin_settings = {
    'logging_enabled': True,
    'sensor_delay': 5
}
presence_state = False
shutdown_scheduled = False

# GPIO initialisatie
def setup_gpio():
    """Initialiseer GPIO voor PIR sensor"""
    GPIO.setmode(GPIO.BCM)
    GPIO.setup(PIR_PIN, GPIO.IN)
    GPIO.add_event_detect(PIR_PIN, GPIO.BOTH, callback=presence_callback, bouncetime=2000)

def presence_callback(channel):
    """Callback wanneer PIR sensor beweging detecteert"""
    global presence_state
    
    if not device_twin_settings.get('logging_enabled', True):
        return
        
    new_state = GPIO.input(PIR_PIN) == GPIO.HIGH
    
    if new_state != presence_state:
        presence_state = new_state
        asyncio.create_task(send_presence_update(new_state))

async def send_presence_update(is_present):
    """Verstuur aanwezigheidsupdate naar IoT Hub"""
    try:
        presence_status = "aanwezig" if is_present else "afwezig"
        print(f"💡 Aanwezigheidsstatus gewijzigd: {presence_status}")
        
        telemetry_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "device_id": "PresenceDetector",
            "presence_status": presence_status,
            "is_present": is_present,
            "event_type": "presence_change"
        }
        
        await send_telemetry(telemetry_data)
        
    except Exception as e:
        print(f"❌ Fout bij verzenden aanwezigheidsupdate: {e}")

async def connect(connection_string):
    """Maak verbinding met Azure IoT Hub"""
    try:
        client = IoTHubDeviceClient.create_from_connection_string(connection_string)
        await client.connect()
        return client
    except Exception as e:
        raise e

async def disconnect():
    """Verbreek verbinding met IoT Hub"""
    if client:
        await client.disconnect()
    GPIO.cleanup()

async def twin_patch_handler(patch):
    """Handle Device Twin updates"""
    print(f"📝 Device Twin update ontvangen: {patch}")
    
    if 'logging_enabled' in patch:
        device_twin_settings['logging_enabled'] = patch['logging_enabled']
        status = "ingeschakeld" if patch['logging_enabled'] else "uitgeschakeld"
        print(f"🔧 Logging systeem {status}")
        
        # Update reported properties
        await update_reported_properties({'logging_enabled': patch['logging_enabled']})
    
    if 'sensor_delay' in patch:
        device_twin_settings['sensor_delay'] = patch['sensor_delay']
        print(f"⏱️ Sensor vertraging ingesteld op: {patch['sensor_delay']} seconden")

async def update_reported_properties(properties):
    """Update reported properties in Device Twin"""
    try:
        await client.patch_twin_reported_properties(properties)
        print(f"✅ Reported properties bijgewerkt: {properties}")
    except Exception as e:
        print(f"❌ Fout bij updaten reported properties: {e}")

async def method_request_handler(method_request):
    """Handle direct method calls"""
    print(f"📨 Method request ontvangen: {method_request.name}")
    
    if method_request.name == "shutdown_device":
        return await handle_shutdown_method(method_request)
    elif method_request.name == "get_status":
        return await handle_status_method(method_request)
    else:
        response_payload = {"error": f"Onbekende methode: {method_request.name}"}
        method_response = MethodResponse.create_from_method_request(
            method_request, 400, response_payload
        )
        await client.send_method_response(method_response)

async def handle_shutdown_method(method_request):
    """Handle shutdown direct method"""
    global shutdown_scheduled
    
    try:
        shutdown_delay = method_request.payload.get('delay_seconds', 10)
        shutdown_time = datetime.now(timezone.utc).timestamp() + shutdown_delay
        
        response_payload = {
            "message": f"Device wordt uitgeschakeld over {shutdown_delay} seconden",
            "scheduled_shutdown_time": datetime.fromtimestamp(shutdown_time).isoformat(),
            "shutdown_delay_seconds": shutdown_delay
        }
        
        method_response = MethodResponse.create_from_method_request(
            method_request, 200, response_payload
        )
        await client.send_method_response(method_response)
        
        print(f"⏰ Device uitschakeling gepland over {shutdown_delay} seconden")
        
        # Plan daadwerkelijke shutdown
        shutdown_scheduled = True
        await asyncio.sleep(shutdown_delay)
        
        print("🔴 Device wordt nu uitgeschakeld...")
        await send_telemetry({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "device_id": "PresenceDetector",
            "event_type": "shutdown",
            "message": "Device wordt uitgeschakeld"
        })
        
        # Disconnect en shutdown
        await disconnect()
        os.system("sudo shutdown -h now")
        
    except Exception as e:
        print(f"❌ Fout bij shutdown: {e}")

async def handle_status_method(method_request):
    """Handle status direct method"""
    try:
        response_payload = {
            "logging_enabled": device_twin_settings.get('logging_enabled', True),
            "presence_detected": presence_state,
            "sensor_delay": device_twin_settings.get('sensor_delay', 5),
            "current_time": datetime.now(timezone.utc).isoformat()
        }
        
        method_response = MethodResponse.create_from_method_request(
            method_request, 200, response_payload
        )
        await client.send_method_response(method_response)
        
    except Exception as e:
        print(f"❌ Fout bij status method: {e}")

async def send_telemetry(data):
    """Verstuur telemetry data naar Azure IoT Hub"""
    try:
        message = Message(json.dumps(data))
        message.message_id = str(uuid.uuid4())
        message.content_type = "application/json"
        message.content_encoding = "utf-8"
        
        await client.send_message(message)
        print(f"📤 Telemetry verzonden: {data}")
        
    except Exception as e:
        print(f"❌ Fout bij verzenden telemetry: {e}")

async def send_heartbeat():
    """Verstuur periodieke heartbeat"""
    while True:
        try:
            if device_twin_settings.get('logging_enabled', True):
                heartbeat_data = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "device_id": "PresenceDetector",
                    "event_type": "heartbeat",
                    "presence_state": presence_state,
                    "logging_enabled": device_twin_settings.get('logging_enabled', True)
                }
                await send_telemetry(heartbeat_data)
                
        except Exception as e:
            print(f"❌ Fout bij heartbeat: {e}")
            
        await asyncio.sleep(60)  # Elke minuut

async def main():
    global client
    
    print("🚀 Presence Detector Starting...")
    print("=====================================")
    
    # GPIO setup
    setup_gpio()
    print("✅ GPIO geïnitialiseerd")
    
    # IoT Hub verbinding
    client = await connect(CONNECTION_STRING)
    client.on_twin_desired_properties_patch_received = twin_patch_handler
    client.on_method_request_received = method_request_handler
    
    # Initialiseer reported properties
    await update_reported_properties({
        'logging_enabled': device_twin_settings['logging_enabled'],
        'sensor_delay': device_twin_settings['sensor_delay'],
        'device_status': 'online'
    })
    
    print("✅ Verbonden met Azure IoT Hub")
    print("🔧 Logging systeem: INGESCHAKELD")
    print("📡 Luisteren naar PIR sensor op GPIO 17...")
    print("💡 Druk op Ctrl+C om te stoppen")
    print("=====================================")
    
    # Start heartbeat task
    heartbeat_task = asyncio.create_task(send_heartbeat())
    
    try:
        # Hoofdloop
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Gestopt door gebruiker")
    finally:
        heartbeat_task.cancel()
        await disconnect()

def signal_handler(sig, frame):
    """Handle Ctrl+C signal"""
    print('\n🛑 Ontvangen stop signaal...')
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"❌ Onverwachte fout: {e}")
        GPIO.cleanup()