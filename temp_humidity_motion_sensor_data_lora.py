import paho.mqtt.client as mqtt
import json
import os
import requests
import pandas as pd
from datetime import datetime

# -----------------------
# Configuration
# -----------------------
broker = "eu1.cloud.thethings.network"
port = 1883
username = "bd-test-app2@ttn"
password = "NNSXS.NGFSXX4UXDX55XRIDQZS6LPR4OJXKIIGSZS56CQ.6O4WUAUHFUAHSTEYRWJX6DDO7TL2IBLC7EV2LS4EHWZOOEPCEUOA"
device_id = "lht65n-01-temp-humidity-sensor"
csv_file = "sensor_data.csv"

print("Current working directory:", os.getcwd())

# -----------------------
# CSV Save Function
# -----------------------
def save_to_csv(records):
    if not records:
        print("No records to save.")
        return
    
    df = pd.DataFrame(records)

    # Convert time to Uganda time (UTC+3)
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'])
        df['time'] = df['time'] + pd.Timedelta(hours=3)  # Convert to Uganda time
    
    if not os.path.exists(csv_file):
        df.to_csv(csv_file, mode="w", header=True, index=False)
        print(f"✔ Created new CSV file with {len(records)} records")
    else:
        # Read existing data and append new records
        existing_df = pd.read_csv(csv_file)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        
        # Remove duplicates by timestamp
        combined_df.drop_duplicates(subset=["time"], keep="last", inplace=True)
        combined_df.to_csv(csv_file, index=False)
        print(f"✔ Added {len(records)} new records to {csv_file} (total: {len(combined_df)} records)")

# -----------------------
# Fetch Historical Data
# -----------------------
def get_historical_sensor_data():
    app_id = "bd-test-app2"
    api_key = password
    url = f"https://{broker}/api/v3/as/applications/{app_id}/devices/{device_id}/packages/storage/uplink_message"

    headers = {"Authorization": f"Bearer {api_key}"}
    params = {"last": "48h"}  # fetch last 48 hours of data

    response = requests.get(url, headers=headers, params=params)
    print("HTTP status code:", response.status_code)

    if response.status_code == 200:
        if not response.text.strip():
            print("No historical messages available for this device.")
            return []

        records = []
        # The response contains one JSON object per line
        for line in response.text.strip().splitlines():
            try:
                item = json.loads(line)
                result = item.get("result", {})
                decoded = result.get("uplink_message", {}).get("decoded_payload", {})
                timestamp = result.get("received_at", "")
                
                # Only add if we have the required fields
                if all(key in decoded for key in ["field1", "field3", "field4", "field5"]):
                    records.append({
                        "time": timestamp,
                        "battery_voltage": decoded.get("field1"),
                        "humidity": decoded.get("field3"),
                        "motion": decoded.get("field4"),
                        "temperature": decoded.get("field5")
                    })
            except json.JSONDecodeError as e:
                print("Skipping invalid line:", e)
                continue

        if records:
            print(f"Parsed {len(records)} historical records.")
            save_to_csv(records)
            return records
        else:
            print("No valid historical records found.")
            return []
    else:
        print("Error fetching historical data:", response.status_code, response.text)
        return []

# -----------------------
# MQTT Callbacks
# -----------------------
topic = f"v3/{username}/devices/{device_id}/up"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to TTN MQTT broker!")
        client.subscribe(topic)
    else:
        print(f"Failed to connect, return code {rc}")

def on_message(client, userdata, message):
    try:
        payload = json.loads(message.payload.decode())
        decoded = payload.get("uplink_message", {}).get("decoded_payload", {})
        timestamp = payload.get("received_at", "")
        
        print("Received MQTT message with fields:", list(decoded.keys()))
        
        # Check if we have all the required fields
        if all(key in decoded for key in ["field1", "field3", "field4", "field5"]):
            record = {
                "time": timestamp,
                "battery_voltage": decoded.get("field1"),
                "humidity": decoded.get("field3"),
                "motion": decoded.get("field4"),
                "temperature": decoded.get("field5")
            }
            save_to_csv([record])  # Pass as a list of one record
        else:
            print("Missing required fields in MQTT message")
    except Exception as e:
        print("Error processing MQTT message:", e)

# -----------------------
# Main Execution
# -----------------------
if __name__ == "__main__":
    # First, fetch historical data
    historical_records = get_historical_sensor_data()
    
    # Then set up MQTT client for real-time updates
    client = mqtt.Client()
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(broker, port, 60)
        client.loop_forever()
    except Exception as e:
        print("Error connecting to MQTT broker:", e)