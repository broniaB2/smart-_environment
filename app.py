import streamlit as st
import pandas as pd
from supabase.client import create_client, Client 
import joblib
import os

# --- Supabase Configuration ---
SUPABASE_URL = "https://omelnzdbrwfcvxbnilvp.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9tZWxuemRicndmY3Z4Ym5pbHZwIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTgyNzA5MzMsImV4cCI6MjA3Mzg0NjkzM30.OnFZsg-FFQDgamRBEsP_Sb9XK-fntVAIUj1WQRqSPU0"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Dashboard UI ---
st.set_page_config(page_title="IoT Environmental Dashboard", layout="wide")

st.title("🏡 IoT Environmental Monitoring")
st.markdown("Real-time and historical data from your sensor.")

# --- Model Loading and Caching ---
# Use Streamlit's caching to load the model once
@st.cache_resource
def load_model():
    """Loads the trained machine learning model."""
    try:
        # The model file must be in the same directory as this script.
        model = joblib.load('smart_Environment_model.h5')
        return model
    except FileNotFoundError:
        st.error("Error: The model file 'smart_Environment_model.h5' was not found. Please ensure it is in the same directory.")
        return None
    except Exception as e:
        st.error(f"Error loading model: {e}")
        return None

# Load the model at the start of the app
model = load_model()

# Add a refresh button to invalidate the cache
if st.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

@st.cache_data(ttl=60) # Cache data for 60 seconds to avoid excessive API calls
def fetch_data():
    """Fetches all sensor data from the Supabase table."""
    try:
        # Fetching 500 records to show a good trend on the charts
        response = supabase.table("sensor_data").select("*").order("time", desc=True).limit(500).execute()
        df = pd.DataFrame(response.data)
        if not df.empty:
            df["time"] = pd.to_datetime(df["time"])
            df = df.sort_values("time", ascending=True)
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

# Fetch the data
df = fetch_data()

if not df.empty:
    # --- Key Metrics at a Glance ---
    st.subheader("Current Status")
    last_record = df.iloc[-1]
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            label="🌡️ Temperature",
            value=f"{last_record['temperature']} °C",
            delta_color="off"
        )
    with col2:
        st.metric(
            label="💧 Humidity",
            value=f"{last_record['humidity']} %",
            delta_color="off"
        )
    with col3:
        st.metric(
            label="🔋 Battery Voltage",
            value=f"{last_record['battery_voltage']} V",
            delta_color="off"
        )
    with col4:
        st.metric(
            label="🚶 Motion",
            value="Detected" if last_record['motion'] else "No Motion",
            delta_color="off"
        )
    
    # --- Prediction from the Model ---
    st.markdown("---")
    st.subheader("ML Model Prediction")
    if model is not None:
        # Prepare the input for the model
        # The model expects a 2D array: [[temperature, humidity]]
        input_data = pd.DataFrame({
            "temperature": [last_record["temperature"]],
            "humidity": [last_record["humidity"]]
        })
        
        # Make the prediction
        predicted_motion = model.predict(input_data)[0]
        
        # Display the prediction in a clear way
        prediction_status = "Motion Likely" if predicted_motion > 0.5 else "No Motion Predicted"
        st.metric(label="Predicted Activity", value=prediction_status)
        st.markdown(f"**Predicted motion value:** `{predicted_motion:.2f}`")

    st.markdown("---")
    
    # --- Trend Charts ---
    st.subheader("Historical Trends")
    tab1, tab2 = st.tabs(["Temperature & Humidity", "Battery Voltage"])
    
    with tab1:
        st.line_chart(df.set_index("time")[["temperature", "humidity"]])
        
    with tab2:
        st.line_chart(df.set_index("time")[["battery_voltage"]])
        
    st.markdown("---")
    
    # --- Raw Data Table ---
    st.subheader("Recent Data")
    st.dataframe(df.tail(25).sort_values("time", ascending=False))

else:
    st.warning("No data found in the `sensor_data` table. Please ensure your device is sending data.")

