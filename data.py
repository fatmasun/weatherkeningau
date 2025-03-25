import pymysql
import requests
from datetime import datetime
import pytz
import os
import time
from flask import Flask, jsonify
import threading

app = Flask(__name__)

# OpenWeatherMap API details
API_KEY = "e82a4d76542a77e2cae25e5efacef03c"
CITY = "Keningau"
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

# MySQL Database Connection (FreeSQLDatabase)
DB_HOST = "sql12.freesqldatabase.com"
DB_USER = "sql12769379"
DB_PASSWORD = "XjSDiyttDc"
DB_NAME = "sql12769379"

# Function to store data in MySQL
def store_data(temp, humidity, wind_speed, timestamp):
    try:
        connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        cursor = connection.cursor()

        query = "INSERT INTO weather_data (timestamp, temperature, humidity, wind_speed) VALUES (%s, %s, %s, %s)"
        cursor.execute(query, (timestamp, temp, humidity, wind_speed))

        connection.commit()
        cursor.close()
        connection.close()
        print(f"✅ Data stored: {timestamp} - Temp: {temp}°C, Humidity: {humidity}%, Wind Speed: {wind_speed} m/s")
    except Exception as e:
        print("❌ Database Error:", e)

# Function to fetch weather data every 5 minutes
def collect_weather_data():
    while True:
        response = requests.get(URL)
        if response.status_code == 200:
            data = response.json()
            temp = data["main"]["temp"]
            humidity = data["main"]["humidity"]
            wind_speed = data["wind"]["speed"]

            # Get current time with timezone
            tz = pytz.timezone("Asia/Kuching")  # Adjust timezone if needed
            timestamp = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')

            store_data(temp, humidity, wind_speed, timestamp)
        else:
            print("❌ Error fetching weather data")

        time.sleep(300)  # Wait for 5 minutes

# Start the background thread for data collection
# threading.Thread(target=collect_weather_data, daemon=True).start()

# Flask API route to fetch stored weather data
@app.route('/weather', methods=['GET'])
def get_weather_data():
    try:
        connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME)
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        cursor.execute("SELECT * FROM weather_data ORDER BY timestamp DESC LIMIT 10")
        data = cursor.fetchall()

        cursor.close()
        connection.close()
        return jsonify(data)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if _name_ == "_main_":
    # Start the weather data fetching in a background thread
    threading.Thread(target=collect_weather_data, daemon=True).start()
    app.run(host="0.0.0.0", port=5000, debug=True)
