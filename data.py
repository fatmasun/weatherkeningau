from flask import Flask, jsonify
import requests
import pymysql
import time
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
    except Exception as e:
        print("Database Error:", e)

# Function to fetch weather data every 5 minutes
def collect_weather_data():
    while True:
        response = requests.get(URL)
        if response.status_code == 200:
            data = response.json()
            temp = data["main"]["temp"]
            humidity = data["main"]["humidity"]
            wind_speed = data["wind"]["speed"]
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')

            store_data(temp, humidity, wind_speed, timestamp)
            print(f"Logged: {timestamp} - Temp: {temp}°C, Humidity: {humidity}%, Wind Speed: {wind_speed} m/s")

        else:
            print("Error fetching weather data")

        time.sleep(300)  # Wait for 5 minutes

# Start the background thread for data collection
threading.Thread(target=collect_weather_data, daemon=True).start()

# Flask route to fetch stored data
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

if __name__ == '__main__':
    app.run(debug=True)
