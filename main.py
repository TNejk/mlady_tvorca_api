from flask import Flask, jsonify, request
import psycopg2
from paho.mqtt import publish
import os

db_password = os.getenv('MLADY_TVORCA_DB_PASSWORD')
app = Flask(__name__)

def connect():
  try:
    conn = psycopg2.connect(database='postgres', user='postgres', password=db_password, host='localhost', port='5433')
    cursor = conn.cursor()
    return conn, cursor
  except psycopg2.Error as e:
    return None, jsonify({'error': str(e), 'code': 500})

def disconnect(connection,cursor):
  cursor.close()
  connection.close()

def fetch_data(cursor,query):
  try:
    cursor.execute(query)
    return cursor.fetchall()
  except psycopg2.Error as e:
    return {'error': str(e).strip(), 'code': 500}

@app.route('/get/data/all', methods=['GET'])
def get_data():
  conn, cursor = connect()
  if conn is None:
    return cursor
  query = "SELECT * FROM mqtt_data"
  data = fetch_data(cursor, query)
  if isinstance(data, dict) and 'error' in data:
    disconnect(conn, cursor)
    return jsonify(data), 500
  disconnect(conn, cursor)
  return jsonify(data), 200

@app.route('/get/data/temperature', methods=['GET'])
def get_temperature():
  conn, cursor = connect()
  if conn is None:
    return cursor
  date = request.values.get('date')
  if not date:
    disconnect(conn, cursor)
    return jsonify({'error': 'Missing parameter: date'}), 400
  query = f"SELECT * FROM mqtt_data WHERE topic = 'Tunel_1/esp32/TeplotaVzduchu' AND date > :{date} ORDER BY date ASC"
  data = fetch_data(cursor, query)
  if isinstance(data, dict) and 'error' in data:
    disconnect(conn, cursor)
    return jsonify(data)
  disconnect(conn, cursor)
  return jsonify(data), 200

@app.route('/get/data/humidity', methods=['GET'])
def get_humidity():
  conn, cursor = connect()
  if conn is None:
    return cursor
  date = request.values.get('date')
  if not date:
    disconnect(conn, cursor)
    return jsonify({'error': 'Missing parameter: date'}), 400
  query = "SELECT * FROM mqtt_data WHERE topic = 'Tunel_1/esp32/VlhkostVzduchu' AND date > :{date} ORDER BY date ASC"
  data = fetch_data(cursor, query)
  if isinstance(data, dict) and 'error' in data:
    disconnect(conn, cursor)
    return jsonify(data)
  disconnect(conn, cursor)
  return jsonify(data), 200

@app.route('/get/data/pressure', methods=['GET'])
def get_pressure():
  conn, cursor = connect()
  if conn is None:
    return cursor
  date = request.values.get('date')
  if not date:
    disconnect(conn, cursor)
    return jsonify({'error': 'Missing parameter: date'}), 400
  query = "SELECT * FROM mqtt_data WHERE topic = 'Tunel_1/esp32/TlakVzduchu' AND date > :{date} ORDER BY date ASC"
  data = fetch_data(cursor, query)
  if isinstance(data, dict) and 'error' in data:
    disconnect(conn, cursor)
    return jsonify(data)
  disconnect(conn, cursor)
  return jsonify(data), 200

@app.route('/get/data/wind-speed', methods=['GET'])
def get_wind_speed():
  conn, cursor = connect()
  if conn is None:
    return cursor
  date = request.values.get('date')
  if not date:
    disconnect(conn, cursor)
    return jsonify({'error': 'Missing parameter: date'}), 400
  query = "SELECT * FROM mqtt_data WHERE topic = 'Tunel_1/esp32/AeroVrtula' AND date > :{date} ORDER BY date ASC"
  data = fetch_data(cursor, query)
  if isinstance(data, dict) and 'error' in data:
    disconnect(conn, cursor)
    return jsonify(data)
  disconnect(conn, cursor)
  return jsonify(data), 200

@app.route('/get/data/tenzometer', methods=['GET','POST'])
def get_tenzometer():
  conn, cursor = connect()
  if conn is None:
    return cursor

  if request.method == 'POST':
    try:
      publish.single("Tunel/esp32/SetTare", "SET_TARE", hostname="localhost", port=1983)
      return jsonify({'message': 'Weights are reseting'}), 200
    except Exception as e:
      return jsonify({'error': str(e)}), 500
  elif request.method == 'GET':
    device = request.values.get('device')
    date = request.values.get('date')
    if not device:
      disconnect(conn,cursor)
      return jsonify({'error': 'Missing parameter: device'}), 400
    if not date:
      disconnect(conn,cursor)
      return jsonify({'error': 'Missing parameter: date'}), 400
    if device == 'tenz_y':
      query = "SELECT * FROM mqtt_data WHERE topic = 'Tunel_1/esp32/Tenzometer_os_Y' AND date > :{date} ORDER BY date ASC"
    elif device == 'tenz_x':
      query = "SELECT * FROM mqtt_data WHERE topic = 'Tunel_1/esp32/Tenzometer_os_X' AND date > :{date} ORDER BY date ASC"
    else:
      disconnect(conn, cursor)
      return jsonify({'error': 'Invalid parameter: device'}), 400

    data = fetch_data(cursor, query)
    if isinstance(data, dict) and 'error' in data:
      disconnect(conn, cursor)
      return jsonify(data)
    disconnect(conn, cursor)
    return jsonify(data), 200

  disconnect(conn, cursor)
  return jsonify({"message": "No valid action taken"}), 400

if __name__ == "__main__":
  app.run(debug=True, host='0.0.0.0', port=6789)