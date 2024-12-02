from flask import Flask, jsonify, request
import psycopg2
from paho.mqtt import publish
import os

DATABASE = os.environ.get('DATABASE')
USER = os.environ.get('USER')
POSTGRES_HOST = os.environ.get('POSTGRES_HOST')
POSTGRES_PORT = os.environ.get('POSTGRES_PORT')
POSTGRES_PASS = os.environ.get('POSTGRES_PASS')

app = Flask(__name__)

def connect():
  try:
    conn = psycopg2.connect(database=DATABASE, user=USER, password=POSTGRES_PASS, host=POSTGRES_HOST, port=POSTGRES_PORT)
    cursor = conn.cursor()
    return conn, cursor
  except psycopg2.Error as e:
    return None, jsonify({'error': str(e), 'code': 500})

def disconnect(connection,cursor):
  cursor.close()
  connection.close()

def fetch_data(cursor, data1, data2):
  query = "SELECT * FROM mqtt_data WHERE topic = %s AND date > %s ORDER BY date ASC"
  try:
    cursor.execute(query, (data1, data2))
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
  data = fetch_data(cursor, 'Tunel_1/esp32/TeplotaVzduchu', date)
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
  data = fetch_data(cursor, 'Tunel_1/esp32/VlhkostVzduchu', date)
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
  data = fetch_data(cursor, 'Tunel_1/esp32/TlakVzduchu', date)
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
  data = fetch_data(cursor, 'Tunel_1/esp32/AeroVrtula', date)
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
      data1 = 'Tunel_1/esp32/Tenzometer_os_Y'
    elif device == 'tenz_x':
      data1 = 'Tunel_1/esp32/Tenzometer_os_X'
    else:
      disconnect(conn, cursor)
      return jsonify({'error': 'Invalid parameter: device'}), 400

    data = fetch_data(cursor, data1, date)
    if isinstance(data, dict) and 'error' in data:
      disconnect(conn, cursor)
      return jsonify(data)
    disconnect(conn, cursor)
    return jsonify(data), 200

  disconnect(conn, cursor)
  return jsonify({"message": "No valid action taken"}), 400

if __name__ == "__main__":
  app.run(debug=True, host='0.0.0.0', port=8080)