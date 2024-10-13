from flask import Flask, jsonify, request
import psycopg2

app = Flask(__name__)

@app.route('/get/data', methods=['GET'])
def get_data():
  conn = psycopg2.connect(database='postgres', user='postgres', password='supersecretpassword', host='127.0.0.1', port='5433')
  cursor = conn.cursor()
  cursor.execute('SELECT * FROM data')
