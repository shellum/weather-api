from flask import Flask, jsonify, Response, make_response
from typing import List
from pydantic import BaseModel
from mysql.connector import connection
from collections import defaultdict
import os

app = Flask(__name__)

@app.route('/')
def get_sql_report():
    sql = """with actuals AS (
SELECT
	AVG(low) as low,
	AVG(high) as high,
	AVG(CASE weather 
		WHEN 'Sun' THEN 1
		WHEN 'Clouds' THEN 2
		WHEN 'Rain' THEN 3
		ELSE 5
	END) as weather_num,
	DATE_FORMAT(time - INTERVAL 12 HOUR, '%Y-%m-%d') as actual_date
FROM
	weather
WHERE 
	days_out = 0 AND
	time > (time - INTERVAL 30 DAY)
GROUP BY
	actual_date, days_out
), forecasted_and_actuals as (SELECT
	weather.low,
	weather.high,
	weather.weather,
	CASE weather 
		WHEN 'Sun' THEN 1
		WHEN 'Clouds' THEN 2
		WHEN 'Rain' THEN 3
		ELSE 5
	END as weather_num,
	weather.channel,
	DATE_FORMAT(weather.time - INTERVAL 12 HOUR, '%Y-%m-%d') as save_date,
	days_out,
	DATE_FORMAT(weather.time - INTERVAL 12 HOUR + INTERVAL weather.days_out DAY, '%Y-%m-%d') as forecasted_date,
	actuals.low as actual_low,
	actuals.high as actual_high,
	actuals.weather_num as actual_weather
FROM
	weather
INNER JOIN
	actuals
ON
    DATE_FORMAT(weather.time - INTERVAL 12 HOUR + INTERVAL weather.days_out DAY, '%Y-%m-%d') = actuals.actual_date
ORDER BY
	time DESC,
	days_out ASC), deltas as (SELECT
	ABS(low - actual_low) as degrees_delta_low,
	ABS(high - actual_high) as degrees_delta_high,
	ABS(weather_num - actual_weather) as delta_weather,
	days_out,
	channel
FROM forecasted_and_actuals) SELECT
	ROUND(AVG(degrees_delta_low), 1) delta_high,
	ROUND(AVG(degrees_delta_high), 1) delta_low,
	ROUND(AVG(delta_weather), 1) delta_weather,
	MAX(degrees_delta_low) as max_delta_high,
	MAX(degrees_delta_high) as max_delta_low,
	MAX(delta_weather) as max_delta_weather,
	days_out,
	channel,
    ROW_NUMBER() OVER (ORDER BY channel DESC) as rownum
FROM
	deltas
GROUP BY
	channel, days_out
ORDER BY
	days_out ASC, channel ASC"""
    results = get_mysql_results(sql)
    response = jsonify(results)
    response.headers['Access-Control-Allow-Origin'] = '*'
    return (response)

@app.route('/history')
def history():
    results = get_mysql_results("SELECT id, low, high, weather, channel, DATE_FORMAT(time - INTERVAL 12 HOUR, '%Y-%m-%d') as time, days_out FROM weather ORDER BY time DESC, days_out ASC")
    channel_index = 4
    time_index = 5
    weather_by_channel = recursive_list_to_dict_by_key(results, channel_index, time_index)
    response = make_response(weather_by_channel)
    response.headers['Access-Control-Allow-Origin'] = '*'
    return dict(response)

def recursive_list_to_dict_by_key(data: List, key_index: int, sub_key_index: int) -> dict:
    single_dict = list_to_dict_by_key(data, key_index)
    double_dict = dict()
    for key in single_dict:
        double_dict[key] = list_to_dict_by_key(single_dict[key], sub_key_index)
    return double_dict

def list_to_dict_by_key(data: List, key_index: int) -> dict:
    results = defaultdict()
    for item in data:
        channel = item[key_index]
        if (results.get(channel) == None):
            results[channel] = list()
        results[channel].append(item)
    return results

def get_mysql_results(sql: str) -> Response:
    conn = connection.MySQLConnection(
        host=os.getenv('MYSQL_HOST', '192.168.3.251'),
        port=os.getenv('MYSQL_PORT', '3306'),
        user=os.getenv('MYSQL_USER', ''),
        password=os.getenv('MYSQL_PASS', ''),
        database=os.getenv('MYSQL_DB', 'weather'),
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5555)