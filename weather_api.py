from flask import Flask, jsonify, Response, make_response
from typing import List
from pydantic import BaseModel
from mysql.connector import connection
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd
from pandas import Timestamp, DataFrame
import os

app = Flask(__name__)
OFFSET_HOURS = -12

def zero_time(year, month, day) -> Timestamp:
    return pd.Timestamp(year, month, day, hour=0, minute=0, second=0, microsecond=0, tz=None)

def get_actuals_from_weather(weather: DataFrame) -> DataFrame:
	weather.loc[:, 'time'] = weather['time'].map(lambda x: string_to_date_plus_hours(x, OFFSET_HOURS))
	actuals = weather[(weather['days_out'] == 0) & (weather['time'] > (weather['time'] - pd.Timedelta(days=30)))]
	aggs = actuals.groupby('time').agg({'low': 'mean', 'high': 'mean', 'weather': 'mean'}).reset_index()
	return aggs

def string_to_date_plus_hours(date: str, hours_to_add) -> datetime:
    date_time = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    date_minus_hours = date_time + timedelta(hours=hours_to_add)
    date_minus_hours_no_time = date_minus_hours.replace(hour=0, minute=0, second=0, microsecond=0)
    return date_minus_hours_no_time

def add_actuals_deltas(actuals_data: DataFrame, weather_data: DataFrame) -> DataFrame:
    weather_data['forecasted_date'] = weather_data.apply(get_forecast_date, axis=1)
    merged = DataFrame.merge(actuals_data, weather_data, left_on='time', right_on='forecasted_date', suffixes=('_actual','_forecasted'))
    merged_with_deltas = get_data_with_deltas(merged)
    return merged_with_deltas

def get_data_with_deltas(df: DataFrame) -> DataFrame:
    df_with_deltas = df.copy()
    df_with_deltas['low_delta'] = abs(df_with_deltas['low_actual'] - df_with_deltas['low_forecasted'])
    df_with_deltas['high_delta'] = abs(df_with_deltas['high_actual'] - df_with_deltas['high_forecasted'])
    df_with_deltas['weather_delta'] = abs(df_with_deltas['weather_actual'] - df_with_deltas['weather_forecasted'])
    return df_with_deltas

def get_forecast_date(row) -> datetime:
    return row['time'] + timedelta(days=row['days_out'])

def get_weather_deltas_report():
    weather_dump = get_mysql_results("SELECT id, low, high, weather, channel, DATE_FORMAT(time - INTERVAL 12 HOUR, '%Y-%m-%d 00:00:00') as time, days_out FROM weather ORDER BY time DESC, days_out ASC")
    weather_list = list(map(map_sql_result_to_list, weather_dump))
    weather_df = DataFrame(weather_list, columns=['id', 'high', 'low', 'weather', 'channel', 'time', 'days_out'])
    actuals = get_actuals_from_weather(weather_df)
    weather_with_deltas = add_actuals_deltas(actuals, weather_df)
    deltas_by_channel_days_out = weather_with_deltas.groupby(['channel', 'days_out']).agg({'low_delta': 'mean', 'high_delta': 'mean', 'weather_delta': 'mean'}).reset_index()
    return deltas_by_channel_days_out.to_dict(orient='records')

def map_sql_result_to_list(row: dict):
    weather = row['weather']
    default = 1
    if weather == 'Sun':
        default = 1
    elif weather == 'Clouds':
        default = 2
    elif weather == 'Rain':
        default = 3
    row['weather'] = default
    return list(row.values())

@app.route('/')
def get_sql_report():
    results = get_weather_deltas_report()
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
        use_pure=True,
        unix_socket=None
    )
    cursor = conn.cursor(dictionary=True)
    cursor.execute(sql)
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return result


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5555)