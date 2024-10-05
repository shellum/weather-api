import unittest
from weather_api import get_actuals_from_weather, string_to_date_plus_hours, add_actuals_deltas, get_forecast_date, zero_time, get_data_with_deltas, map_sql_result_to_list
from pandas import DataFrame
from datetime import datetime
from collections import defaultdict
import pandas as pd

class TestWeatherDataProcessing(unittest.TestCase):

    def test_string_to_date_plus_hours(self):
        hours_to_add = -12
        date = '2024-09-07 05:05:11'
        expected = datetime.strptime('2024-09-06', "%Y-%m-%d")
        actual = string_to_date_plus_hours(date, hours_to_add)
        self.assertEqual(actual, expected)

    def test_get_actuals_from_weather(self):
        weather_data = [
            [1,  64, 94, 1,    'chan2', '2024-09-07 05:05:11', 0],
            [2,  66, 94, 1,    'chan2', '2024-09-07 05:05:12', 1],
            [3,  68, 92, 3,   'chan2', '2024-09-07 05:05:13', 2],
            [4,  66, 90, 3,   'chan2', '2024-09-07 05:05:14', 3],
            [5,  63, 88, 3,   'chan2', '2024-09-07 05:05:15', 4],
            [6,  61, 86, 3,   'chan2', '2024-09-07 05:05:16', 5],
            [7,  53, 74, 3,   'chan2', '2024-09-07 05:05:16', 6],
            [8,  63, 93, 2, 'chan5', '2024-09-07 05:05:17', 0],
            [9,  66, 92, 2, 'chan5', '2024-09-07 05:05:18', 1],
            [10, 68, 91, 2, 'chan5', '2024-09-07 05:05:19', 2]
        ]
        expected = [
            [zero_time(2024, 9, 6), 63.5, 93.5, 1.5],
        ]
        weather_df = DataFrame(weather_data, columns=['id', 'low', 'high', 'weather', 'channel', 'time', 'days_out'])
        actual_df = get_actuals_from_weather(weather_df)
        actual = actual_df.values.tolist()
        self.assertListEqual(actual, expected)

    def test_get_forecast_date(self):
        weather_data = [
            [1,  64, 94, 'Sun',    'chan2', datetime.strptime('2024-09-07', "%Y-%m-%d"), 0],
            [2,  66, 94, 'Sun',    'chan2', datetime.strptime('2024-09-07', "%Y-%m-%d"), 1],
            [3,  68, 92, 'Rain',   'chan2', datetime.strptime('2024-09-07', "%Y-%m-%d"), 2]
        ]
        weather_df = DataFrame(weather_data, columns=['id', 'low', 'high', 'weather', 'channel', 'time', 'days_out'])
        expected = [
            [1,  64, 94, 'Sun',  'chan2', zero_time(2024, 9, 7), 0, pd.Timestamp(2024, 9, 7, hour=0, minute=0, second=0, microsecond=0, tz=None)],
            [2,  66, 94, 'Sun',  'chan2', zero_time(2024, 9, 7), 1, pd.Timestamp(2024, 9, 8, hour=0, minute=0, second=0, microsecond=0, tz=None)],
            [3,  68, 92, 'Rain', 'chan2', zero_time(2024, 9, 7), 2, pd.Timestamp(2024, 9, 9, hour=0, minute=0, second=0, microsecond=0, tz=None)],
        ]
        weather_df['forecasted_date'] = weather_df.apply(get_forecast_date, axis=1)
        actual = weather_df.values.tolist()
        self.assertListEqual(actual, expected)

    def test_add_actuals_deltas(self):
        weather_data = [
            [1,  64, 94, 1,    'chan2', zero_time(2024, 9, 7), 0],
            [2,  66, 94, 1,    'chan2', zero_time(2024, 9, 7), 1],
            [3,  68, 92, 3,   'chan2', zero_time(2024, 9, 7), 2],
            [4,  66, 90, 3,   'chan2', zero_time(2024, 9, 7), 3],
            [5,  63, 88, 3,   'chan2', zero_time(2024, 9, 7), 4],
            [6,  61, 86, 3,   'chan2', zero_time(2024, 9, 7), 5],
            [7,  53, 74, 3,   'chan2', zero_time(2024, 9, 7), 6],
            [8,  63, 93, 2, 'chan5', zero_time(2024, 9, 7), 0],
            [9,  66, 92, 2, 'chan5', zero_time(2024, 9, 7), 1],
            [10, 68, 91, 2, 'chan5', zero_time(2024, 9, 7), 2]
        ]
        real_data = [
            [datetime.strptime('2024-09-08', "%Y-%m-%d"), 63.5, 93.5, 1.5],
        ]
        expected = [
            [zero_time(2024, 9, 8), 63.5, 93.5, 1.5, 2, 66, 94, 1, 'chan2', zero_time(2024, 9, 7), 1, zero_time(2024, 9, 8), 2.5, 0.5, 0.5],
            [zero_time(2024, 9, 8), 63.5, 93.5, 1.5, 9, 66, 92, 2, 'chan5', zero_time(2024, 9, 7), 1, zero_time(2024, 9, 8), 2.5, 1.5, 0.5]
        ]
        weather_df = DataFrame(weather_data, columns=['id', 'low', 'high', 'weather', 'channel', 'time', 'days_out'])
        real_df = DataFrame(real_data, columns=['time', 'low', 'high', 'weather'])
        actual = add_actuals_deltas(real_df, weather_df)
        actual_list = actual.values.tolist()
        self.assertListEqual(actual_list, expected)

    def test_get_data_with_deltas(self):
        initial_data = DataFrame([[10, 12, 55, 60, 2, 1],[21, 19, 40, 41, 2, 2]], columns=['low_actual', 'low_forecasted', 'high_actual', 'high_forecasted', 'weather_actual', 'weather_forecasted'])
        expected = [
            [10, 12, 55, 60, 2, 1, 2, 5, 1],
            [21, 19, 40, 41, 2, 2, 2, 1, 0]
        ]
        actuals = get_data_with_deltas(initial_data)
        actuals_data = actuals.values.tolist()
        self.assertListEqual(actuals_data, expected)

    def test_map_sql_result_to_list(self):
        sql_result = [{'weather':'Sun', 'low':23}, {'weather':'Rain', 'low':40}]
        results = list(map(map_sql_result_to_list, sql_result))
        expected = [[1, 23], [3, 40]]
        self.assertListEqual(results, expected)

unittest.main()