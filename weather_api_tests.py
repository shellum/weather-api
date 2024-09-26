import unittest
import weather_api
from collections import defaultdict

class TestWeatherDataProcessing(unittest.TestCase):
    def test_list_to_dict_by_key(self):
        weather_data = [
            (1, 50, 60, 'Sun', 'chan1', '2021-01-01'),
            (2, 51, 61, 'Sun', 'chan2', '2021-01-01'),
            (3, 52, 62, 'Sun', 'chan1', '2021-01-01'),
            (4, 53, 63, 'Sun', 'chan3', '2021-01-01'),
            (5, 54, 64, 'Sun', 'chan3', '2021-01-01'),
            (6, 55, 65, 'Sun', 'chan2', '2021-01-01'),
        ]
        expected_data = defaultdict()
        expected_data['chan1'] = [
                (1, 50, 60, 'Sun', 'chan1', '2021-01-01'),
                (3, 52, 62, 'Sun', 'chan1', '2021-01-01')
            ]
        expected_data['chan2'] = [
                (2, 51, 61, 'Sun', 'chan2', '2021-01-01'),
                (6, 55, 65, 'Sun', 'chan2', '2021-01-01')
            ]
        expected_data['chan3'] = [
                (4, 53, 63, 'Sun', 'chan3', '2021-01-01'),
                (5, 54, 64, 'Sun', 'chan3', '2021-01-01')
            ]
        actual_data = weather_api.list_to_dict_by_key(weather_data, 4)
        self.assertDictEqual(actual_data, expected_data)

    def test_recursive_list_to_dict_by_key(self):
        weather_data = [
            (1, 50, 60, 'Sun', 'chan1', '2021-01-01', 1),
            (2, 51, 61, 'Sun', 'chan2', '2021-01-01', 1),
            (3, 52, 62, 'Sun', 'chan1', '2021-01-01', 2),
            (4, 53, 63, 'Sun', 'chan3', '2021-01-01', 1),
            (5, 54, 64, 'Sun', 'chan3', '2021-01-01', 2),
            (6, 55, 65, 'Sun', 'chan2', '2021-01-01', 2),
            (7, 56, 66, 'Sun', 'chan1', '2021-01-02', 1),
            (8, 57, 67, 'Sun', 'chan2', '2021-01-02', 1),
            (9, 58, 68, 'Sun', 'chan1', '2021-01-02', 2)
        ]
        expected_data = defaultdict()
        expected_data['chan1'] = {
             '2021-01-01': [
                (1, 50, 60, 'Sun', 'chan1', '2021-01-01', 1),
                (3, 52, 62, 'Sun', 'chan1', '2021-01-01', 2)
             ],
             '2021-01-02': [
                (7, 56, 66, 'Sun', 'chan1', '2021-01-02', 1),
                (9, 58, 68, 'Sun', 'chan1', '2021-01-02', 2)
             ]
        }
        expected_data['chan2'] = {
             '2021-01-01': [
                (2, 51, 61, 'Sun', 'chan2', '2021-01-01', 1),
                (6, 55, 65, 'Sun', 'chan2', '2021-01-01', 2)
             ],
             '2021-01-02': [
                (8, 57, 67, 'Sun', 'chan2', '2021-01-02', 1)
             ]
        }
        expected_data['chan3'] = {
             '2021-01-01': [
                (4, 53, 63, 'Sun', 'chan3', '2021-01-01', 1),
                (5, 54, 64, 'Sun', 'chan3', '2021-01-01', 2)
             ]
        }
        actual_data = weather_api.recursive_list_to_dict_by_key(weather_data, 4, 5)
        self.assertDictEqual(actual_data, expected_data)

    def test_colocate_forecasts_with_actuals(self):
        data = dict()
        data['chan1'] = {
             '2021-01-01': [
                (1, 50, 60, 'Sun', 'chan1', '2021-01-01', 0),
                (3, 52, 62, 'Sun', 'chan1', '2021-01-01', 1)
             ],
             '2021-01-02': [
                (7, 56, 66, 'Sun', 'chan1', '2021-01-02', 0),
                (9, 58, 68, 'Sun', 'chan1', '2021-01-02', 1)
             ]
        }
        data['chan2'] = {
             '2021-01-01': [
                (2, 51, 61, 'Sun', 'chan2', '2021-01-01', 0),
                (6, 55, 65, 'Sun', 'chan2', '2021-01-01', 1)
             ],
             '2021-01-02': [
                (8, 57, 67, 'Sun', 'chan2', '2021-01-02', 3)
             ]
        }
        expected_data = dict()
        expected_data['2024-01-01'] = {
            '0': {
                'actual': {
                    'low': 50,
                    'high': 60,
                    'weather': 'Sun',
                    'reports': 2
                },
                'forecast': [
                ]
            },
            '1': {
                'actual': {
                    'low': 51,
                    'high': 61,
                    'weather': 'Sun'
                },
                'forecast': [
                ]
            }
        }
        expected_data['2024-01-02'] = {
            'chan1': {
                '1': {
                    'actual': {
                        'low': 56,
                        'high': 66,
                        'weather': 'Sun'
                    },
                    'forecast': [
                        {'low': 52, 'high': 60, 'weather': 'Sun', 'channel': 'chan1'},
                        {'low': 51, 'high': 61, 'weather': 'Sun', 'channel': 'chan2'}
                    ]
                }

            }
        }

unittest.main()