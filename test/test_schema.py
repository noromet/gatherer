# """
# Test cases for the schema.py classes.
# """

# import unittest
# from test.factories import create_weather_record


# class TestWeatherRecord(unittest.TestCase):
#     """
#     Test cases for the WeatherRecord class.
#     """

#     def test_sanity_check_valid_data(self):
#         """
#         Test the sanity check method with valid data.
#         """
#         record = create_weather_record()
#         record.sanity_check()
#         assert not record.flagged

#     def test_apply_pressure_offset(self):
#         """
#         Test the apply_pressure_offset method with different offsets.
#         """
#         record_to_sum = create_weather_record(pressure=1013.0)
#         record_to_sum.apply_pressure_offset(5.0)
#         assert record_to_sum.pressure == 1018.0

#         record_to_subtract = create_weather_record(pressure=1013.0)
#         record_to_subtract.apply_pressure_offset(-5.0)
#         assert record_to_subtract.pressure == 1008.0

#         record_to_zero = create_weather_record(pressure=1013.0)
#         record_to_zero.apply_pressure_offset(0.0)
#         assert record_to_zero.pressure == 1013.0

#     def test_apply_rounding(self):
#         """
#         Test the apply_rounding method with different decimal places.
#         """
#         record = create_weather_record(
#             temperature=25.123,
#             wind_speed=10.456,
#             max_wind_speed=20.789,
#             wind_direction=180.0,
#             rain=5.678,
#             humidity=50.123,
#             pressure=1013.456,
#             cumulative_rain=10.987,
#             max_temperature=30.654,
#             min_temperature=20.321,
#             wind_gust=15.432,
#             max_wind_gust=25.876,
#         )
#         record.apply_rounding(decimals=1)
#         assert record.temperature == 25.1
#         assert record.wind_speed == 10.5
#         assert record.max_wind_speed == 20.8
#         assert record.rain == 5.7
#         assert record.humidity == 50.1
#         assert record.pressure == 1013.5
#         assert record.cumulative_rain == 11.0
#         assert record.max_temperature == 30.7
#         assert record.min_temperature == 20.3
#         assert record.wind_gust == 15.4
#         assert record.max_wind_gust == 25.9


# class TestWeatherRecordSanity(unittest.TestCase):
#     """
#     Test cases for the WeatherRecord class sanity checks.
#     """

#     def test_temperature_sanity(self):
#         """
#         Test the temperature sanity check.
#         """
#         record_temp_below_minimum = create_weather_record(
#             temperature=-40.0,
#             max_temperature=30.0,
#             min_temperature=20.0,
#         )
#         record_temp_below_minimum.sanity_check()
#         assert record_temp_below_minimum.flagged
#         assert record_temp_below_minimum.temperature is None

#         record_temp_above_maximum = create_weather_record(
#             temperature=51.0,
#             max_temperature=30.0,
#             min_temperature=20.0,
#         )
#         record_temp_above_maximum.sanity_check()
#         assert record_temp_above_maximum.flagged
#         assert record_temp_above_maximum.temperature is None

#         record_temp_valid = create_weather_record(
#             temperature=25.0,
#             max_temperature=30.0,
#             min_temperature=20.0,
#         )
#         record_temp_valid.sanity_check()
#         assert not record_temp_valid.flagged
#         assert record_temp_valid.temperature == 25.0

#     def test_wind_speed_sanity(self):
#         """
#         Test the wind speed sanity check.
#         """
#         record_wind_speed_below_minimum = create_weather_record(wind_speed=-1.0)
#         record_wind_speed_below_minimum.sanity_check()
#         assert record_wind_speed_below_minimum.flagged
#         assert record_wind_speed_below_minimum.wind_speed is None

#         record_wind_speed_above_maximum = create_weather_record(wind_speed=501.0)
#         record_wind_speed_above_maximum.sanity_check()
#         assert record_wind_speed_above_maximum.flagged
#         assert record_wind_speed_above_maximum.wind_speed is None

#         record_wind_speed_valid = create_weather_record(wind_speed=250.0)
#         record_wind_speed_valid.sanity_check()
#         assert not record_wind_speed_valid.flagged
#         assert record_wind_speed_valid.wind_speed == 250.0

#     def test_humidity_sanity(self):
#         """
#         Test the humidity sanity check.
#         """
#         record_humidity_below_minimum = create_weather_record(humidity=-1.0)
#         record_humidity_below_minimum.sanity_check()
#         assert record_humidity_below_minimum.flagged
#         assert record_humidity_below_minimum.humidity is None

#         record_humidity_above_maximum = create_weather_record(humidity=101.0)
#         record_humidity_above_maximum.sanity_check()
#         assert record_humidity_above_maximum.flagged
#         assert record_humidity_above_maximum.humidity is None

#         record_humidity_valid = create_weather_record(humidity=50.0)
#         record_humidity_valid.sanity_check()
#         assert not record_humidity_valid.flagged
#         assert record_humidity_valid.humidity == 50.0

#     def test_pressure_sanity(self):
#         """
#         Test the pressure sanity check.
#         """
#         record_pressure_below_minimum = create_weather_record(pressure=799.0)
#         record_pressure_below_minimum.sanity_check()
#         assert record_pressure_below_minimum.flagged
#         assert record_pressure_below_minimum.pressure is None

#         record_pressure_above_maximum = create_weather_record(pressure=1101.0)
#         record_pressure_above_maximum.sanity_check()
#         assert record_pressure_above_maximum.flagged
#         assert record_pressure_above_maximum.pressure is None

#         record_pressure_valid = create_weather_record(pressure=1000.0)
#         record_pressure_valid.sanity_check()
#         assert not record_pressure_valid.flagged
#         assert record_pressure_valid.pressure == 1000.0

#     def test_wind_direction_sanity(self):
#         """
#         Test the wind direction sanity check.
#         """
#         record_wind_direction_below_minimum = create_weather_record(wind_direction=-1.0)
#         record_wind_direction_below_minimum.sanity_check()
#         assert record_wind_direction_below_minimum.flagged
#         assert record_wind_direction_below_minimum.wind_direction is None

#         record_wind_direction_above_maximum = create_weather_record(
#             wind_direction=361.0
#         )
#         record_wind_direction_above_maximum.sanity_check()
#         assert record_wind_direction_above_maximum.flagged
#         assert record_wind_direction_above_maximum.wind_direction is None

#         record_wind_direction_valid = create_weather_record(wind_direction=180.0)
#         record_wind_direction_valid.sanity_check()
#         assert not record_wind_direction_valid.flagged
#         assert record_wind_direction_valid.wind_direction == 180.0

#     def test_rain_sanity(self):
#         """
#         Test the rain sanity check.
#         """
#         record_rain_below_minimum = create_weather_record(rain=-1.0)
#         record_rain_below_minimum.sanity_check()
#         assert record_rain_below_minimum.flagged
#         assert record_rain_below_minimum.rain is None

#         record_rain_above_maximum = create_weather_record(rain=501.0)
#         record_rain_above_maximum.sanity_check()
#         assert record_rain_above_maximum.flagged
#         assert record_rain_above_maximum.rain is None

#         record_rain_valid = create_weather_record(rain=250.0)
#         record_rain_valid.sanity_check()
#         assert not record_rain_valid.flagged
#         assert record_rain_valid.rain == 250.0

#     def test_cumulative_rain_sanity(self):
#         """
#         Test the cumulative rain sanity check.
#         """
#         record_cumulative_rain_below_minimum = create_weather_record(
#             cumulative_rain=-1.0
#         )
#         record_cumulative_rain_below_minimum.sanity_check()
#         assert record_cumulative_rain_below_minimum.flagged
#         assert record_cumulative_rain_below_minimum.cumulative_rain is None

#         record_cumulative_rain_above_maximum = create_weather_record(
#             cumulative_rain=15001.0
#         )
#         record_cumulative_rain_above_maximum.sanity_check()
#         assert record_cumulative_rain_above_maximum.flagged
#         assert record_cumulative_rain_above_maximum.cumulative_rain is None

#         record_cumulative_rain_valid = create_weather_record(cumulative_rain=10000.0)
#         record_cumulative_rain_valid.sanity_check()
#         assert not record_cumulative_rain_valid.flagged
#         assert record_cumulative_rain_valid.cumulative_rain == 10000.0
