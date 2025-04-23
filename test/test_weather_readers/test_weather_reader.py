"""
Test cases for the WeatherReader base class.
"""

import unittest
import datetime
import zoneinfo
from test.factories import create_weather_station
from weather_readers import WeatherReader
from schema import WeatherRecord


class MockWeatherReader(WeatherReader):
    """
    No-functionality weather reader, to be used in unit testing.
    """

    def __init__(self, required_fields=None):
        super().__init__()
        self.required_fields = required_fields if required_fields is not None else []

    def fetch_data(self):
        """
        Mock method to simulate fetching data from a weather station.
        """
        return {"live": {}, "daily": {}}

    def parse(self):
        """
        Mock method to simulate parsing data from a weather station.
        """
        return self.get_fields()


class TestWeatherReader(unittest.TestCase):
    """
    Test cases for the WeatherReader base class.
    """

    def setUp(self):
        """
        Set up the test cases.
        """
        self.base_reader = MockWeatherReader(["field1", "field3"])
        self.station = create_weather_station()

    def test_validate_connection_fields(self):
        """
        Test the validate_connection_fields method.
        """
        reader_requires_all = MockWeatherReader(["field1", "field2", "field3"])

        station_with_all = create_weather_station(
            field1="value1", field2="value2", field3="value3"
        )
        self.assertTrue(
            reader_requires_all.validate_connection_fields(station_with_all)
        )

        station_with_none = create_weather_station(
            field1=None, field2=None, field3=None
        )
        self.assertFalse(
            reader_requires_all.validate_connection_fields(station_with_none)
        )

        station_with_some = create_weather_station(
            field1="value1", field2=None, field3="value3"
        )
        self.assertFalse(
            reader_requires_all.validate_connection_fields(station_with_some)
        )

    def test_get_fields_structure(self):
        """
        Test that the get_fields method returns the correct dictionary structure.
        """
        fields = self.base_reader.get_fields()
        expected_fields = {
            "source_timestamp": None,
            "taken_timestamp": fields[
                "taken_timestamp"
            ],  # Match dynamically generated timestamp
            "live": {
                "temperature": None,
                "wind_speed": None,
                "wind_direction": None,
                "rain": None,
                "humidity": None,
                "pressure": None,
                "wind_gust": None,
            },
            "daily": {
                "max_temperature": None,
                "min_temperature": None,
                "max_wind_speed": None,
                "max_wind_gust": None,
                "cumulative_rain": None,
            },
            "flagged": False,
        }

        # Assert the structure matches
        self.assertEqual(
            fields["source_timestamp"], expected_fields["source_timestamp"]
        )
        self.assertEqual(fields["flagged"], expected_fields["flagged"])
        self.assertEqual(fields["live"], expected_fields["live"])
        self.assertEqual(fields["daily"], expected_fields["daily"])
        self.assertIsInstance(fields["taken_timestamp"], datetime.datetime)
        self.assertEqual(fields["taken_timestamp"].tzinfo, datetime.timezone.utc)

    def test_build_weather_record_meta_fields(self):
        """
        Test the build_weather_record method with meta fields.
        """
        fields = self.base_reader.get_fields()

        fields.update(
            {
                "source_timestamp": datetime.datetime.now(datetime.timezone.utc),
                "taken_timestamp": datetime.datetime.now(datetime.timezone.utc),
                "flagged": False,
            }
        )
        record = self.base_reader.build_weather_record(
            fields=fields, station=self.station
        )
        self.assertIsInstance(record, WeatherRecord)
        self.assertEqual(record.source_timestamp, fields["source_timestamp"])
        self.assertEqual(record.taken_timestamp, fields["taken_timestamp"])
        self.assertEqual(record.flagged, fields["flagged"])
        self.assertIsNone(record.gatherer_thread_id)
        self.assertEqual(record.station_id, self.station.id)

    def test_build_weather_record_live_fields(self):
        """
        Test the build_weather_record method with live fields.
        """
        fields = self.base_reader.get_fields()
        fields["live"].update(
            {
                "temperature": 25.0,
                "wind_speed": 10.0,
                "wind_direction": 180.0,
                "rain": 5.0,
                "humidity": 50.0,
                "pressure": 1013.0,
                "wind_gust": 15.0,
            }
        )
        record = self.base_reader.build_weather_record(
            fields=fields, station=self.station
        )

        self.assertIsInstance(record, WeatherRecord)
        self.assertEqual(record.temperature, fields["live"]["temperature"])
        self.assertEqual(record.wind_speed, fields["live"]["wind_speed"])
        self.assertEqual(record.wind_direction, fields["live"]["wind_direction"])
        self.assertEqual(record.rain, fields["live"]["rain"])
        self.assertEqual(record.humidity, fields["live"]["humidity"])
        self.assertEqual(record.pressure, fields["live"]["pressure"])
        self.assertEqual(record.wind_gust, fields["live"]["wind_gust"])

    def test_build_weather_record_daily_fields(self):
        """
        Test the build_weather_record method with daily fields.
        """
        # Test the build_weather_record method with daily fields
        fields = self.base_reader.get_fields()
        fields["daily"].update(
            {
                "max_temperature": 30.0,
                "min_temperature": 20.0,
                "max_wind_speed": 25.0,
                "max_wind_gust": 35.0,
                "cumulative_rain": 10.0,
            }
        )
        record = self.base_reader.build_weather_record(
            fields=fields, station=self.station, use_daily=True
        )

        self.assertIsInstance(record, WeatherRecord)
        self.assertEqual(record.max_temperature, fields["daily"]["max_temperature"])
        self.assertEqual(record.min_temperature, fields["daily"]["min_temperature"])
        self.assertEqual(record.max_wind_speed, fields["daily"]["max_wind_speed"])
        self.assertEqual(record.max_wind_gust, fields["daily"]["max_wind_gust"])
        self.assertEqual(record.cumulative_rain, fields["daily"]["cumulative_rain"])

        # Test the build_weather_record method with daily fields, but without using daily
        record = self.base_reader.build_weather_record(
            fields=fields, station=self.station, use_daily=False
        )
        self.assertIsInstance(record, WeatherRecord)
        self.assertEqual(record.max_temperature, None)
        self.assertEqual(record.min_temperature, None)
        self.assertEqual(record.max_wind_speed, None)
        self.assertEqual(record.max_wind_gust, None)
        self.assertEqual(record.cumulative_rain, None)

    def test_validate_date_age_utc(self):
        """
        Test the validate_date_age method with UTC timezone.
        """
        # date too old: 31 min ago
        old_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            minutes=35
        )
        result, _ = self.base_reader.validate_date_age(old_date)
        self.assertFalse(result)

        # date in the future
        future_date = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            minutes=5
        )
        result, _ = self.base_reader.validate_date_age(future_date)
        self.assertFalse(result)

        # date old enough: 29 min ago
        new_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            minutes=29
        )
        result, _ = self.base_reader.validate_date_age(new_date)
        self.assertTrue(result)

    def test_validate_date_age_other_timezones(self):
        """
        Test the validate_date_age method with other timezones.
        """
        # date too old: 31 min ago
        old_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            minutes=35
        )
        old_date.astimezone(zoneinfo.ZoneInfo("America/New_York"))
        result, _ = self.base_reader.validate_date_age(old_date)
        self.assertFalse(result)

        # date in the future
        future_date = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            minutes=5
        )
        future_date.astimezone(zoneinfo.ZoneInfo("America/New_York"))
        result, _ = self.base_reader.validate_date_age(future_date)
        self.assertFalse(result)

        # date old enough: 29 min ago
        new_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            minutes=29
        )
        new_date.astimezone(zoneinfo.ZoneInfo("America/New_York"))
        result, _ = self.base_reader.validate_date_age(new_date)
        self.assertTrue(result)

    def test_validate_date_age_no_timezone(self):
        """
        Test the validate_date_age method with naive datetime objects.
        """
        # date too old: 31 min ago
        old_date = datetime.datetime.now() - datetime.timedelta(minutes=35)
        result, _ = self.base_reader.validate_date_age(old_date)
        self.assertFalse(result)

        # date in the future
        future_date = datetime.datetime.now() + datetime.timedelta(minutes=5)
        result, _ = self.base_reader.validate_date_age(future_date)
        self.assertFalse(result)

        # date old enough: 29 min ago
        new_date = datetime.datetime.now() - datetime.timedelta(minutes=29)
        result, _ = self.base_reader.validate_date_age(new_date)
        self.assertFalse(result)


class TestWeatherReaderUtils(unittest.TestCase):
    """
    Testing class for the utility methods of the WeatherReader class.
    """

    def setUp(self):
        """
        Set up the test cases.
        """
        self.base_reader = MockWeatherReader()

    def test_max_or_none(self):
        """
        Test the max_or_none method.
        """
        self.assertEqual(self.base_reader.max_or_none([1, 2, 3]), 3)
        self.assertEqual(self.base_reader.max_or_none([]), None)
        self.assertEqual(self.base_reader.max_or_none(None), None)
        self.assertEqual(self.base_reader.max_or_none([2, 3]), 3)
        self.assertEqual(self.base_reader.max_or_none([-1, 3]), 3)
        self.assertEqual(self.base_reader.max_or_none([3, 3]), 3)

    def test_min_or_none(self):
        """
        Test the min_or_none method.
        """
        self.assertEqual(self.base_reader.min_or_none([1, 2, 3]), 1)
        self.assertEqual(self.base_reader.min_or_none([]), None)
        self.assertEqual(self.base_reader.min_or_none(None), None)
        self.assertEqual(self.base_reader.min_or_none([2, 3]), 2)
        self.assertEqual(self.base_reader.min_or_none([-1, -3]), -3)
        self.assertEqual(self.base_reader.max_or_none([3, 3]), 3)

    def test_coalesce(self):
        """
        Test the coalesce method.
        """
        self.assertEqual(self.base_reader.coalesce([None, None, 3]), 3)
        self.assertEqual(self.base_reader.coalesce([None, 2, 3]), 2)
        self.assertEqual(self.base_reader.coalesce([1, None, 3]), 1)
        self.assertEqual(self.base_reader.coalesce([None, None]), None)
        self.assertEqual(self.base_reader.coalesce([]), None)
        self.assertEqual(self.base_reader.coalesce(None), None)

    def test_smart_azimuth(self):
        """
        Test the smart_azimuth method.
        """
        null_values = [None, "-", "N/A"]
        for value in null_values:
            self.assertIsNone(self.base_reader.smart_azimuth(value))

        # test with valid int and float
        self.assertEqual(self.base_reader.smart_azimuth(0), 0)
        self.assertEqual(self.base_reader.smart_azimuth(360), 0)
        self.assertEqual(self.base_reader.smart_azimuth(180), 180)
        self.assertEqual(self.base_reader.smart_azimuth(90.2), 90)

        # test with invalid int and float
        self.assertIsNone(self.base_reader.smart_azimuth(361))
        self.assertIsNone(self.base_reader.smart_azimuth(-1))

        # test with string values
        in_to_out = {
            "0": 0,
            "360": 0,
            "180": 180,
            "none": None,
            "150 º": 150,
            "SW": 225,
            "No": 315,
            "oSw": 247.5,
            "N": 0,
            "S": 180,
            "128373": None,
            "7a6sgd65": None,
        }

        for in_value, out_value in in_to_out.items():
            self.assertEqual(
                self.base_reader.smart_azimuth(in_value),
                out_value,
                f"Failed for {in_value}",
            )

    def test_safe_float(self):
        """
        Test the safe_float method.
        """
        null_values = [None, "-", "N/A"]
        for value in null_values:
            self.assertIsNone(self.base_reader.safe_float(value))

        # test with valid int and float
        self.assertEqual(self.base_reader.safe_float(0), 0.0)
        self.assertEqual(self.base_reader.safe_float(1.5), 1.5)
        self.assertEqual(self.base_reader.safe_float(3.14), 3.14)

        # test with limit cases
        self.assertIsInstance(self.base_reader.safe_float("NaN"), float)
        self.assertIsInstance(self.base_reader.safe_float("Infinity"), float)
        self.assertIsInstance(self.base_reader.safe_float("-Infinity"), float)

        # test with string values
        in_to_out = {
            "0": 0.0,
            "1.5": 1.5,
            "3.14": 3.14,
            "none": None,
            "3.14e10": 31400000000.0,
        }
        for in_value, out_value in in_to_out.items():
            self.assertEqual(
                self.base_reader.safe_float(in_value),
                out_value,
                f"Failed for {in_value}",
            )

    def test_safe_int(self):
        """
        Test the safe_int method.
        """
        null_values = [None, "-", "N/A"]
        for value in null_values:
            self.assertIsNone(self.base_reader.safe_int(value))

        # test with valid int and float
        self.assertEqual(self.base_reader.safe_int(0), 0)
        self.assertEqual(self.base_reader.safe_int(1.5), 1)
        self.assertEqual(self.base_reader.safe_int(3.14), 3)

        # test with limit cases
        self.assertIsNone(self.base_reader.safe_int("NaN"), int)
        self.assertIsNone(self.base_reader.safe_int("Infinity"), int)
        self.assertIsNone(self.base_reader.safe_int("-Infinity"), int)

        # test with string values
        in_to_out = {
            "0": 0,
            "1.5": None,
            "3.14": None,
            "none": None,
            "312": 312,
            "-5": -5,
        }

        for in_value, out_value in in_to_out.items():
            self.assertEqual(
                self.base_reader.safe_int(in_value), out_value, f"Failed for {in_value}"
            )

    def test_smart_parse_datetime_simple(self):
        """
        Test the smart_parse_datetime method.
        Needs to correcty get dates on the following formats:
            ["%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M", "%d/%m/%y %H:%M"]
        and the default parser from datetime.
        """

        safe_date_to_test = datetime.datetime(
            2000, 6, 22, 12, 30, 0
        )  # cannot confuse 22/06 with 06/22

        as_format_1 = safe_date_to_test.strftime("%d/%m/%Y %H:%M")
        as_format_2 = safe_date_to_test.strftime("%d-%m-%Y %H:%M")
        as_format_3 = safe_date_to_test.strftime("%d/%m/%y %H:%M")
        as_format_4 = safe_date_to_test.strftime("%Y-%m-%d %H:%M:%S.%f")

        parsed_1 = self.base_reader.smart_parse_datetime(as_format_1)
        parsed_2 = self.base_reader.smart_parse_datetime(as_format_2)
        parsed_3 = self.base_reader.smart_parse_datetime(as_format_3)
        parsed_4 = self.base_reader.smart_parse_datetime(as_format_4)

        self.assertEqual(parsed_1, safe_date_to_test)
        self.assertEqual(parsed_2, safe_date_to_test)
        self.assertEqual(parsed_3, safe_date_to_test)
        self.assertEqual(parsed_4, safe_date_to_test)

    def test_smart_parse_datetime_ambiguous(self):
        """
        Test the smart_parse_datetime method for ambiguous dates.

        On cases like "12/03/2023 15:30", it is not clear what number
        is the month and which is the day. In this case, the parser function
        should pick the closest date to the current date.
        """

        ambiguous_dates = ["12/03/2020 15:30", "03/12/2020 15:30"]

        for date_str in ambiguous_dates:
            parsed_date = self.base_reader.smart_parse_datetime(date_str)
            self.assertIsInstance(parsed_date, datetime.datetime)
            self.assertEqual(parsed_date.day, 3)
            self.assertEqual(parsed_date.month, 12)
            self.assertEqual(parsed_date.year, 2020)

    def test_smart_parse_datetime_future(self):
        """
        Test the smart_parse_datetime method for future dates.

        If the date is in the future, it should return None.
        """

        date = datetime.datetime.now(datetime.timezone.utc)

        date_str = date.strftime("%d/%m/%Y %H:%M")
        parsed_date = self.base_reader.smart_parse_datetime(date_str)
        self.assertEqual(parsed_date.year, date.year)
        self.assertEqual(parsed_date.month, date.month)
        self.assertEqual(parsed_date.day, date.day)
        self.assertEqual(parsed_date.hour, date.hour)
        self.assertEqual(parsed_date.minute, date.minute)

    def test_smart_parse_datetime_invalid_format(self):
        """
        Test the smart_parse_datetime method for invalid formats.
        """

        invalid_dates = ["12/03/2020", "03-12-2020", "aaa"]

        for date_str in invalid_dates:
            with self.assertRaises(
                ValueError, msg=f"Failed to raise ValueError for {date_str}"
            ):
                self.base_reader.smart_parse_datetime(date_str)
