"""
Base classes for test cases.
"""

import json
import os
import unittest
import datetime
from unittest import mock
from typing import Dict, Any


class WeatherReaderTestBase(unittest.TestCase):
    """
    Base class for weather reader tests that need to load API response fixtures.

    Child classes should define:
    - live_fixture_filename: name of the live fixture file
        to load (e.g., 'ecowitt_live_response.json')
    - daily_fixture_filename: name of the daily fixture file
        to load (optional, e.g., 'ecowitt_daily_response.json')
    """

    live_fixture_filename = None
    daily_fixture_filename = None

    def setUp(self):
        """
        Set up test fixtures by loading sample data from files.
        Fails the test if the required files don't exist or cannot be read.
        """
        if self.live_fixture_filename is None:
            self.fail("Child test class must define live_fixture_filename")

        # Initialize test data dictionary
        self.test_data = {}

        # Define path to test fixtures
        self.fixtures_dir = self._get_fixtures_directory()

        # Load live data
        self.test_data["live"] = self._load_fixture(self.live_fixture_filename)

        # Load daily data if specified
        if self.daily_fixture_filename:
            self.test_data["daily"] = self._load_fixture(self.daily_fixture_filename)

        # Set up datetime mocking
        self._setup_datetime_mock()

    def _get_fixtures_directory(self) -> str:
        """
        Get the path to the fixtures directory and ensure it exists.

        Returns:
            str: Path to the fixtures directory
        """
        fixtures_dir = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "test_weather_readers/responses"
        )

        # Ensure the fixtures directory exists
        os.makedirs(fixtures_dir, exist_ok=True)
        return fixtures_dir

    def _load_fixture(self, filename: str) -> Any:
        """
        Load fixture data from a file based on its extension.

        Args:
            filename: Name of the fixture file to load

        Returns:
            The loaded fixture data

        Raises:
            unittest.TestCase.failureException: If the file doesn't exist or can't be read properly
        """
        file_path = os.path.join(self.fixtures_dir, filename)
        if not os.path.exists(file_path):
            self.fail(f"Test fixture file not found: {file_path}")

        _, file_ext = os.path.splitext(filename.lower())

        try:
            if file_ext == ".json":
                return self._load_json_fixture(file_path)
            return self._load_text_fixture(file_path)
        except Exception as e:
            self.fail(f"Failed to read test fixture file: {file_path}. Error: {str(e)}")

        return None

    def _load_json_fixture(self, file_path: str) -> Dict[str, Any]:
        """
        Load and parse a JSON fixture file.

        Args:
            file_path: Path to the JSON file

        Returns:
            Dict: Parsed JSON data

        Raises:
            json.JSONDecodeError: If the file contains invalid JSON
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            self.fail(f"Invalid JSON in test fixture file: {file_path}")

        return None

    def _load_text_fixture(self, file_path: str) -> str:
        """
        Load a plain text fixture file.

        Args:
            file_path: Path to the text file

        Returns:
            str: Text content of the file
        """
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()

    def _setup_datetime_mock(self) -> None:
        """
        Set up a mock for datetime.datetime to return a fixed timestamp.
        """
        # Set a fixed timestamp for datetime.now() calls
        # 1744834800 is April 16, 2025 18:20:00 UTC
        self.fixed_timestamp = 1744834800
        self.fixed_datetime = datetime.datetime.fromtimestamp(
            self.fixed_timestamp, tz=datetime.timezone.utc
        )

        # Create a mock for datetime.datetime
        datetime_mock = mock.MagicMock(wraps=datetime.datetime)
        datetime_mock.now.return_value = self.fixed_datetime
        self.datetime_patcher = mock.patch("datetime.datetime", datetime_mock)
        self.datetime_patcher.start()

    def tearDown(self):
        """
        Clean up by stopping the datetime patch.
        """
        self.datetime_patcher.stop()
