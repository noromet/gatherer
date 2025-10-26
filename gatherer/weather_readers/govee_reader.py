"""
Implements a reader for the Govee API.
"""

import datetime
import logging
import uuid

from gatherer.schema import WeatherRecord, WeatherStation

from .utils import UnitConverter
from .weather_reader import WeatherReader


class GoveeReader(WeatherReader):
    """
    Weather data reader for the Govee API.
    """

    CAPABILITY_TO_FIELD = {
        "sensorTemperature": "temperature",
        "sensorHumidity": "humidity",
    }

    def __init__(self, live_endpoint: str):
        super().__init__(live_endpoint)
        self.required_fields = ["field1", "field2"]

    def parse(self, station: WeatherStation, data: dict) -> WeatherRecord:
        """
        Parse the fetched data into a WeatherRecord object.
        Args:
            station (WeatherStation): The weather station object.
            data (dict): The raw data fetched from the API.
        Returns:
            WeatherRecord: The parsed weather record.
        """
        live_data = data.get("live").get("payload", {}).get("capabilities", {})
        if not live_data:
            logging.error(
                "No payload found in data for station %s. "
                "Check the API response format.",
                station.id,
            )
            return None

        fields = self.get_fields()

        is_online = False

        for capability in live_data:
            capability_type = capability.get("type")
            capability_instance = capability.get("instance")
            capability_value = capability.get("state", {}).get("value")

            if capability_instance in self.CAPABILITY_TO_FIELD:
                if capability_type != "devices.capabilities.property":
                    continue
                field_name = self.CAPABILITY_TO_FIELD.get(capability_instance, None)
                if not field_name:
                    logging.warning(
                        "Unknown capability instance '%s' for station %s.",
                        capability_instance,
                        station.id,
                    )
                    continue

                float_val = self.safe_float(capability_value)

                match field_name:
                    case "temperature":
                        float_val = UnitConverter.fahrenheit_to_celsius(float_val)

                fields["live"][field_name] = float_val

            elif (
                capability_type == "devices.capabilities.online"
                and capability_instance == "online"
            ):
                is_online = str(capability_value).lower() == "true"

        fields["source_timestamp"] = datetime.datetime.now(tz=station.local_timezone)

        return fields if is_online else None

    def fetch_live_data(self, station: WeatherStation) -> dict:
        """Call the Govee API and retrieve raw data."""
        sku, mac = station.field1.split("/")

        headers = {
            "Govee-API-Key": station.field2,
            "Accept": "application/json",
        }
        body = {
            "requestId": str(uuid.uuid4()),
            "payload": {
                "sku": sku,
                "device": mac,
            },
        }
        response = self.make_post_request(
            self.live_endpoint, headers=headers, body=body
        )

        if response and response.status_code == 200:
            return response.json()

        logging.error(
            "Failed to fetch live data from Govee API for station %s. "
            "Status code: %s",
            station.id,
            response.status_code if response else "No response",
        )
        return None
