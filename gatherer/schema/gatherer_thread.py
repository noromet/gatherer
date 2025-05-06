"""
This module defines the schema for weather data gathering and processing.
GathererThread: Represents a thread responsible for gathering weather data,
including metadata about the process.
"""

import datetime
import uuid
from dataclasses import dataclass
from typing import Dict


@dataclass
class GathererThread:
    """
    A class to represent a thread responsible for gathering weather data.

    Attributes:
        id (uuid.UUID): Unique identifier for the gatherer thread.
        thread_timestamp (datetime.datetime): Timestamp of the thread's execution.
        total_stations (int): Total number of weather stations being monitored.
        error_stations (int): Number of weather stations with errors.
        errors (dict): Dictionary containing error messages and their corresponding station IDs.
        command (str): Command that executed the thread.
    """

    id: uuid.UUID
    thread_timestamp: datetime.datetime
    total_stations: int
    error_stations: int
    errors: Dict
    command: str
