"""Gatherer module for processing weather station data."""

from collections import defaultdict
import statistics
import concurrent.futures
import threading
import json
import queue
import logging

from gatherer.schema import WeatherRecord, WeatherStation
from gatherer.postprocessing import Validator, Corrector
from gatherer.database import Database


class Gatherer:
    """
    Main class for gathering and processing weather station data.

    Attributes:
        run_id (str): Unique identifier for the current run.
        dry_run (bool): Indicates whether the application is in dry-run mode.
        max_threads (int): Maximum number of threads for multithreading.
        stations (set): Set of weather stations to process.
        readers (dict): Dictionary of data readers by connection type.
        benchmark_results (dict): Dictionary of benchmark results by connection type.
    """

    def __init__(self, run_id: str, dry_run: bool, max_threads: int, readers: dict):
        """
        Initializes the Gatherer instance.

        :param run_id: Unique identifier for the run.
        :param dry_run: Whether to perform a dry run.
        :param max_threads: Maximum number of threads for multithreading.
        :param readers: Dictionary of data readers by connection type.
        """
        self.run_id = run_id
        self.dry_run = dry_run
        self.max_threads = max_threads
        self.stations = set()
        self.readers = readers
        self.validator = Validator()
        self.corrector = Corrector()
        # Collection for benchmark results - thread-safe because it's accessed with a lock
        self.benchmark_results = defaultdict(list)
        self.benchmark_lock = threading.Lock()

    def add_station(self, station: WeatherStation):
        """Adds a single station to the set."""
        if not station:
            logging.error("Station is None")
            return
        if station in self.stations:
            logging.error("Station %s already saved", station)
            return
        self.stations.add(station)

    def add_many(self, stations: list[WeatherStation]):
        """Adds multiple stations to the set."""
        if not stations:
            logging.error("Stations list is None")
            return
        for station in stations:
            self.add_station(station)

    def validate_station_timezones(self, station: WeatherStation) -> bool:
        """Validates the timezones of a station."""
        valid_timezones = {"Europe/Madrid", "Europe/Lisbon", "Etc/UTC"}
        if station.data_timezone.key not in valid_timezones:
            logging.error("Invalid data timezone for station %s. Skipping.", station.id)
            return False
        if station.local_timezone.key not in valid_timezones:
            logging.error(
                "Invalid local timezone for station %s. Skipping.", station.id
            )
            return False
        return True

    def process_station(self, station: WeatherStation) -> dict:
        """Processes a single station."""
        logging.info(
            "Processing station %s, type %s", station.id, station.connection_type
        )
        if not self.validate_station_timezones(station):
            return {
                "status": "error",
                "error": f"Invalid timezone for station {station.id}",
            }

        # Create a new reader instance for each station to ensure thread safety
        reader_class = self.readers.get(station.connection_type)
        if not reader_class:
            message = f"Invalid connection type for station {station.id}"
            logging.error(message)
            return {"status": "error", "error": message}

        # Create a new instance with the same parameters
        reader = reader_class()

        try:
            record = reader.read(station)

            # Collect benchmarking data if available
            if hasattr(reader, "response_times_ms") and reader.response_times_ms:
                with self.benchmark_lock:
                    self.benchmark_results[station.connection_type].extend(
                        reader.response_times_ms
                    )

            if not record:
                message = f"No data retrieved for station {station.id}"
                logging.error(message)
                return {"status": "error", "error": message}

            record = self._process_record(record, station)

            if not self.dry_run:
                Database.save_record(record)
                logging.info("Record saved for station %s", station.id)
            else:
                logging.debug(
                    json.dumps(record.__dict__, indent=4, sort_keys=True, default=str)
                )
                logging.info(
                    "Dry run enabled, record not saved for station %s", station.id
                )

            return {"status": "success"}

        except Exception as e:
            logging.error("Error processing station %s: %s", station.id, e)
            if not self.dry_run:
                Database.increment_incident_count(station.id)
            return {"status": "error", "error": str(e)}

    def _process_record(self, record: WeatherRecord, station: WeatherStation) -> bool:
        """Processes and saves a record."""
        record.station_id = station.id
        record.gatherer_thread_id = self.run_id

        record = self.corrector.correct(record, station.pressure_offset)
        record = self.validator.validate(record)

        return record

    def _process_chunk(
        self, chunk: list[WeatherStation], chunk_number: int, result_queue: queue.Queue
    ):
        """Processes a chunk of stations."""
        logging.info(
            "Processing chunk %d on %s", chunk_number, threading.current_thread().name
        )
        results = {station.id: self.process_station(station) for station in chunk}
        result_queue.put(results)

    def multithread_processing(self, stations: list[WeatherStation]) -> dict:
        """Processes stations using multithreading."""
        chunks = self._split_into_chunks(stations)
        result_queue = queue.Queue()

        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_threads
        ) as executor:
            futures = [
                executor.submit(self._process_chunk, chunk, i, result_queue)
                for i, chunk in enumerate(chunks)
            ]
            concurrent.futures.wait(futures)

        results = {}
        while not result_queue.empty():
            results.update(result_queue.get())
        return results

    def _split_into_chunks(
        self, stations: list[WeatherStation]
    ) -> list[list[WeatherStation]]:
        """Splits stations into chunks for multithreading."""
        chunk_size = len(stations) // self.max_threads
        remainder_size = len(stations) % self.max_threads
        chunks = [
            stations[i * chunk_size : (i + 1) * chunk_size]
            for i in range(self.max_threads)
        ]
        for i in range(remainder_size):
            chunks[i].append(stations[-(i + 1)])
        return chunks

    def process(self, single_thread: bool) -> dict:
        """Processes all stations."""
        if not self.stations:
            logging.error("No active stations found!")
            return {}

        if single_thread or len(self.stations) < 30:
            results = {
                station.id: self.process_station(station) for station in self.stations
            }
        else:
            results = self.multithread_processing(list(self.stations))

        # Display benchmark statistics after processing
        self._log_benchmark_results()

        return results

    def _log_benchmark_results(self):
        """Log the benchmark results for each connection type."""
        if not self.benchmark_results:
            return

        logging.info("===== BENCHMARK RESULTS =====")
        for connection_type, times in self.benchmark_results.items():
            if not times:
                continue

            avg_time = statistics.mean(times)
            median_time = statistics.median(times)
            min_time = min(times)
            max_time = max(times)
            count = len(times)

            logging.info(
                "%s: %d requests, avg=%.2f ms, median=%.2f ms, min=%.2f ms, max=%.2f ms",
                connection_type,
                count,
                avg_time,
                median_time,
                min_time,
                max_time,
            )
