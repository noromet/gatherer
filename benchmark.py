"""
Benchmarking script for the Gatherer library.
"""

from uuid import uuid4
import time

import matplotlib.pyplot as plt

from gatherer.gatherer import Gatherer
from gatherer.benchmark import BenchmarkWeatherReader
from gatherer.schema import WeatherStation


THREADS = [1, 4, 8]
N_STATIONS = [10, 100, 250, 500, 750, 1000]


def benchmark():
    """
    Entry point to the benchmark and graphing process.
    """

    # Dictionary to store results: {thread_count: {station_count: elapsed_time}}
    results = {thread: {} for thread in THREADS}

    for n_stations in N_STATIONS:
        for threads in THREADS:
            print(f"Running benchmark with {n_stations} stations and {threads} threads")
            elapsed_time = _run(n_stations, threads)
            results[threads][n_stations] = elapsed_time
            print(f"Elapsed time: {elapsed_time:.2f} seconds")

    # Plot the results
    plt.figure(figsize=(10, 6))

    colors = ["blue", "red", "green"]
    markers = ["o", "s", "^"]

    for i, threads in enumerate(THREADS):
        x = list(results[threads].keys())
        y = list(results[threads].values())
        plt.plot(
            x,
            y,
            label=f"{threads} threads",
            color=colors[i],
            marker=markers[i],
            linewidth=2,
        )

    plt.xlabel("Number of Stations")
    plt.ylabel("Elapsed Time (seconds)")
    plt.title("Benchmark Results: Elapsed Time vs Number of Stations")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()

    # Save the plot
    plt.savefig("benchmark_results.png", dpi=300)
    print("Plot saved.")

    # Also display the plot if running interactively
    plt.show()

    print(results)

    return results


def _run(n_stations, threads):
    """
    Run the benchmark for a given number of stations and threads.
    """
    connections = [
        "meteoclimatic",
        "weatherlink_v1",
        "weatherlink_v2",
        "holfuy",
        "thingspeak",
        "ecowitt",
        "realtime",
        "wunderground",
    ]

    readers = {conn: BenchmarkWeatherReader() for conn in connections}

    gatherer = Gatherer(
        run_id=uuid4().hex,
        dry_run=True,
        max_threads=threads,
        readers=readers,
    )

    stations = [
        WeatherStation(
            id=f"station_{i}",
            connection_type=connections[i % len(connections)],
            _data_timezone="Europe/Madrid",
            _local_timezone="Europe/Madrid",
            field1="value1",
            field2="value2",
            field3="value3",
            pressure_offset=0,
        )
        for i in range(n_stations)
    ]

    gatherer.add_many(stations)

    start_time = time.monotonic()
    gatherer.process(single_thread=False)
    end_time = time.monotonic()

    return end_time - start_time


if __name__ == "__main__":
    benchmark()
else:
    pass
