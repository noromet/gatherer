"""
Grapher.
"""

import matplotlib.pyplot as plt
import numpy as np

# Running benchmark with 10 stations and 1 threads
# Elapsed time: 4.04 seconds
# Running benchmark with 10 stations and 4 threads
# Elapsed time: 4.04 seconds
# Running benchmark with 10 stations and 8 threads
# Elapsed time: 4.04 seconds
# Running benchmark with 100 stations and 1 threads
# Elapsed time: 43.67 seconds
# Running benchmark with 100 stations and 4 threads
# Elapsed time: 11.95 seconds
# Running benchmark with 100 stations and 8 threads
# Elapsed time: 7.11 seconds
# Running benchmark with 250 stations and 1 threads
# Elapsed time: 109.03 seconds
# Running benchmark with 250 stations and 4 threads
# Elapsed time: 28.85 seconds
# Running benchmark with 250 stations and 8 threads
# Elapsed time: 17.90 seconds
# Running benchmark with 500 stations and 1 threads
# Elapsed time: 218.60 seconds
# Running benchmark with 500 stations and 4 threads
# Elapsed time: 56.93 seconds
# Running benchmark with 500 stations and 8 threads
# Elapsed time: 31.49 seconds
# Running benchmark with 750 stations and 1 threads
# Elapsed time: 328.33 seconds
# Running benchmark with 750 stations and 4 threads
# Elapsed time: 84.43 seconds
# Running benchmark with 750 stations and 8 threads
# Elapsed time: 47.97 seconds
# Running benchmark with 1000 stations and 1 threads
# Elapsed time: 437.40 seconds
# Running benchmark with 1000 stations and 4 threads
# Elapsed time: 114.84 seconds
# Running benchmark with 1000 stations and 8 threads
# Elapsed time: 64.78 seconds

# table
table = [
    ["threads", "stations", "elapsed_time"],
    [1, 10, 4.04],
    [4, 10, 4.04],
    [8, 10, 4.04],
    [1, 100, 43.67],
    [4, 100, 11.95],
    [8, 100, 7.11],
    [1, 250, 109.03],
    [4, 250, 28.85],
    [8, 250, 17.90],
    [1, 500, 218.60],
    [4, 500, 56.93],
    [8, 500, 31.49],
    [1, 750, 328.33],
    [4, 750, 84.43],
    [8, 750, 47.97],
    [1, 1000, 437.40],
    [4, 1000, 114.84],
    [8, 1000, 64.78],
]

# Create a dictionary to store the results
results = {}
for row in table[1:]:
    threads, stations, elapsed_time = row
    if threads not in results:
        results[threads] = {}
    results[threads][stations] = elapsed_time

# Define the station counts for vertical lines
station_values = [10, 100, 250, 500, 750, 1000]

# Get max elapsed time for setting up the plot range
max_elapsed_time = (
    max(max(times.values()) for times in results.values()) * 1.1
)  # Add 10% for margin

# Plot with improvements
plt.figure(figsize=(12, 8))
colors = ["blue", "red", "green"]
markers = ["o", "s", "^"]
line_styles = ["-", "--", "-."]

# Set specific y-ticks with a reasonable range
y_ticks = np.arange(0, max_elapsed_time, 50)
plt.yticks(y_ticks)

# Add horizontal grid lines at y-tick positions first (so they appear below the data lines)
for y in y_ticks:
    plt.axhline(y=y, color="gray", linestyle=":", alpha=0.5)

# Add vertical lines at specified station counts (below the data lines)
for station in station_values:
    plt.axvline(x=station, color="gray", linestyle=":", alpha=0.7)

# Plot the data lines last so they appear on top of the grid
for i, threads in enumerate(sorted(results.keys())):
    x = list(results[threads].keys())
    y = list(results[threads].values())
    plt.plot(
        x,
        y,
        label=f"{threads} threads",
        color=colors[i],
        marker=markers[i],
        linewidth=2,
        linestyle=line_styles[i],
        markersize=8,
        zorder=10,
    )  # Added zorder to ensure lines are on top

    # Add data point labels
    for j, (station_count, elapsed_time) in enumerate(zip(x, y)):
        plt.annotate(
            f"{elapsed_time:.1f}s",
            (station_count, elapsed_time),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=8,
            zorder=11,
        )  # Ensure labels are also on top

# Configure the plot
plt.xlabel("Number of Stations", fontsize=12, fontweight="bold")
plt.ylabel("Elapsed Time (seconds)", fontsize=12, fontweight="bold")
plt.title(
    "Benchmark Results: Elapsed Time vs Number of Stations",
    fontsize=14,
    fontweight="bold",
)

# Set specific x-ticks at station values
plt.xticks(station_values)

plt.legend(fontsize=10, loc="upper left")
plt.grid(False)  # Disable default grid since we're adding custom grid lines
plt.tight_layout()

# Optional: Add some more polish
plt.gca().spines["top"].set_visible(False)
plt.gca().spines["right"].set_visible(False)

# Save the plot
plt.savefig("benchmark_results_improved.png", dpi=300, bbox_inches="tight")
print("Plot saved as 'benchmark_results_improved.png'")

plt.show()
