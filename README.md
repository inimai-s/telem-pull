Telemetron Custom Pull Script
Script that takes in a csv list of different SXIDs, start and end times, and epochs. Able to output telemetron telemetry pull for custom input.

Inputs: pull_params.csv (change as necessary)
Columns: (from left to right)
identifier: SXID column
start_time: start time of pull in '%Y-%m-%d %H:%M:%S' format
end_time: end time of pull in '%Y-%m-%d %H:%M:%S'
epoch_sec: time between consecutive measurements in seconds

Script: custom_pull_script.py
Takes pull_params.csv as input and outputs output.csv that contains aggregated measurements for the inputs specified.

Installation python packages:
datetime
pandas
numpy
sx-telemetron
scipy
multiprocessing
