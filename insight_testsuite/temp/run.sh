#!/bin/bash

g++ -std=c++0x  -O3 ./src/anomaly_detection.cpp -o ./src/anomaly_detection.exe

./src/anomaly_detection.exe ./log_input/batch_log.json ./log_input/stream_log.json ./log_output/flagged_purchases.json
