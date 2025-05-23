#!/bin/bash

FILE="/data/courses/2025_dat470_dit066/glove/glove.840B.300d.txt"
QUERIES="/data/courses/2025_dat470_dit066/glove/queries.txt"
SCRIPT="assignment6_problem3.py"

echo "Running $SCRIPT on $FILE with $QUERIES..."
python3 "$SCRIPT" "$FILE" "$QUERIES" -D $1
