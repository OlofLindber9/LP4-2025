#!/bin/bash

FILE="/data/courses/2025_dat470_dit066/glove/glove.840B.300d.txt"
QUERIES="/data/courses/2025_dat470_dit066/glove/queries.txt"
SCRIPT="assignment6_problem4.py"

echo "Running $SCRIPT on $FILE with $QUERIES..."
python3 "$SCRIPT" "$FILE" "$QUERIES" -D $1 -k $2 -L $3
# python3 assignment6_problem4.py /data/courses/2025_dat470_dit066/glove/glove.6B.50d.txt /data/courses/2025_dat470_dit066/glove/queries.txt -D 50 -k 20 -L 10