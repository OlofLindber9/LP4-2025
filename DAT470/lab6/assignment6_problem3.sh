#!/bin/bash

FILE="/data/courses/2025_dat470_dit066/glove/glove.6B.50d.txt"
SCRIPT="assignment6_problem3.py"
QUERIES="queries.txt"
PLANES=50

echo "Running $SCRIPT on $FILE..."
python3 "$SCRIPT" "$FILE" "$QUERIES" -D $PLANES