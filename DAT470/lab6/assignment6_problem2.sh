#!/bin/bash

FILE="/data/courses/2025_dat470_dit066/glove/glove.6B.50d.txt"
SCRIPT="assignment6_problem2.py"
QUERIES="queries.txt"

echo "Running $SCRIPT on $FILE..."
python3 "$SCRIPT" "$FILE" "$QUERIES"