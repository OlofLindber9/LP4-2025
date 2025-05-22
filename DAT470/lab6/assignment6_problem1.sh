#!/bin/bash

FILE="/data/courses/2025_dat470_dit066/glove/glove.840B.300d.txt"
SCRIPT="assignment6_problem1.py"

echo "Running $SCRIPT on $FILE..."
python3 "$SCRIPT" "$FILE"