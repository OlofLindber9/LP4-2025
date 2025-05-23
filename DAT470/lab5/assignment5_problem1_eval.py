#!/usr/bin/env python3

import argparse
from collections import Counter
import numpy as np
from assignment5_problem1 import murmur3_32

if __name__ == '__main__':
    # Config
    path = "/data/courses/2025_dat470_dit066/words"
    seed = 0xee418b6c

    # Read words
    with open(path, "r", encoding="utf-8") as f:
        words = [line.strip() for line in f if line.strip()]

    counts = Counter()
    for key in words:
        h = murmur3_32(key,seed)
        lsb = h & 0b1111111
        counts[lsb] += 1

    freq = [counts[i] for i in range(128)]
    mean = np.mean(freq)
    stddev = np.std(freq)

    collisions = sum((count * (count - 1)) / 2 for count in counts.values())
    nr_pairs = sum(freq) * (sum(freq) - 1) / 2
    collision_prob = collisions / nr_pairs if nr_pairs > 0 else 0

    print(f"Frequency list: {freq}")
    print(f"Mean: {mean}")
    print(f"Standard deviation: {stddev}")
    print(f"Total number of words: {sum(freq)}")
    print(f"Collision probability: {collision_prob:.6f}")