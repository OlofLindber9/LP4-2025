#!/usr/bin/env python3

import argparse
import assignment5_problem1 as p1
from collections import Counter
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Computes MurMurHash3 for each word in a file and plots a histogram.'
    )
    parser.add_argument('filepath', help='Path to input file containing words', type=str)
    args = parser.parse_args()

    seed = 0xee418b6c
    hash_counts = Counter()

    with open(args.filepath, 'r', encoding='utf-8') as f:
        for line in f:
            for word in line.strip().split():
                h = p1.murmur3_32(word, seed)
                h = h & 0x0fffffff  # 28-bit mask
                hash_counts[h] += 1

    # Get all hash values and their frequencies
    hash_values = list(hash_counts.keys())
    frequencies = list(hash_counts.values())


    plt.figure(figsize=(10, 5))
    plt.hist(hash_values, weights=frequencies, bins=50, color='skyblue', edgecolor='black')
    plt.title("Histogram of Murmur3 Hashes (28-bit masked)")
    plt.xlabel("Hash value (binned)")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig("hash_histogram.png")
