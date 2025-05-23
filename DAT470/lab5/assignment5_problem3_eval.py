#!/usr/bin/env python3

import argparse
import random
import statistics
import sys
import os
from pyspark import SparkContext, SparkConf
import math
import time

def rol32(x, r):
    return ((x << r) | (x >> (32 - r))) & 0xffffffff

def murmur3_32(key, seed=0):
    c1 = 0xcc9e2d51
    c2 = 0x1b873593
    r1 = 15
    r2 = 13
    m = 5
    n = 0xe6546b64

    if isinstance(key, str):
        key = key.encode('utf-8')  # Ensure we work with bytes

    length = len(key)
    hash = seed

    # Body
    for block_start in range(0, length // 4 * 4, 4):
        k = int.from_bytes(key[block_start:block_start + 4], 'little')
        k = (k * c1) & 0xffffffff
        k = rol32(k, r1)
        k = (k * c2) & 0xffffffff

        hash ^= k
        hash = rol32(hash, r2)
        hash = (hash * m + n) & 0xffffffff

    # Tail
    tail = key[length // 4 * 4:]
    k = 0
    for i in range(len(tail)):
        k |= tail[i] << (i * 8)
    if len(tail) > 0:
        k = (k * c1) & 0xffffffff
        k = rol32(k, r1)
        k = (k * c2) & 0xffffffff
        hash ^= k

    # Finalization
    hash ^= length
    hash ^= (hash >> 16)
    hash = (hash * 0x85ebca6b) & 0xffffffff
    hash ^= (hash >> 13)
    hash = (hash * 0xc2b2ae35) & 0xffffffff
    hash ^= (hash >> 16)

    return hash


def auto_int(x):
    """Auxiliary function to help convert e.g. hex integers"""
    return int(x,0)

def dlog2(n):
    return n.bit_length() - 1

def rho(n):
    """Given a 32-bit number n, return the 1-based position of the first
    1-bit"""
    counter = 0
    
    while n & 0x100000000 == 0:
        if n == 0:
            return counter
        n = n << 1
        counter += 1
    return counter

def compute_jr(key,seed,log2m):
    """hash the string key with murmur3_32, using the given seed
    then take the **least significant** log2(m) bits as j
    then compute the rho value **from the left**

    E.g., if m = 1024 and we compute hash value 0x70ffec73
    or 0b01110000111111111110110001110011
    then j = 0b0001110011 = 115
         r = 2
         since the 2nd digit of 0111000011111111111011 is the first 1

    Return a tuple (j,r) of integers
    """
    h = murmur3_32(key,seed)
    j = ~(0xffffffff << log2m) & h
    r = rho(h)
    return j, r

def get_files(path):
    """
    A generator function: Iterates through all .txt files in the path and
    returns the content of the files

    Parameters:
    - path : string, path to walk through

    Yields:
    The content of the files as strings
    """
    for (root, dirs, files) in os.walk(path):
        for file in files:
            if file.endswith('.txt'):
                path = f'{root}/{file}'
                with open(path,'r') as f:
                    yield f.read()

def alpha(m):
    """Auxiliary function: bias correction"""
    if m == 16:
        return 0.673
    elif m == 32:
        return 0.697
    elif m == 64:
        return 0.709
    else:
        return 0.7213 / (1 + 1.079 / m)

def E_estimate(M, m):
    """Estimate the cardinality of the set using the HyperLogLog algorithm
    and the bias correction factor alpha(m)"""
    Z = math.fsum([1.0 / (1 << mj) for mj in M])
    E = alpha(m) * m * m / Z
    return E

def print_estimates(estimates):
    # Bin width
    bin_width = 8

    # Create histogram bins
    histogram = {}
    for est in estimates:
        bin_start = (int(est) // bin_width) * bin_width
        histogram[bin_start] = histogram.get(bin_start, 0) + 1

    # Sort and print bins
    sorted_bins = sorted(histogram.items())
    print(f"\nHistogram bins (width {bin_width}):")
    print("[")
    for bin_start, count in sorted_bins:
        print(f"  ({bin_start}, {count}),")
    print("]")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Using HyperLogLog, computes the approximate number of '
                    'distinct words in all .txt files under the given path.'
    )
    parser.add_argument('path', help='path to walk', type=str)
    parser.add_argument('-m', '--num-registers', type=int, required=True,
                        help='number of registers (must be a power of two)')
    parser.add_argument('-w', '--num-workers', type=int, default=1,
                        help='number of Spark workers')
    args = parser.parse_args()

    m = args.num_registers
    if m <= 0 or (m & (m - 1)) != 0:
        sys.stderr.write(f'{sys.argv[0]}: m must be a positive power of 2\n')
        quit(1)
    log2m = dlog2(m)

    num_workers = args.num_workers
    if num_workers < 1:
        sys.stderr.write(f'{sys.argv[0]}: must have a positive number of '
                         'workers\n')
        quit(1)

    path = args.path
    if not os.path.isdir(path):
        sys.stderr.write(f"{sys.argv[0]}: `{path}' is not a valid directory\n")
        quit(1)

    true_cardinality = 284689
    sigma = 1.04 / math.sqrt(m)

    start = time.time()
    conf = SparkConf()
    conf.setMaster(f'local[{num_workers}]')
    conf.set('spark.driver.memory', '64g')
    sc = SparkContext(conf=conf)

    # Load and cache the data once
    data = sc.parallelize(get_files(path)).flatMap(lambda x: x.split()).cache()
    words = data.collect()

    # Generate 1000 random seeds
    seeds = [random.randint(0, 0xffffffff) for _ in range(1000)]
    seed_rdd = sc.parallelize(seeds, num_workers)

    def estimate_from_seed(seed):
        M = [0] * m
        for word in words:
            j, r = compute_jr(word, seed, log2m)
            M[j] = max(M[j], r)

        E = E_estimate(M, m)
        Z = M.count(0)

        if E <= (5 / 2) * m and Z > 0:
            E = m * math.log(m / Z)
        elif E > (1 / 30) * (2 ** 32):
            E = -(2 ** 32) * math.log(1 - E / (2 ** 32))

        return E

    # Parallel HLL estimation
    estimates = seed_rdd.map(estimate_from_seed).collect()

    sc.stop()
    end = time.time()

    avg = statistics.mean(estimates)
    std = statistics.stdev(estimates)

    print(f'Average estimate: {avg:.2f}')
    print(f'Standard deviation: {std:.2f}')

    for k in [1, 2, 3]:
        lower = true_cardinality * (1 - k * sigma)
        upper = true_cardinality * (1 + k * sigma)
        count = sum(lower <= est <= upper for est in estimates)
        fraction = count / 1000
        print(f'Fraction within n (1 +/- {k} sigma: {fraction:.3f}')
    print_estimates(estimates)

    print(f'Number of workers: {num_workers}')
    print(f'Total time: {end - start:.2f} seconds')