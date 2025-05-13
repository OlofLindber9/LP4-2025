#!/usr/bin/env python3

import argparse
import sys
import os
from pyspark import SparkContext, SparkConf
import math
import time
import re

def rol32(x,k):
    """Auxiliary function (left rotation for 32-bit words)"""
    return ((x << k) | (x >> (32-k))) & 0xffffffff

def murmur3_32(key, seed):
    """Computes the 32-bit murmur3 hash"""
    c1 = 0xcc9e2d51
    c2 = 0x1b873593
    r1 = 15
    r2 = 13
    m = 5
    n = 0xe6546b64

    hash = seed
    length = len(key)
    nblocks = length // 4
    
    for i in range(nblocks):
        k = key[i*4:(i+1)*4]
        k = int.from_bytes(k.encode('utf-8'), 'little')
        k = (k * c1) & 0xffffffff
        k = rol32(k, r1)
        k = (k * c2) & 0xffffffff
        hash ^= k
        hash = rol32(hash, r2)
        hash = (hash * m + n) & 0xffffffff

    tail = key[nblocks*4:]
    if len(tail) > 0:
        remaining = int.from_bytes(tail.encode('utf-8'), 'little')
        remaining = (remaining * c1) & 0xffffffff
        remaining = rol32(remaining, r1)
        remaining = (remaining * c2) & 0xffffffff
        hash ^= remaining
    
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
        n = n << 1
        counter += 1
        if n == 0:
            return 0
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
    # print(f'{h:08x}')
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
    return 0.7213 / (1 + 1.079 / m)

def E_estimate(M, m):
    """Estimate the cardinality of the set using the HyperLogLog algorithm
    and the bias correction factor alpha(m)"""
    Z = 0
    for j in range(m):
        Z += 1 / (2 ** M[j])
    E = alpha(m) * m ** 2 / Z
    return E

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Using HyperLogLog, computes the approximate number of '
            'distinct words in all .txt files under the given path.'
    )
    parser.add_argument('path',help='path to walk',type=str)
    parser.add_argument('-s','--seed',type=auto_int,default=0,help='seed value')
    parser.add_argument('-m','--num-registers',type=int,required=True,
                            help=('number of registers (must be a power of two)'))
    parser.add_argument('-w','--num-workers',type=int,default=1,
                        help='number of Spark workers')
    args = parser.parse_args()

    seed = args.seed
    m = args.num_registers
    if m <= 0 or (m&(m-1)) != 0:
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

    start = time.time()
    conf = SparkConf()
    conf.setMaster(f'local[{num_workers}]')
    conf.set('spark.driver.memory', '16g')
    sc = SparkContext(conf=conf)

    data = sc.parallelize(get_files(path)) \
        .flatMap(lambda text: re.findall(r'[a-z]+', text.lower()))
        # .flatMap(lambda x: x.split())
        # .flatMap(lambda text: re.findall(r'\b\w+\b', text.lower()))
    
    jrs = data.map(lambda x: compute_jr(x, seed, log2m)) \
        .reduceByKey(lambda a,b: max(a, b)) \
        .collect()
    
    print(jrs)
    M = [0] * m
    for j, r in jrs:
        M[j] = max(M[j], r)

    # Compute cardinality estimate
    E = E_estimate(M, m)

    # Count zero entries in M
    Z = M.count(0)

    # Bias Correction
    if E <= (5 / 2) * m and Z > 0:
        E = m * math.log(m / Z)
    elif E > (1 / 30) * (2 ** 32):
        E = -(2 ** 32) * math.log(1 - E / (2 ** 32))
        
    end = time.time()

    print(f'Cardinality estimate: {E}')
    print(f'Number of workers: {num_workers}')
    print(f'Took {end-start} s')
