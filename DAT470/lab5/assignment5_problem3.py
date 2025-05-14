#!/usr/bin/env python3

import argparse
import sys
import os
from pyspark import SparkContext, SparkConf
import math
import time
import assignment5_problem1 as p1
import assignment5_problem2 as p2
import re

def murmur3_32(key, seed):
    """Computes the 32-bit murmur3 hash"""
    # copy from Problem 1
    return p1.murmur3_32(key, seed)

def auto_int(x):
    """Auxiliary function to help convert e.g. hex integers"""
    return int(x,0)

def dlog2(n):
    return n.bit_length() - 1

def rho(n):
    # Copy from Problem 2
    return p2.rhoRight(n)

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
    # Copy from Problem 2
    return p2.compute_jr(key,seed,log2m)

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
    return 0.7213/(1+(1.079/m))

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
            .flatMap(lambda x: x.split())
                #.flatMap(lambda file: re.findall(r'\b\w+\b', file[1].lower()))


    jrs = data.map(lambda x: compute_jr(x, seed, log2m))

    max_r_per_j = jrs.reduceByKey(lambda a, b: max(a, b))

    max_r = max_r_per_j.collect()

    registers = [0] * m
    for j, r in max_r:
        registers[j] = r
    #registers = max_r_per_j.collectAsMap()

    #print(registers)
    #registers = [12,17,14,14,11,11,14,11,15,14,14,13,15,15,12,14]
    #print(registers)
    sum = 0
    for r in registers:
        sum += 2**(-r)

    E = alpha(m) * m**2 * sum**-1

    v_0 = 0
    for r in registers:
        if r == 0:
            v_0 += 1

    if E <= (5*m)/2 and v_0 > 0:
        E = m * math.log(m/v_0)
    elif E <= 1/30 * 2**32:
        E = -2**32 * math.log(1 - (E / 2**32))
    
    end = time.time()

    print(f'Cardinality estimate: {E}')
    print(f'Number of workers: {num_workers}')
    print(f'Took {end-start} s')

    
    

    