#!/usr/bin/env python3

import argparse
import sys


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
    """Auxiliary function to compute discrete base2 logarithm"""
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
    print(f'{h:08x}')
    j = ~(0xffffffff << log2m) & h
    r = rho(h)
    return j, r


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Computes (j,r) pairs for input integers.'
    )
    parser.add_argument('key',nargs='*',help='key(s) to be hashed',type=str)
    parser.add_argument('-s','--seed',type=auto_int,default=0,help='seed value')
    parser.add_argument('-m','--num-registers',type=int,required=True,
                            help=('Number of registers (must be a power of two)'))
    args = parser.parse_args()

    seed = args.seed
    m = args.num_registers
    if m <= 0 or (m&(m-1)) != 0:
        sys.stderr.write(f'{sys.argv[0]}: m must be a positive power of 2\n')
        quit(1)

    log2m = dlog2(m)

    for key in args.key:
        h = murmur3_32(key,seed)

        j, r = compute_jr(key,seed,log2m)

        print(f'{key}\t{j}\t{r}')
        