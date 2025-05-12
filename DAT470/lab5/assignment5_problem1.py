#!/usr/bin/env python3

import argparse

def rol32(x,k):
    """Auxiliary function (left rotation for 32-bit words)"""
    return ((x << k) | (x >> (32-k))) & 0xffffffff

def murmur3_32(key, seed):
    """Computes the 32-bit murmur3 hash"""
    key = key.encode("utf-8")
    #seed = seed.encode("utf-8")

    c1 = int("0xcc9e2d51", 16)
    c2 = int("0x1b873593", 16)
    r1 = 15 & 0xFFFFFFFF
    r2 = 13 & 0xFFFFFFFF
    m = 5 & 0xFFFFFFFF
    n = int("0xe6546b64", 16)

    hash = seed
    for i in range(0, len(key), 4):
        chunk = key[i:i+4]

        k = int.from_bytes(chunk, byteorder="little")
        k = (k * c1) % 2**32
        k = rol32(k,r1)
        k = (k * c2) % 2**32

        hash = hash ^ k
        hash = rol32(hash, r2)
        hash = ((hash * m) + n) % 2**32

    remainingBytes = key[len(key) - len(key) % 4:]

    if(remainingBytes):
        remainingBytes = int.from_bytes(remainingBytes, byteorder='little')
        remainingBytes = (remainingBytes * c1) % 2**32
        remainingBytes = rol32(remainingBytes, r1)
        remainingBytes = (remainingBytes * c2) % 2**32

        hash = hash ^ remainingBytes

    # algorithm says to XOR with length here
    hash = hash ^ len(key)


    hash = hash ^ (hash >> 16)
    hash = (hash * int("0x85ebca6b", 16)) % 2**32
    hash = hash ^ (hash >> 13)
    hash = (hash * int("0xc2b2ae35", 16)) % 2**32
    hash = hash ^ (hash >> 16)

    return hash
    
def auto_int(x):
    """Auxiliary function to help convert e.g. hex integers"""
    return int(x,0)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Computes MurMurHash3 for the keys.'
    )
    parser.add_argument('key',nargs='*',help='key(s) to be hashed',type=str)
    parser.add_argument('-s','--seed',type=auto_int,default=0,help='seed value')
    args = parser.parse_args()

    seed = args.seed
    for key in args.key:
        h = murmur3_32(key,seed)
        print(f'{h:#010x}\t{key}')
        