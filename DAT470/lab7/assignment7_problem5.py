import numpy as np
import argparse
import pandas as pd
import csv
import sys
import time
import cupy as cp
from cuvs.neighbors import brute_force

def load_glove(fn):
    """
    Loads the glove dataset from the file
    Returns (X,L) where X is the dataset vectors and L is the words associated
    with the respective rows.
    """
    df = pd.read_table(fn, sep = ' ', index_col = 0, header = None,
                           quoting = csv.QUOTE_NONE, keep_default_na = False)
    X = np.ascontiguousarray(df, dtype = np.float32)
    L = df.index.tolist()
    return (X, L)

def load_pubs(fn):
    """
    Loads the pubs dataset from the file
    Returns (X,L) where X is the dataset vectors (easting,northing) and 
    L is the list of names of pubs, associated with each row
    """
    df = pd.read_csv(fn)
    L = df['name'].tolist()
    X = np.ascontiguousarray(df[['easting','northing']], dtype = np.float32)
    return (X, L)

def load_queries(fn):
    """
    Loads the m*d array of query vectors from the file
    """
    return np.loadtxt(fn, delimiter = ' ', dtype = np.float32)

def load_query_labels(fn):
    """
    Loads the m-long list of correct query labels from a file
    """
    with open(fn,'r') as f:
        return f.read().splitlines()

if __name__ == '__main__':
    parser = argparse.ArgumentParser( \
          description = 'Perform nearest neighbor queries under the '
          'Euclidean metric using linear scan, measure the time '
          'and optionally verify the correctness of the results')
    parser.add_argument(
        '-d', '--dataset', type=str, required=True,
        help = 'Dataset file (must be pubs or glove)')
    parser.add_argument(
        '-q', '--queries', type=str, required=True,
        help = 'Queries file (must be compatible with the dataset)')
    parser.add_argument(
        '-l', '--labels', type=str, required=False,
        help = 'Optional correct query labels; if provided, the correctness '
        'of returned results is checked')
    args = parser.parse_args()

    t1 = time.time()
    if 'pubs' in args.dataset:
        (X,L) = load_pubs(args.dataset)
    elif 'glove' in args.dataset:
        (X,L) = load_glove(args.dataset)
    else:
        sys.stderr.write(f'{sys.argv[0]}: error: Only glove/pubs supported\n')
        exit(1)
    t2 = time.time()

    (n,d) = X.shape
    assert len(L) == n

    t3 = time.time()
    Q = load_queries(args.queries)
    t4 = time.time()

    assert X.flags['C_CONTIGUOUS']
    assert Q.flags['C_CONTIGUOUS']
    assert X.dtype == np.float32
    assert Q.dtype == np.float32
    
    m = Q.shape[0]
    assert Q.shape[1] == d

    t5 = time.time()
    QL = None
    if args.labels is not None:
        QL = load_query_labels(args.labels)
        assert len(QL) == m

    # First access to the GPU is slower than later
    _ = cp.asarray([], blocking = True)
    t6 = time.time()
    X_gpu = cp.asarray(X, blocking = True)
    Q_gpu = cp.asarray(Q, blocking = True)
    t7 = time.time()

    index = brute_force.build(X_gpu, metric='euclidean')
    _, neighbors = brute_force.search(index, Q_gpu, 1)
    I = cp.asarray(neighbors, dtype=cp.int32).flatten()
    t8 = time.time()
    
    cp.cuda.Stream.null.synchronize()
    
    t9 = time.time()
    I = cp.asnumpy(I, blocking = True)
    t9 = time.time()
    assert I.shape == (m,)

    num_erroneous = 0
    if QL is not None:
        for (i,j) in enumerate(I):
            if QL[i] != L[j]:
                # sys.stderr.write(f'{i}th query was erroneous: got "{L[j]}", '
                #                      f'but expected "{QL[i]}"\n')
                num_erroneous += 1
    print(f'Loading dataset ({n} vectors of length {d}) took', t2-t1)
    print(f'Performing {m} NN queries took', t8-t7)
    print(f'Transferring dataset and queries from host to device took', t7-t6)
    print(f'Transferring results from device to host took', t9-t8)
    print(f'Number of erroneous queries: {num_erroneous}')
