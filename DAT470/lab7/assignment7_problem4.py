import numpy as np
import argparse
import pandas as pd
import csv
import sys
import time
import cupy as cp

def linear_scan(X, Q, b):
    """
    Perform linear scan for querying nearest neighbor.
    X: n*d dataset
    Q: m*d queries
    b: batch size
    Returns an m-vector of indices I; the value i reports the row in X such 
    that the Euclidean norm of ||X[I[i],:]-Q[i]|| is minimal
    """
    
    m = Q.shape[0]
    n = X.shape[0]
    I = cp.zeros(m, dtype=cp.int32) 

    X_norm_sq = cp.sum(X ** 2, axis=1)  
    
    for i in range(0, m, b):
        i_end = min(i + b, m)
        #Q_batch = Q[i:i+b]
        Q_batch = Q[i:i_end] 

        Q_norm_sq = cp.sum(Q_batch ** 2, axis=1)
        #D = Q_batch[:, cp.newaxis, :] - X[cp.newaxis, :, :]
        D_sq = Q_norm_sq[:, None] + X_norm_sq[None, :] - 2 * Q_batch.dot(X.T)
        #D_sq = cp.sum(D**2, axis=-1) 
         
        I[i:i+b] = cp.argmin(D_sq, axis=1)
        
        #argmin_result = cp.argmin(D_sq, axis=1)
        argmin_result = cp.argmin(D_sq, axis=1).astype(cp.int32)

        assert argmin_result.dtype == I.dtype
        assert isinstance(argmin_result, cp.ndarray)
        assert isinstance(I, cp.ndarray)
        assert I[i:i_end].shape == argmin_result.shape, \
        f"Shape mismatch: I[{i}:{i_end}].shape = {I[i:i_end].shape}, argmin shape = {argmin_result.shape}"

        #print(f"Batch [{i}:{i_end}] - I slice shape: {I[i:i_end].shape}, argmin shape: {argmin_result.shape}")

        I[i:i_end] = argmin_result


    return I.astype(np.int32)

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
    parser.add_argument(
        '-b', '--batch-size', type=int, required=False,
        help = 'Size of batches')
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
    
    
    #dummy transfer
    _ = cp.array(np.zeros(10, dtype=np.float32))
    cp.cuda.Stream.null.synchronize()
    t6 = time.time()

    # Copy data from host (CPU) to device (GPU)
    t_host_to_device_start = time.time()
    X_gpu = cp.array(X, dtype=cp.float32) 
    Q_gpu = cp.array(Q, dtype=cp.float32) 
    cp.cuda.Stream.null.synchronize()
    t_host_to_device_end = time.time()

    I_gpu = linear_scan(X_gpu,Q_gpu,args.batch_size)
    
    # Synchronize GPU to ensure computation is done
    cp.cuda.Stream.null.synchronize()

    t_device_to_host_start = time.time()
    I = cp.asnumpy(I_gpu)
    cp.cuda.Stream.null.synchronize()
    t_device_to_host_end = time.time()

    t7 = time.time()
    assert I.shape == (m,)

    num_erroneous = 0
    if QL is not None:
        for (i,j) in enumerate(I):
            if QL[i] != L[j]:
                sys.stderr.write(f'{i}th query was erroneous: got "{L[j]}", '
                                     f'but expected "{QL[i]}"\n')
                num_erroneous += 1

    print(f'Loading dataset ({n} vectors of length {d}) took', t2-t1)
    print(f'Performing {m} NN queries took', t7-t6)
    print(f'Number of erroneous queries: {num_erroneous}')
    print(f'Transferring dataset and queries from host to device took {t_host_to_device_end - t_host_to_device_start:.4f} seconds')
    print(f'Transferring results from device to host took {t_device_to_host_end - t_device_to_host_start:.4f} seconds')

