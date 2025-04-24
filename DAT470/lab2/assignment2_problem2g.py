import os
import argparse
import sys
import time
import multiprocessing as mp

def get_filenames(path):
    """
    A generator function: Iterates through all .txt files in the path and
    returns the full names of the files

    Parameters:
    - path : string, path to walk through

    Yields:
    The full filenames of all files ending in .txt
    """
    for (root, dirs, files) in os.walk(path):
        for file in files:
            if file.endswith('.txt'):
                yield f'{root}/{file}'

def get_file(path):
    """
    Reads the content of the file and returns it as a string.

    Parameters:
    - path : string, path to a file

    Return value:
    The content of the file in a string.
    """
    with open(path,'r') as f:
        return f.read()

def count_words_in_file(filename_queue,wordcount_queue,batch_size):
    """
    Counts the number of occurrences of words in the file
    Performs counting until a None is encountered in the queue
    Counts are stored in wordcount_queue
    Whitespace is ignored

    Parameters:
    - filename_queue, multiprocessing queue :  will contain filenames and None as a sentinel to indicate end of input
    - wordcount_queue, multiprocessing queue : (word,count) dictionaries are put in the queue, and end of input is indicated with a None
    - batch_size, int : size of batches to process

    Returns: None
    """
    # Since the I/O between processes easily forms a bottleneck,
    # use the parameter batch size to specify how many files the workers should
    # process before they copy their intermediate word count dictionary into
    # wordcount queue

    while True:
        counts = dict()
        for _ in range(batch_size):
            filename = filename_queue.get()
            if filename is None:
                wordcount_queue.put(counts)
                wordcount_queue.put(None)
                return

            file = get_file(filename)

            for word in file.split():
                if word in counts:
                    counts[word] += 1
                else:
                    counts[word] = 1

        wordcount_queue.put(counts)

def get_top10(counts):
    """
    Determines the 10 words with the most occurrences.
    Ties can be solved arbitrarily.

    Parameters:
    - counts, dictionary : a mapping from words (str) to counts (int)
    
    Return value:
    A list of (count,word) pairs (int,str)
    """
    return [(v,k) for k,v in sorted(counts.items(), key=lambda x:x[1], reverse=True)][:10]


def merge_counts(out_queue,wordcount_queue,num_workers):
    """
    Merges the counts from the queue into the shared dict global_counts. 
    Quits when num_workers Nones have been encountered.

    Parameters:
    - global_counts, manager dict : global dictionary where to store the counts
    - wordcount_queue, manager queue : queue that contains (word,count) pairs and Nones to signal end of input from a worker
    - num_workers, int : number of workers (i.e., how many Nones to expect)

    Return value: None
    """
    global_counts = dict()
    num_nones = 0

    while num_nones < num_workers:
        # get the next dictionary from the queue
        counts = wordcount_queue.get()
        if counts is None:
            num_nones += 1
            continue

        # merge the counts into the global dictionary
        for (word,count) in counts.items():
            if word in global_counts:
                global_counts[word] += count
            else:
                global_counts[word] = count

    # compute the checksum and top 10
    checksum = compute_checksum(global_counts)
    top10 = get_top10(global_counts)

    out_queue.put(checksum)
    out_queue.put(top10)
    return

def compute_checksum(counts):
    """
    Computes the checksum for the counts as follows:
    The checksum is the sum of products of the length of the word and its count

    Parameters:
    - counts, dictionary : word to count dictionary

    Return value:
    The checksum (int)
    """
    return sum(len(k) * v for (k,v) in counts.items())

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Counts words of all the text files in the given directory')
    parser.add_argument('-w', '--num-workers', help = 'Number of workers', default=1, type=int)
    parser.add_argument('-b', '--batch-size', help = 'Batch size', default=1, type=int)
    parser.add_argument('path', help = 'Path that contains text files')
    args = parser.parse_args()

    path = args.path

    if not os.path.isdir(path):
        sys.stderr.write(f'{sys.argv[0]}: ERROR: `{path}\' is not a valid directory!\n')
        quit(1)

    num_workers = args.num_workers
    if num_workers < 1:
        sys.stderr.write(f'{sys.argv[0]}: ERROR: Number of workers must be positive (got {num_workers})!\n')
        quit(1)

    batch_size = args.batch_size
    if batch_size < 1:
        sys.stderr.write(f'{sys.argv[0]}: ERROR: Batch size must be positive (got {batch_size})!\n')
        quit(1)

    t1 = time.time()

    # construct workers and queues
    filename_queue = mp.Queue()
    wordcount_queue = mp.Queue()
    out_queue = mp.Queue()
    workers = [mp.Process(target=count_words_in_file, args=(filename_queue,wordcount_queue,batch_size)) 
               for _ in range(num_workers)]
 
    # construct a special merger process
    merger = mp.Process(target=merge_counts, args=(out_queue,wordcount_queue,num_workers))
    
    # workers then put dictionaries for the merger
    for worker in workers:
        worker.start()

    # the merger shall return the checksum and top 10 through the out queue
    merger.start()

    # put filenames into the input queue
    for filename in get_filenames(path):
        filename_queue.put(filename)
    # put None into the input queue to signal end of input
    for _ in range(num_workers):
        filename_queue.put(None)

    # wait for all workers to finish
    for worker in workers:
        worker.join()

    # get the checksum and top 10 from the out queue
    checksum = out_queue.get()
    top10 = out_queue.get()

    t2 = time.time()
    # print the results
    print(f'Checksum: {checksum}')
    print(f'Total time with {num_workers} processes: {t2-t1:.2f} seconds')