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

def count_words_in_file(filename):
    """
    Counts the number of occurrences of words in the file
    Whitespace is ignored

    Parameters:
    - filename, string : the path to a file

    Returns: Dictionary that maps words (strings) to counts (ints)
    """
    with open(filename,'r') as f:
        file = f.read()

    counts = dict()

    for word in file.split():
        if word in counts:
            counts[word] += 1
        else:
            counts[word] = 1
    return counts



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




def merge_counts(dict_to, dict_from):
    """
    Merges the word counts from dict_from into dict_to, such that
    if the word exists in dict_to, then the count is added to it,
    otherwise a new entry is created with count from dict_from

    Parameters:
    - dict_to, dictionary : dictionary to merge to
    - dict_from, dictionary : dictionary to merge from

    Return value: None
    """
    for (k,v) in dict_from.items():
        if k not in dict_to:
            dict_to[k] = v
        else:
            dict_to[k] += v



def compute_checksum(counts):
    """
    Computes the checksum for the counts as follows:
    The checksum is the sum of products of the length of the word and its count

    Parameters:
    - counts, dictionary : word to count dictionary

    Return value:
    The checksum (int)
    """
    return sum([len(k) * v for (k,v) in counts])


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
    file_counts = list()
    # Using multiprocessing.Pool, parallelize the loop that counts the words.
    with mp.Pool(num_workers) as p:
        file_counts = p.map(count_words_in_file, get_filenames(path))

    t2 = time.time()
    global_counts = dict()
    for counts in file_counts:
        merge_counts(global_counts,counts)
    
    t3 = time.time()
    top10 = get_top10(global_counts)
    
    t4 = time.time()

    time_count_words = t2 - t1
    time_merge_counts = t3 - t2
    time_get_top10 = t4 - t3
    time_total = t4 - t1
    print(f"Time spent on count_words(): {time_count_words:.2f} seconds")
    print(f"Time spent on merge_counts(): {time_merge_counts:.2f} seconds")
    print(f"Time spent on get_top10(): {time_get_top10:.2f} seconds")
    print(f"Total time: {time_total:.2f} seconds")