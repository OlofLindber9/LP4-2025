#!/usr/bin/env python3

import time
import argparse
import findspark
findspark.init()
from pyspark import SparkContext

def parse_line(line):
    """
    Parses string line into (user_id, [following_id]) entries.
    The input line is expected to be in the format:
    user_id: following_id1 following_id2 following_id3
    """
    fields = line.split(':')
    user_id = fields[0]
    following = [f for f in fields[1].split(' ') if f.strip()]
    return (user_id, len(following))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = \
                                    'Compute Twitter followers.')
    parser.add_argument('-w','--num-workers',default=1,type=int,
                            help = 'Number of workers')
    parser.add_argument('filename',type=str,help='Input filename')
    args = parser.parse_args()

    start = time.time()
    sc = SparkContext(master = f'local[{args.num_workers}]')

    lines = sc.textFile(args.filename)

    # Parse the input data
    followed = lines.map(parse_line).cache()

    # Calculate the user who follows the most people
    max_followed = followed.reduce(lambda x, y: x if x[1] >= y[1] else y)
    
    # Calculate the average number of people followed
    (sum_followed, total_users) = followed.aggregate((0, 0),
        lambda acc, x: (acc[0] + x[1], acc[1] + 1),
        lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
    )
    average_followed = sum_followed / total_users if total_users > 0 else 0

    # Calculate the number of accounts that follow no-one
    count_no_followed = followed.filter(lambda x: x[1] == 0).count()

    end = time.time()
    
    total_time = end - start

    print(f'max follows: {max_followed[0]} follows {max_followed[1]}')
    print(f'users follow on average: {average_followed}')
    print(f'number of user who follow no-one: {count_no_followed}')
    print(f'num workers: {args.num_workers}')
    print(f'total time: {total_time} seconds')
