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
    lines = lines.map(parse_line)

    # Calculate the maximum number of people followed
    # and the id of the account with the maximum number of people followed
    max_followed = lines.sortBy(lambda x: x[1], ascending=False).first()
    max_followed_id = max_followed[0]
    max_followed_count = max_followed[1]
    # id of the account with the maximum number of people followed
    # the average number of people followed
    total_followers = lines.map(lambda x: x[1]).reduce(lambda x, y: x + y)
    count = lines.count()
    average_followers = total_followers / count if count > 0 else 0
    #  the number of accounts that follow no-one
    count_no_followers = lines.filter(lambda x: x[1] == 0).count()    
    end = time.time()
    
    total_time = end - start

    print(f'max followers: {max_followed_id} has {max_followed_count} followers')
    print(f'followers on average: {average_followers}')
    print(f'number of user with no followers: {count_no_followers}')
    print(f'num workers: {args.num_workers}')
    print(f'total time: {total_time} seconds')

