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
    return (user_id,following)

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
    
    lines = lines.flatMap(lambda pair: [(pair[0],0) + (y,1) for y in pair[1]])
    lines = lines.reduceByKey(lambda a,b: a + b)
    max_followers = lines.sortBy(lambda x: x[1], ascending=False).first()

    total_followers = lines.map(lambda x: x[1]).reduce(lambda x, y: x + y)
    count = lines.count()
    average_followers = total_followers / count if count > 0 else 0

    count_no_followers = lines.filter(lambda x: x[1] == 0).count()   
   
    end = time.time()
    
    total_time = end - start

    # the first ??? should be the twitter id
    print(f'max followers: {max_followers[0]} has {max_followers[1]} followers')
    print(f'followers on average: {average_followers}')
    print(f'number of user with no followers: {count_no_followers}')
    print(f'num workers: {args.num_workers}')
    print(f'total time: {total_time}')

