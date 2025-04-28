#!/usr/bin/env python3

from mrjob.job import MRJob

class MRMineral(MRJob):
    def mapper(self, _, line):
        fields = line.split(',')
        sun = f"{fields[1]} {fields[0]}"
        try:
            mineral_value = int(fields[5])
        except (ValueError):
            return
        yield (sun, mineral_value)

    def reducer(self, sun, values):
        total_value = sum(values)
        yield (sun, total_value)


if __name__ == '__main__':
    MRMineral().run()