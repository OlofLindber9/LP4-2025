#!/usr/bin/env python3

from mrjob.job import MRJob
import csv
from io import StringIO

class MRMineral(MRJob):
    def mapper(self, _, line):
        #f = StringIO(line)
        reader = csv.reader(line)
        for row in reader:
            if row[0] == 'Constellation':
                return
            Constellation, Star, Planet, Type, Bio_units, Ru, Mineral_volume, Tilt, Density, Radius, Gravity, Temperature, Day, Pressure, Orbit = row
            yield Star + Constellation, int(Ru)

    def reducer(self, StarConstellation, Ru):
        yield(StarConstellation,sum(Ru))

if __name__ == '__main__':
    MRMineral().run()