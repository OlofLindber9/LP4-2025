#!/usr/bin/env python3

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMineral(MRJob):
    def configure_args(self):
        super(MRMineral, self).configure_args()
        self.add_passthru_arg(
            '-k', type=int, default=10, 
            help='Number of most valuable systems to display.')

    def mapper(self, _, line):
        fields = line.split(',')
        if fields[1] == 'Prime':
            sun = fields[0]
        else:
            sun = f"{fields[1]} {fields[0]}"
        try:
            mineral_value = int(fields[5])
        except (ValueError):
            return
        yield (sun, mineral_value)
    
    def combiner(self, sun, resource_units):
        total_ru = sum(resource_units)
        yield (sun, total_ru)

    def reducer(self, sun, resource_units):
        total_ru = sum(resource_units)
        yield (None, (sun, total_ru))

    def reducer2(self, _, sun_and_ru):
        top_k = [(None, -1)] * self.options.k

        for sun, ru in sun_and_ru:
            if ru > top_k[-1][1]:
                top_k[-1] = (sun, ru)
                top_k.sort(key=lambda x:x[1], reverse=True)
        
        yield (f'top {self.options.k}', top_k)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                    combiner=self.combiner,
                    reducer=self.reducer),
            MRStep(reducer=self.reducer2)
        ]

if __name__ == '__main__':
    MRMineral().run()