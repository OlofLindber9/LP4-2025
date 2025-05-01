 #!/usr/bin/env python3

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRJobTwitterFollowers(MRJob):
    def mapper(self, _, line):
        fields = line.split(':')
        user_id = fields[0]
        followed = [f for f in fields[1].split(' ') if f.strip()]
        for u in followed:
            yield (u, 1)
        yield (user_id, 0)

    def combiner(self, user_id, ones):
        yield (user_id, sum(ones))

    def reducer(self, user_id, counts):
        yield (None, (user_id, sum(counts)))

    def reducer2(self, _, id_and_followers):
        id_and_followers = list(id_and_followers)
        most_followers = max(id_and_followers, key=lambda x: x[1])
        most_followers_id = most_followers[0]
        most_followers_count = most_followers[1]
        total_followers = sum(count for _, count in id_and_followers)
        count = sum(1 for _ in id_and_followers)
        average_followers = total_followers / count if count > 0 else 0
        count_no_followers = sum(1 for _, count in id_and_followers if count == 0)
        
        yield ('most followers id', most_followers_id)
        yield ('most followers', most_followers_count)
        yield ('average followers', average_followers)
        yield ('count no followers', count_no_followers)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                     combiner=self.combiner,
                     reducer=self.reducer),
            MRStep(reducer=self.reducer2)
        ]
    

if __name__ == '__main__':
    MRJobTwitterFollowers.run()

