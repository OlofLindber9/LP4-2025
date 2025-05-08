 #!/usr/bin/env python3

from mrjob.job import MRJob

class MRJobTwitterFollows(MRJob):
    def mapper(self, _, line):
        fields = line.split(':')
        user_id = fields[0]
        following = [f for f in fields[1].split(' ') if f.strip()]
        yield (None, (user_id, len(following)))

    def reducer(self, _, id_and_followers):
        id_and_followers = list(id_and_followers)
        most_followed = sorted(id_and_followers, key=lambda x: x[1], reverse=True)[0]
        most_followed_id = most_followed[0]
        most_followed_count = most_followed[1]
        total_followers = sum(count for _, count in id_and_followers)
        count = sum(1 for _ in id_and_followers)
        average_followers = total_followers / count if count > 0 else 0
        count_no_followers = sum(1 for _, count in id_and_followers if count == 0)
        yield ('most followed id', most_followed_id)
        yield ('most followed', most_followed_count)
        yield ('average followers', average_followers)
        yield ('count follows no-one', count_no_followers)

if __name__ == '__main__':
    MRJobTwitterFollows.run()

