 #!/usr/bin/env python3

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRJobTwitterFollows(MRJob):
    # The final (key,value) pairs returned by the class should be
    # 
    # yield ('most followed id', ???)
    # yield ('most followed', ???)
    # yield ('average followed', ???)
    # yield ('count follows no-one', ???)
    #
    # You will, of course, need to replace ??? with a suitable expression
    def mapper(self, _, line):
        fields = line.split(':')
        user = fields[0]
        follows = fields[1]
        #print(follows)
        follows = [f for f in follows.split(",") if f.strip()]
        number_of_followed_accounts = len(follows)
        yield (user, number_of_followed_accounts)
    
    def reducer(self, user, number_of_followed_accounts):
        total = sum(number_of_followed_accounts)
        yield (None, (user, total))

    def reducer2(self, _, user_and_number_of_followed_accounts):
        data = list(user_and_number_of_followed_accounts)
        follows_no_one = 0
        total_follows = 0
        data.sort(key=lambda x:x[1], reverse=True)
        for user, number_of_followed_accounts in data:
            if number_of_followed_accounts == 0:
                follows_no_one += 1
            total_follows += number_of_followed_accounts
        print(len(data))
        print(total_follows)
        print(follows_no_one)
        yield ('most followed id', data[0][0])
        yield ('most followed', data[0][1])
        yield ('average followed', total_follows/len(data))
        yield ('count follows no-one', follows_no_one)
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                    reducer=self.reducer),
            MRStep(reducer=self.reducer2)
        ]

if __name__ == '__main__':
    MRJobTwitterFollows.run()

