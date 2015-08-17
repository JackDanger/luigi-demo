import luigi
import random
import time
import random
from luigi import six
from collections import defaultdict

class Streams(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget("./output/fake_stream_%s.txt" % self.date)

    def run(self):
        with self.output().open('w') as output:
            for _ in range(1000):
                output.write('{} {} {}\n'.format(
                    random.randint(0, 999),
                    random.randint(0, 999),
                    random.randint(0, 999)))


class DateWriter(luigi.Task):
    date_interval = luigi.DateIntervalParameter()

    def requires(self):
        return [Streams(date) for date in self.date_interval]

    def output(self):
        return luigi.LocalTarget("./output/streams_%s.tsv" % self.date_interval)

    def run(self):
        count = defaultdict(int)
        for s in self.input():
            with s.open('r') as in_file:
                for line in in_file:
                    _, key, value = line.strip().split()
                    count[key] += 1
        with self.output().open('w') as out_file:
            for key, value in six.iteritems(count):
                out_file.write('{},{}\n'.format(key, value))


class WordPicker(luigi.Task):
    def output(self):
        return luigi.LocalTarget('./output/word-picker/word' + str(time.time()) + '.txt')

    def run(self):
        time.sleep(1)
        words = ["purple", "blue", "tooth", "beachball"]
        index = random.randint(0,3)
        chosen_word = words[index]

        with self.output().open('w') as f:
            f.write(chosen_word)

class FlipWordBackwards(luigi.Task):
    def requires(self):
        return WordPicker()

    def output(self):
        return luigi.LocalTarget('./output/word-picker/reversed_word' + str(time.time()) + '.txt')

    def run(self):
        time.sleep(1)
        print self.input()
        with self.input().open('r') as raw_word:
            word = raw_word.read()

        with self.output().open('w') as f:
            f.write('I just reversed {0}'.format(word[::-1]))

if __name__ == "__main__":
    luigi.run()
