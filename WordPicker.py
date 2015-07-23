import luigi
import random

class WordPicker(luigi.Task):

    def output(self):
        return luigi.LocalTarget('output/word-picker/word.txt')
    
    def run(self):
        words = ["purple", "blue", "tooth", "beachball"]
        index = random.randint(0,3)
        chosen_word = words[index]
        f = self.output().open('w')
        f.write(chosen_word)
class RefFile(luigi.Task):
    """Read a 'reference' file from disk"""
    def output(self):
        return luigi.LocalTarget("output/word-picker/aRefFile.txt")

class FlipWordBackwards(luigi.Task):
    def requires(self):
        return RefFile()

    def output(self):
        return luigi.LocalTarget('output/word-picker/reversed_word.txt')

    def run(self):
        print self.input()
        with self.input().open('r') as raw_word:
            word = raw_word.read()
            print word 

if __name__ == "__main__":
    luigi.run()
