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
        f.close()

if __name__ == "__main__":
    luigi.run()
