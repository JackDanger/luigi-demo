import inspect, os
import luigi
import random
import json 
#from connections import LocalConnection 
import mysql.connector

def line():
    """Returns the current line number in our program."""
    return inspect.currentframe().f_back.f_lineno

print line()

###

# get the current working directory
CURRENT_FILE = os.path.abspath(inspect.getfile(inspect.currentframe()))
CURRENT_PATH = os.path.dirname(CURRENT_FILE)

print line()
# load a config file
with open(CURRENT_PATH + '/config.json', 'r') as file_buff:
    CONFIG = json.loads(file_buff.read())

print line()
# create the localdb connection object

cnx = mysql.connector.connect(user=CONFIG['user'],
                            host=CONFIG['server'],
                            database=CONFIG['database'])

#local_db = LocalConnection.LocalDBConnection(driver=CONFIG['driver'],
                                    #server=CONFIG['server'],
                                     #port=CONFIG['port'],
                                     #database=CONFIG['database'],
                                     #user=CONFIG['user'],
                                     #password=CONFIG['password'])

print line()

# Drop the markers table if it exists
connection = cnx
print line()
cursor = connection.cursor()
print line()
cursor.execute("CREATE TABLE my_test SELECT * FROM luigid.markers;")
print line()
connection.commit()
print line()
connection.close()
print line()


#####

class WordPicker(luigi.Task):
    def output(self):
        return luigi.LocalTarget('./output/word-picker/word.txt')

    def run(self):
        words = ["purple", "blue", "tooth", "beachball"]
        index = random.randint(0,3)
        chosen_word = words[index]

        with self.output().open('w') as f:
            f.write(chosen_word)

class FlipWordBackwards(luigi.Task):
    def requires(self):
        return WordPicker()

    def output(self):
        return luigi.LocalTarget('./output/word-picker/reversed_word.txt')

    def run(self):
        print self.input()
        with self.input().open('r') as raw_word:
            word = raw_word.read()

        with self.output().open('w') as f:
            f.write('I just reversed {0}'.format(word[::-1]))

if __name__ == "__main__":
    luigi.run()
