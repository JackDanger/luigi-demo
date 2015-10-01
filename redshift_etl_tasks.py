import mysql.connector
import luigi
from luigi.contrib.redshift import RedshiftTarget

import time
import yaml
import datetime

import subprocess
import os
import re

class MySQL:
    connection = None

    @staticmethod
    def execute(sql):
        if self.connection is None:
            self.connection = mysql.connector.connect(user='user', host='localhost', database='luigid')
        self.connection.connect()
        cursor = self.connection.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        self.connection.commit()
        return row

    @staticmethod
    def execute_yaml_file(filename):
        with open(filename, 'r') as ymlfile:
            data = yaml.load(ymlfile)
            return MySQL.execute(data['sql'])

class Redshift:
    @staticmethod
    def fetchone(sql):
        return Redshift.execute(sql).fetchone()

    @staticmethod
    def fetchall(sql):
        return Redshift.execute(sql).fetchall()

    @staticmethod
    def execute(sql):
        cursor = Redshift.cursor()
        print "Executing:", sql
        cursor.execute(sql)
        return cursor

    @staticmethod
    def cursor():
        """Connects to Redshift, returns a cursor for the connection"""
        env = os.environ['ENVIRONMENT'] if 'ENVIRONMENT' in os.environ else 'development'
        with open('database.yml', 'r') as ymlfile:
            dbconfig = yaml.load(ymlfile)[env]
        connection = RedshiftTarget(**dbconfig).connect()
        connection.autocommit = True
        return connection.cursor()

class Markers(luigi.Target):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def create_marker_table(self):
        Redshift.execute(
            """
                CREATE TABLE IF NOT EXISTS markers (
                      mark_key      VARCHAR(128)  NOT NULL
                    , mark_value    VARCHAR(128)  NOT NULL
                    , inserted      TIMESTAMP DEFAULT NOW()
                    , PRIMARY KEY (mark_key, mark_value)
                );
            """)


    def exists(self):
        self.create_marker_table()
        return Redshift.fetchone(
            """
                SELECT 1
                FROM markers
                WHERE
                    mark_key = '{key}'
                    AND mark_value = '{value}';
            """.format(key=self.key,
                       value=self.value))

    def mark_table(self):
        self.create_marker_table()
        Redshift.execute(
            """
                INSERT INTO markers (mark_key, mark_value, inserted)
                VALUES ('{key}', '{value}', '{time}'::TIMESTAMP);
            """.format(key=self.key,
                       value=self.value,
                       time=datetime.datetime.utcnow()))

    @staticmethod
    def truncate():
        """Dangerous!: Removes all marker entries"""
        return Redshift.execute(
            """
                TRUNCATE markers;
            """
        )


class HsqlTask(luigi.Task):
    file = luigi.Parameter(significant=False)
    environment = luigi.Parameter(default='development')
    # An hour timestamp specified in the following format:
    #   2015-06-02T00 (midnight UTC on June 2nd)
    #   2015-08-04T17 (1700 hours UTC on August 4th)
    hour = luigi.DateHourParameter(default=datetime.datetime.utcnow())

    #def __init__(self, *args, **kwargs):
    #    # Customize the task name

    #    # Set a custom task_family instead of the class name
    #    self.task_family = HsqlTask.file_to_id(kwargs['file'])

    #    # This is mostly copied from luigi/task.py's __init__
    #    param_values = self.get_param_values(self.get_params(), args, kwargs)
    #    task_id_parts = []
    #    for name, value in param_values:
    #        if name is not 'file':
    #            task_id_parts.append('%s=%s' % (name, value))
    #    self.task_id = '%s(%s)' % (self.task_family, ', '.join(task_id_parts))

    #    super(HsqlTask, self).__init__(*args, **kwargs)

    def output(self):
        if self.schedule() == 'daily':
            key = self.hour.strftime('%Y-%m-%d')
        else:
            key = self.hour.strftime('%Y-%m-%dT%H')
        return Markers(self.file, key)

    def requires(self):
        """
        If there are one or more dependant files specified in the YAML header
        of the .sql file they'll be instantiated here as tasks and returned.
        """
        c = self.config()
        if 'requires' in c:
            return [self._subtask(dependency) for dependency in c['requires']]

    def run(self):
        for index in xrange(len(self.queries())):
            query = self.queries()[index]
            print "Executing query #" + str(index) + " of " + self.file
            start = datetime.datetime.utcnow()

            print Redshift.execute(query)

            end = datetime.datetime.utcnow()
            print "-> in " + str((end - start).total_seconds()) + " seconds"

        self.output().mark_table()

    # Customize the naming of the task
    @property
    def task_family(self):
        return HsqlTask.file_to_id(self.file)

    def queries(self):
        """A list of SQL queries as defined in the .sql file (and parsed by hsql)"""
        self._parse()
        return getattr(self, '_queries')

    def config(self):
        """The YAML front matter of the .sql file"""
        self._parse()
        return getattr(self, '_config')

    def schedule(self):
        """The 'schedule:' key from the YAML front matter of the .sql file"""
        if 'schedule' in self.config():
            return self.config()['schedule']

    def _subtask(self, otherfile):
        """
        Instantiate another HsqlTask for a .sql file this task depends on.
        """
        dir = os.path.dirname(self.file)
        otherpath = os.path.join(dir, otherfile) + '.sql'
        task = HsqlTask(file=otherpath, environment=self.environment, hour=self.hour)
        # Hourly tasks cannot depend on daily tasks, the semantics are too confusing
        # (specifically, does a 2pm task need today's (impossible) or yesterday's (stale) data?)
        if self.schedule() == 'hourly':
            if task.schedule() == 'daily':
                raise AssertionError(
                    """
                      Hourly tasks ({t1}) cannot depend on daily tasks ({t2})
                    """.format(
                        t1=self.file,
                        t2=task.file))
            # It's a valid hourly task
            return task

        # If a daily task depends on an hourly task we ensure that the task has
        # been completed for every hour of the given day.
        # We do this by instantiating a task for each our and returning the
        # whole list, Luigi takes care of the task resolution.
        if self.schedule() == 'daily':
            if task.schedule() == 'hourly':
                print 'going'
                return [HsqlTask(file=otherpath,
                                 environment=self.environment,
                                 hour=hour) for hour in self.hours()]
            else:
                return task;

    def hours(self):
        """Returns a list of hour stamps within the given day"""
        h = self.hour
        hh = [datetime.datetime(h.year, h.month, h.day, hour) for hour in range(24)]
        print hh
        return hh


    def _parse(self):
        """
        Executes just one time to read the .sql file and any associated config
        """
        if hasattr(self, '_queries'):
            return

        # Read the YAML front matter from the file
        setattr(self, '_config', yaml.load(subprocess.check_output([
            'hsql', self.file,
                    '--env', self.environment,
                    '--yaml'
        ])))

        # Read the normalized queries from the file
        setattr(self, '_queries', subprocess.check_output([
            'hsql', self.file,
                    '--env', self.environment,
                    '--date', "'" + self.hour.isoformat() + " UTC'"
        ]).splitlines())


    @staticmethod
    def file_to_id(filename):
        """
            This supports prettier naming of the .sql files. A file named
            'summaries/payments/daily_executive_report.sql'
            Will be a task visible in the UI as
            'Summaries/Payments/DailyExecutiveReport'
        """
        id = filename.replace('.sql', '')
        parts = id.split('/')
        id = "/".join("".join(x.capitalize() for x in part.split('_')) for part in parts)
        return id.strip()

if __name__ == "__main__":
    luigi.run()
