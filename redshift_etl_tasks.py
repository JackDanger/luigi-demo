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
    @staticmethod
    def execute(sql):
        connection = mysql.connector.connect(user='user', host='localhost', database='luigid')
        connection.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        row = cursor.fetchone()
        connection.commit()
        connection.close()
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
        cursor.execute(sql)
        return cursor

    @staticmethod
    def cursor():
        """Connects to Redshift, returns a cursor for the connection"""
        with open('database.yml', 'r') as ymlfile:
            dbconfig = yaml.load(ymlfile)['localhost']
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
                    id              BIGSERIAL
                    , mark_key      VARCHAR(128)  NOT NULL
                    , mark_value    VARCHAR(128)  NOT NULL
                    , inserted      TIMESTAMP DEFAULT NOW()
                    , PRIMARY KEY (id)
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
                INSERT INTO markers (mark_key, mark_value)
                VALUES ('{key}', '{value}');
            """.format(key=self.key,
                       value=self.value))

    @staticmethod
    def truncate():
        """Remove all marker entries"""
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
    queries = None
    config = None
    task_id = 'hsql'

    def __init__(self, *args, **kwargs):
        # Customize the task name

        # Set a custom task_family instead of the class name
        self.task_family = HsqlTask.file_to_id(kwargs['file'])

        # This is mostly copied from luigi/task.py's __init__
        param_values = self.get_param_values(self.get_params(), args, kwargs)
        task_id_parts = []
        for name, value in param_values:
            if name is not 'file':
                task_id_parts.append('%s=%s' % (name, value))
        self.task_id = '%s(%s)' % (self.task_family, ', '.join(task_id_parts))

        super(HsqlTask, self).__init__(*args, **kwargs)

    def output(self):
        return Markers(self.file, self._hourstamp())

    def requires(self):
        """
        If there are one or more dependant files specified in the YAML header
        of the .sql file they'll be instantiated here as tasks and returned.
        """
        self._parse()
        if 'requires' in self.config:
            return [self._subtask(dependency) for dependency in self.config['requires']]

    def run(self):
        self._parse()
        for query in self.queries:
            Redshift.execute(query)
        self.output().mark_table()

    # Customize the naming of the task
    def task_family(self):
        return self.task_family

    def _subtask(self, otherfile):
        """
        Instantiate another HsqlTask for a different .sql file
        """
        dir = os.path.dirname(self.file)
        otherpath = os.path.join(dir, otherfile) + '.sql'
        return HsqlTask(file=otherpath, environment=self.environment)

    def _hourstamp(self):
        return self.hour.strftime('%Y-%m-%dT%H')

    def _parse(self):
        """
        Executes just one time to read the .sql file and any associated config
        """
        if self.queries:
            return

        # Read the YAML front matter from the file
        self.config = yaml.load(subprocess.check_output([
            'hsql', self.file,
                    '--env', self.environment,
                    '--yaml'
        ]))

        # Read the normalized queries from the file
        self.queries = subprocess.check_output([
            'hsql', self.file,
                    '--env', self.environment,
                    '--date', "'" + self._hourstamp() + "'"
        ]).splitlines()


    @staticmethod
    def file_to_id(filename):
        id = filename.replace('.sql', '')
        parts = id.split('/')
        id = "/".join("".join(x.capitalize() for x in part.split('_')) for part in parts)
        return id.strip()

if __name__ == "__main__":
    print Markers.truncate()
    luigi.run()
