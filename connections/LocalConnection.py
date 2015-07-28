""" Definitions needed for running in local db """

import luigi
import logging
import pyodbc


logger = logging.getLogger('luigi-interface')


class LocalDBConnection(object):
    """
    Connection to a local MySql box
    """

    def __init__(self, driver, server, port, database, user, password):
        """
        TODO: docs
        """
        self.driver = driver
        self.server = server
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.connection = None


    def connect(self):
        """
        TODO: docs
        """
        connection_string = 'Driver={{{driver}}};Server={server};\
                             Port={port};Database={database};\
                             User={user};Password={password}'\
                             .format(driver=self.driver,
                                     server=self.server,
                                     port=self.port,
                                     database=self.database,
                                     user=self.user,
                                     password=self.password)

        cnx = mysql.connector.connect(user=CONFIG['user'],
                            host=CONFIG['server'],
                            database=CONFIG['database'])
self.connection = pyodbc.connect(connection_string)
        return self.connection

    def close(self):
        """
        TODO: docs
        """
        if self.connection is not None:
            return self.connection.close()
        else:
            return None


class LocalDBTarget(luigi.Target):
    """
    Target for a resource in local db
    """

    def __init__(self, local_db_connection, schema, table, key, value):
        """
        TODO: docs
        """
        self.connection = local_db_connection
        self.schema = schema
        self.table = table
        self.key = key
        self.value = value


    def create_marker_table(self):
        """
        TODO: docs
        """
        connection = self.connection.connect()
        cursor = connection.cursor()
        cursor.execute(
            """
                CREATE TABLE IF NOT EXISTS markers (
                    id              BIGINT(20)    NOT NULL AUTO_INCREMENT
                    , schema_name   VARCHAR(128)  NOT NULL
                    , table_name    VARCHAR(128)  NOT NULL
                    , mark_key      VARCHAR(128)  NOT NULL
                    , mark_value    VARCHAR(128)  NOT NULL
                    , inserted      TIMESTAMP DEFAULT NOW()
                    , PRIMARY KEY (id)
                    , KEY idx_table_mark (schema_name, table_name, mark_key)
                );
            """)
        connection.commit()
        connection.close()


    def exists(self):
        """
        TODO: docs
        """
        # create the marker table if it doesn't exist already
        self.create_marker_table()
        # connect to local db
        connection = self.connection.connect()
        cursor = connection.cursor()
        # check that the key, value pair exists against the object of interest
        cursor.execute(
            """
                SELECT 1
                FROM markers
                WHERE
                    schema_name = '{schema}'
                    AND table_name = '{table}'
                    AND mark_key = '{key}'
                    AND mark_value = '{value}';
            """.format(schema=self.schema,
                       table=self.table,
                       key=self.key,
                       value=self.value))
        row = cursor.fetchone()
        connection.close()
        return row is not None


    def mark_table(self):
        """
        TODO: docs
        """
        # create the table if it doesn't already
        self.create_marker_table()
        # connect to local db
        connection = self.connection.connect()
        cursor = connection.cursor()
        # insert a mark against the table
        cursor.execute(
            """
                INSERT INTO markers (schema_name, table_name, mark_key, mark_value)
                VALUES ('{schema}', '{table}', '{key}', '{value}');
            """.format(schema=self.schema,
                       table=self.table,
                       key=self.key,
                       value=self.value)
            )
        connection.commit()
        # close the connection
        connection.close()
