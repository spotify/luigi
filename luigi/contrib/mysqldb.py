import logging

import luigi

logger = logging.getLogger('luigi-interface')

try:
    import mysql.connector
    from mysql.connector import errorcode
except ImportError as e:
    logger.warning("Loading MySQL module without the python package mysql-connector-python. \
        This will crash at runtime if MySQL functionality is used.")


class MySqlTarget(luigi.Target):
    """Target for a resource in MySql"""

    marker_table = luigi.configuration.get_config().get('mysql', 'marker-table', 'table_updates')

    def __init__(self, host, database, user, password, table, update_id):
        """
        Args:
            host (str): MySql server address. Possibly a host:port string.
            database (str): Database name
            user (str): Database user
            password (str): Password for specified user
            update_id (str): An identifier for this data set

        """
        if ':' in host:
            self.host, self.port = host.split(':')
            self.port = int(self.port)
        else:
            self.host = host
            self.port = 3306
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.update_id = update_id

    def touch(self, connection=None):
        """Mark this update as complete.

        Important: If the marker table doesn't exist, the connection transaction will be aborted
        and the connection reset. Then the marker table will be created.
        """
        self.create_marker_table()

        if connection is None:
            connection = self.connect()
            connection.autocommit = True  # if connection created here, we commit it here

        connection.cursor().execute(
            """INSERT INTO {marker_table} (update_id, target_table)
               VALUES (%s, %s)
               ON DUPLICATE KEY UPDATE
               update_id = VALUES(update_id)
            """.format(marker_table=self.marker_table),
            (self.update_id, self.table)
        )
        # make sure update is properly marked
        assert self.exists(connection)

    def exists(self, connection=None):
        if connection is None:
            connection = self.connect()
            connection.autocommit = True
        cursor = connection.cursor()
        try:
            cursor.execute("""SELECT 1 FROM {marker_table}
                WHERE update_id = %s
                LIMIT 1""".format(marker_table=self.marker_table),
                (self.update_id,)
            )
            row = cursor.fetchone()
        except mysql.connector.Error as e:
            if e.errno ==  errorcode.ER_NO_SUCH_TABLE:
                row = None
            else:
                raise
        return row is not None

    def connect(self, autocommit=False):
        connection = mysql.connector.connect(user=self.user,
                                             password=self.password,
                                             host=self.host,
                                             port=self.port,
                                             database=self.database,
                                             autocommit=autocommit)
        return connection

    def create_marker_table(self):
        """Create marker table if it doesn't exist.

        Using a separate connection since the transaction might have to be reset"""
        connection = self.connect(autocommit=True)
        cursor = connection.cursor()
        try:
            cursor.execute(
                """ CREATE TABLE {marker_table} (
                        id            BIGINT(20)    NOT NULL AUTO_INCREMENT,
                        update_id     VARCHAR(128)  NOT NULL,
                        target_table  VARCHAR(128),
                        inserted      TIMESTAMP DEFAULT NOW(),
                        PRIMARY KEY (update_id),
                        KEY id (id)
                    )
                """
                .format(marker_table=self.marker_table)
            )
        except mysql.connector.Error as e:
            if e.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                pass
            else:
                raise
        connection.close()
