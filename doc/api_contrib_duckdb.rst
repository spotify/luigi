DuckDB Integration (luigi.contrib.duckdb)
==========================================

The DuckDB integration module provides Luigi tasks and targets for working with
DuckDB databases. DuckDB is an in-process SQL OLAP database management system
designed for analytical query workloads.

Overview
--------

DuckDB is an embedded database similar to SQLite but optimized for analytics.
Unlike client-server databases like PostgreSQL or MySQL, DuckDB runs in-process
within your application and stores data in local files (or in-memory).

This makes it an excellent choice for:

* Analytical workloads and data processing pipelines
* Local development and testing
* Single-node data analysis
* Embedded analytics in applications

Features
--------

The luigi.contrib.duckdb module provides three main components:

1. **DuckDBTarget**: A Target class that tracks task completion in a DuckDB marker table
2. **CopyToTable**: A task template for bulk loading data into DuckDB tables
3. **DuckDBQuery**: A task template for executing SQL queries against DuckDB

Installation
------------

To use the DuckDB integration, you need to install the duckdb Python package:

.. code-block:: bash

    pip install duckdb

Configuration
-------------

DuckDB configuration can be specified in your luigi.cfg file:

.. code-block:: ini

    [duckdb]
    marker-table = table_updates

The marker table is used to track which tasks have been completed.

Usage Examples
--------------

Basic CopyToTable Task
~~~~~~~~~~~~~~~~~~~~~~~

Here's a simple example of loading data into a DuckDB table:

.. code-block:: python

    import luigi
    from luigi.contrib import duckdb

    class LoadUserData(duckdb.CopyToTable):
        date = luigi.DateParameter()

        # Path to DuckDB database file
        database = '/path/to/analytics.duckdb'

        # Table name
        table = 'users'

        # Define table schema
        columns = [
            ('user_id', 'INTEGER'),
            ('username', 'VARCHAR'),
            ('email', 'VARCHAR'),
            ('created_at', 'TIMESTAMP')
        ]

        def rows(self):
            # Yield rows to insert
            with self.input().open('r') as f:
                for line in f:
                    parts = line.strip().split('\\t')
                    yield parts

Using DuckDBQuery
~~~~~~~~~~~~~~~~~

Execute SQL queries as Luigi tasks:

.. code-block:: python

    class AggregateUserStats(duckdb.DuckDBQuery):
        date = luigi.DateParameter()

        database = '/path/to/analytics.duckdb'
        table = 'user_stats'

        @property
        def query(self):
            return f'''
                CREATE TABLE IF NOT EXISTS user_stats AS
                SELECT
                    DATE_TRUNC('day', created_at) as signup_date,
                    COUNT(*) as user_count,
                    COUNT(DISTINCT email) as unique_emails
                FROM users
                WHERE DATE(created_at) = '{self.date}'
                GROUP BY signup_date
            '''

        def requires(self):
            return LoadUserData(date=self.date)

In-Memory Databases
~~~~~~~~~~~~~~~~~~~

For testing or temporary data processing, you can use an in-memory database:

.. code-block:: python

    class ProcessData(duckdb.CopyToTable):
        database = ':memory:'  # Use in-memory database
        table = 'temp_data'
        columns = [('id', 'INTEGER'), ('value', 'DOUBLE')]

        def rows(self):
            for i in range(100):
                yield (i, i * 1.5)

Chaining Multiple Tasks
~~~~~~~~~~~~~~~~~~~~~~~

Build complex data pipelines by chaining DuckDB tasks:

.. code-block:: python

    class LoadRawData(duckdb.CopyToTable):
        database = 'pipeline.duckdb'
        table = 'raw_data'
        columns = [('id', 'INTEGER'), ('value', 'VARCHAR')]

        def rows(self):
            # Load raw data
            pass

    class TransformData(duckdb.DuckDBQuery):
        database = 'pipeline.duckdb'
        table = 'transformed_data'

        @property
        def query(self):
            return '''
                CREATE TABLE transformed_data AS
                SELECT id, UPPER(value) as value_upper
                FROM raw_data
            '''

        def requires(self):
            return LoadRawData()

    class ExportResults(luigi.Task):
        def requires(self):
            return TransformData()

        def output(self):
            return luigi.LocalTarget('results.csv')

        def run(self):
            import duckdb
            conn = duckdb.connect('pipeline.duckdb')
            result = conn.execute('SELECT * FROM transformed_data').fetchdf()
            result.to_csv(self.output().path, index=False)
            conn.close()

API Reference
-------------

DuckDBTarget
~~~~~~~~~~~~

.. autoclass:: luigi.contrib.duckdb.DuckDBTarget
   :members:
   :inherited-members:

CopyToTable
~~~~~~~~~~~

.. autoclass:: luigi.contrib.duckdb.CopyToTable
   :members:
   :inherited-members:
   :exclude-members: host, user, password

   .. note::
      The `host`, `user`, and `password` properties are not applicable for DuckDB
      as it is an embedded database. Only the `database` property (file path) is required.

DuckDBQuery
~~~~~~~~~~~

.. autoclass:: luigi.contrib.duckdb.DuckDBQuery
   :members:
   :inherited-members:
   :exclude-members: host, user, password

   .. note::
      The `host`, `user`, and `password` properties are not applicable for DuckDB
      as it is an embedded database. Only the `database` property (file path) is required.

Comparison with Other Database Modules
---------------------------------------

DuckDB vs PostgreSQL/MySQL
~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When to use DuckDB:**

* Single-node analytical workloads
* Local development and testing
* Embedded analytics
* No need for concurrent writes from multiple processes

**When to use PostgreSQL/MySQL:**

* Multi-user applications
* Distributed systems
* Need for replication and high availability
* Transactional workloads (OLTP)

DuckDB Advantages
~~~~~~~~~~~~~~~~~

* **No server setup**: Embedded database, no daemon to manage
* **Fast analytics**: Optimized for OLAP queries
* **Easy deployment**: Single file database, easy to backup and share
* **Great for prototyping**: Quick to set up for data analysis

Best Practices
--------------

1. **Use appropriate database paths**: Store DuckDB files in appropriate locations with proper permissions

2. **Leverage DuckDB's analytics features**: DuckDB excels at analytical queries, window functions, and complex aggregations

3. **Consider concurrency**: DuckDB is optimized for single-writer scenarios. For multiple concurrent writers, consider PostgreSQL

4. **Batch your inserts**: Use the bulk insert capabilities for better performance

5. **Clean up marker tables**: Periodically clean old entries from the marker table to keep it efficient

Example Workflows
-----------------

See the `examples/duckdb_example.py` file for a complete working example
demonstrating a data pipeline with multiple DuckDB tasks.
