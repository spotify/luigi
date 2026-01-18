"""
Example demonstrating DuckDB integration with Luigi.

This example shows how to:
1. Load data into DuckDB tables using CopyToTable
2. Execute queries using DuckDBQuery
3. Chain multiple DuckDB tasks together

You can run this example like this:

    .. code:: console

        $ luigi --module examples.duckdb_example examples.AnalyzeSales --local-scheduler

Requirements:
    - duckdb: pip install duckdb
"""
import luigi
import luigi.contrib.duckdb
from luigi import LocalTarget
import tempfile
import os


class GenerateSalesData(luigi.Task):
    """Generate sample sales data as a TSV file."""

    task_namespace = 'examples'
    date = luigi.DateParameter()

    def output(self):
        return LocalTarget(f'/tmp/sales_data_{self.date}.tsv')

    def run(self):
        # Generate sample sales data
        data = [
            ('2024-01-01', 'Product A', '100.50', '5'),
            ('2024-01-01', 'Product B', '250.00', '3'),
            ('2024-01-01', 'Product C', '75.25', '10'),
            ('2024-01-02', 'Product A', '120.00', '6'),
            ('2024-01-02', 'Product B', '200.00', '2'),
        ]

        with self.output().open('w') as f:
            for row in data:
                f.write('\t'.join(row) + '\n')


class LoadSalesData(luigi.contrib.duckdb.CopyToTable):
    """Load sales data into DuckDB table."""

    task_namespace = 'examples'
    date = luigi.DateParameter()

    # DuckDB configuration
    database = '/tmp/sales_analytics.duckdb'
    table = 'sales'

    # Define table columns
    columns = [
        ('sale_date', 'DATE'),
        ('product_name', 'VARCHAR'),
        ('amount', 'DECIMAL(10,2)'),
        ('quantity', 'INTEGER')
    ]

    def requires(self):
        return GenerateSalesData(date=self.date)

    def rows(self):
        """Read data from input and yield rows."""
        with self.input().open('r') as f:
            for line in f:
                parts = line.strip().split('\t')
                # Convert data types as needed
                yield (parts[0], parts[1], float(parts[2]), int(parts[3]))


class AggregateByProduct(luigi.contrib.duckdb.DuckDBQuery):
    """Aggregate sales by product and store in a summary table."""

    task_namespace = 'examples'
    date = luigi.DateParameter()

    database = '/tmp/sales_analytics.duckdb'
    table = 'product_summary'

    @property
    def query(self):
        return """
            CREATE TABLE IF NOT EXISTS product_summary (
                product_name VARCHAR,
                total_sales DECIMAL(10,2),
                total_quantity INTEGER,
                avg_price DECIMAL(10,2),
                analysis_date DATE
            );

            INSERT INTO product_summary
            SELECT
                product_name,
                SUM(amount) as total_sales,
                SUM(quantity) as total_quantity,
                AVG(amount / quantity) as avg_price,
                CURRENT_DATE as analysis_date
            FROM sales
            WHERE sale_date = '{date}'
            GROUP BY product_name;
        """.format(date=self.date)

    def requires(self):
        return LoadSalesData(date=self.date)


class ExportTopProducts(luigi.Task):
    """Export top products to a file."""

    task_namespace = 'examples'
    date = luigi.DateParameter()

    def requires(self):
        return AggregateByProduct(date=self.date)

    def output(self):
        return LocalTarget(f'/tmp/top_products_{self.date}.txt')

    def run(self):
        import duckdb

        # Connect to DuckDB and query the results
        conn = duckdb.connect('/tmp/sales_analytics.duckdb')

        result = conn.execute("""
            SELECT product_name, total_sales, total_quantity
            FROM product_summary
            WHERE analysis_date = CURRENT_DATE
            ORDER BY total_sales DESC
        """).fetchall()

        # Write results to output file
        with self.output().open('w') as f:
            f.write("Top Products by Sales\n")
            f.write("=" * 50 + "\n")
            for product, sales, quantity in result:
                f.write(f"{product}: ${sales:.2f} ({quantity} units)\n")

        conn.close()


class AnalyzeSales(luigi.WrapperTask):
    """
    Main task that orchestrates the entire pipeline.

    This demonstrates how DuckDB tasks can be chained together
    in a Luigi workflow.
    """

    task_namespace = 'examples'
    date = luigi.DateParameter()

    def requires(self):
        return ExportTopProducts(date=self.date)


# Example of a more complex scenario with multiple tables
class CreateCustomerData(luigi.contrib.duckdb.CopyToTable):
    """Example of loading customer data."""

    task_namespace = 'examples'

    database = '/tmp/sales_analytics.duckdb'
    table = 'customers'

    columns = [
        ('customer_id', 'INTEGER'),
        ('customer_name', 'VARCHAR'),
        ('email', 'VARCHAR'),
        ('signup_date', 'DATE')
    ]

    def rows(self):
        # Example: yield customer data
        customers = [
            (1, 'Alice Smith', 'alice@example.com', '2023-01-15'),
            (2, 'Bob Johnson', 'bob@example.com', '2023-02-20'),
            (3, 'Carol White', 'carol@example.com', '2023-03-10'),
        ]
        for customer in customers:
            yield customer


class JoinSalesWithCustomers(luigi.contrib.duckdb.DuckDBQuery):
    """Example of joining multiple tables."""

    task_namespace = 'examples'
    date = luigi.DateParameter()

    database = '/tmp/sales_analytics.duckdb'
    table = 'sales_by_customer'

    @property
    def query(self):
        return """
            CREATE TABLE IF NOT EXISTS sales_by_customer AS
            SELECT
                c.customer_id,
                c.customer_name,
                s.product_name,
                s.amount,
                s.sale_date
            FROM sales s
            JOIN customers c ON s.customer_id = c.customer_id
            WHERE s.sale_date = '{date}';
        """.format(date=self.date)

    def requires(self):
        return {
            'sales': LoadSalesData(date=self.date),
            'customers': CreateCustomerData()
        }


if __name__ == '__main__':
    import datetime
    luigi.run([
        'examples.AnalyzeSales',
        '--date', str(datetime.date.today()),
        '--local-scheduler'
    ])
