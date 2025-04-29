import luigi
import os
import tempfile
from luigi.contrib.standardize import StandardizeData

class GenerateTestData(luigi.Task):
    """Task to generate sample test data"""
    output_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(self.output_path)

    def run(self):
        # Generate some sample data
        with self.output().open('w') as f:
            for i in range(1, 101):  # Generate 100 data points
                f.write(f"{i}\n")

class AnalyzeStandardizedData(luigi.Task):
    """Task to analyze the standardized data"""
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()

    def requires(self):
        return StandardizeData(
            input_path=self.input_path,
            output_path=self.output_path
        )

    def output(self):
        return luigi.LocalTarget(self.output_path.replace('.csv', '_analysis.txt'))

    def run(self):
        # Read the standardized data
        with self.input().open('r') as f:
            data = [float(line.strip()) for line in f]

        # Perform basic analysis
        mean = sum(data) / len(data)
        variance = sum((x - mean) ** 2 for x in data) / len(data)
        std_dev = variance ** 0.5

        # Write analysis results
        with self.output().open('w') as f:
            f.write(f"Analysis of Standardized Data:\n")
            f.write(f"Number of data points: {len(data)}\n")
            f.write(f"Mean: {mean:.6f}\n")
            f.write(f"Standard Deviation: {std_dev:.6f}\n")
            f.write(f"Min value: {min(data):.6f}\n")
            f.write(f"Max value: {max(data):.6f}\n")

class DataPipeline(luigi.WrapperTask):
    """Main pipeline that orchestrates the data processing workflow"""
    def requires(self):
        # Create temporary directory for test data
        temp_dir = tempfile.mkdtemp()
        raw_data_path = os.path.join(temp_dir, 'raw_data.csv')
        standardized_data_path = os.path.join(temp_dir, 'standardized_data.csv')

        # Generate test data
        yield GenerateTestData(output_path=raw_data_path)

        # Standardize and analyze the data
        yield AnalyzeStandardizedData(
            input_path=raw_data_path,
            output_path=standardized_data_path
        )

if __name__ == "__main__":
    luigi.build([DataPipeline()], local_scheduler=True) 