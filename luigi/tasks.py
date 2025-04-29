import luigi
from luigi import LocalTarget
from .standardize import standardize

class StandardizeData(luigi.Task):
    """Standardizes numerical data in a file to have mean 0 and std 1."""
    
    input_path = luigi.Parameter(description="Path to input data file")
    output_path = luigi.Parameter(description="Path to save standardized data")
    
    def requires(self):
        """Define dependencies (e.g., a task that generates input_path)"""
        return []  # Replace with another task if needed
    
    def output(self):
        """Define output target"""
        return LocalTarget(self.output_path)
    
    def run(self):
        """Run standardization logic"""
        with open(self.input_path, 'r') as f:
            data = [float(line.strip()) for line in f]
        
        # Apply standardization
        standardized_data = standardize(data)
        
        # Write output
        with self.output().open('w') as f:
            for value in standardized_data:
                f.write(f"{value}\n") 