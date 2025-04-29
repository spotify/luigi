import numpy as np
import luigi

def standardize(data):
    """
    Standardize a list of numeric values to have mean 0 and standard deviation 1.
    
    Args:
        data (list): List of numeric values to standardize
        
    Returns:
        list: Standardized values
    """
    if not data:
        return []
    
    if len(data) == 1 or len(set(data)) == 1:
        return [0] * len(data)
    
    data_array = np.array(data)
    mean = np.mean(data_array)
    std = np.std(data_array)
    
    return list((data_array - mean) / std)

class StandardizeData(luigi.Task):
    """Task to standardize data from an input file."""
    input_path = luigi.Parameter()
    output_path = luigi.Parameter()
    
    def output(self):
        return luigi.LocalTarget(self.output_path)
    
    def run(self):
        # Read input data
        with open(self.input_path, 'r') as f:
            data = [float(line.strip()) for line in f]
        
        # Standardize data
        standardized_data = standardize(data)
        
        # Write output
        with self.output().open('w') as f:
            for value in standardized_data:
                f.write(f"{value}\n") 