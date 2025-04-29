import unittest
import os
import tempfile
import luigi
from luigi.contrib.standardize import StandardizeData
from luigi.local_target import LocalTarget

class TestStandardizeData(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.input_path = os.path.join(self.temp_dir, 'input_data.txt')
        self.output_path = os.path.join(self.temp_dir, 'standardized_data.txt')

    def tearDown(self):
        """Clean up test fixtures"""
        if os.path.exists(self.input_path):
            os.remove(self.input_path)
        if os.path.exists(self.output_path):
            os.remove(self.output_path)
        os.rmdir(self.temp_dir)

    def test_standardize_task(self):
        """Test basic standardization functionality"""
        # Create test input with known values
        with open(self.input_path, 'w') as f:
            f.write("1\n2\n3\n4\n5\n")
        
        # Run task
        task = StandardizeData(input_path=self.input_path, output_path=self.output_path)
        luigi.build([task], local_scheduler=True)
        
        # Verify output
        with open(self.output_path, 'r') as f:
            result = [float(line.strip()) for line in f]
            
        # Print debug information
        print("\nStandardized values:", result)
        print("Min value:", min(result))
        print("Max value:", max(result))
            
        # Check basic properties of standardized data
        self.assertEqual(len(result), 5)  # Same number of values
        self.assertTrue(all(-1.5 <= x <= 1.5 for x in result))  # Expected standardized range
        
        # Verify mean is approximately 0
        mean = sum(result) / len(result)
        print("Mean:", mean)
        self.assertAlmostEqual(mean, 0, places=6)
        
        # Verify standard deviation is approximately 1
        variance = sum((x - mean) ** 2 for x in result) / len(result)
        std_dev = variance ** 0.5
        print("Standard deviation:", std_dev)
        self.assertAlmostEqual(std_dev, 1, places=6)

    def test_empty_input(self):
        """Test handling of empty input file"""
        # Create empty input file
        with open(self.input_path, 'w') as f:
            pass
        
        # Run task
        task = StandardizeData(input_path=self.input_path, output_path=self.output_path)
        luigi.build([task], local_scheduler=True)
        
        # Verify output is empty
        with open(self.output_path, 'r') as f:
            result = f.readlines()
        self.assertEqual(len(result), 0)

    def test_single_value(self):
        """Test handling of single value input"""
        # Create input with single value
        with open(self.input_path, 'w') as f:
            f.write("42\n")
        
        # Run task
        task = StandardizeData(input_path=self.input_path, output_path=self.output_path)
        luigi.build([task], local_scheduler=True)
        
        # Verify output is zero (since std dev would be 0)
        with open(self.output_path, 'r') as f:
            result = float(f.readline().strip())
        self.assertEqual(result, 0)

    def test_identical_values(self):
        """Test handling of identical values"""
        # Create input with identical values
        with open(self.input_path, 'w') as f:
            f.write("10\n10\n10\n10\n10\n")
        
        # Run task
        task = StandardizeData(input_path=self.input_path, output_path=self.output_path)
        luigi.build([task], local_scheduler=True)
        
        # Verify all outputs are zero (since std dev would be 0)
        with open(self.output_path, 'r') as f:
            result = [float(line.strip()) for line in f]
        self.assertTrue(all(x == 0 for x in result))

if __name__ == '__main__':
    unittest.main() 