import unittest
import numpy as np
import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from luigi.contrib.standardize import standardize

class TestStandardizeFunction(unittest.TestCase):
    def test_standardize_basic(self):
        """Test basic standardization functionality"""
        data = [1, 2, 3, 4, 5]
        result = standardize(data)
        
        # Verify mean is approximately 0
        mean = sum(result) / len(result)
        self.assertAlmostEqual(mean, 0, places=7)
        
        # Verify standard deviation is approximately 1
        variance = sum((x - mean) ** 2 for x in result) / len(result)
        std_dev = variance ** 0.5
        self.assertAlmostEqual(std_dev, 1, places=7)

    def test_standardize_empty(self):
        """Test handling of empty input"""
        self.assertEqual(standardize([]), [])

    def test_standardize_single_value(self):
        """Test handling of single value input"""
        self.assertEqual(standardize([42]), [0])

    def test_standardize_identical_values(self):
        """Test handling of identical values"""
        self.assertEqual(standardize([5, 5, 5]), [0, 0, 0])

    def test_standardize_negative_values(self):
        """Test handling of negative values"""
        data = [-3, -2, -1, 0, 1, 2, 3]
        result = standardize(data)
        
        # Verify mean is approximately 0
        mean = sum(result) / len(result)
        self.assertAlmostEqual(mean, 0, places=7)
        
        # Verify standard deviation is approximately 1
        variance = sum((x - mean) ** 2 for x in result) / len(result)
        std_dev = variance ** 0.5
        self.assertAlmostEqual(std_dev, 1, places=7)

    def test_standardize_large_numbers(self):
        """Test handling of large numbers"""
        data = [1000, 2000, 3000, 4000, 5000]
        result = standardize(data)
        
        # Verify mean is approximately 0
        mean = sum(result) / len(result)
        self.assertAlmostEqual(mean, 0, places=7)
        
        # Verify standard deviation is approximately 1
        variance = sum((x - mean) ** 2 for x in result) / len(result)
        std_dev = variance ** 0.5
        self.assertAlmostEqual(std_dev, 1, places=7)

    def test_standardize_float_values(self):
        """Test handling of floating point values"""
        data = [0.1, 0.2, 0.3, 0.4, 0.5]
        result = standardize(data)
        
        # Verify mean is approximately 0
        mean = sum(result) / len(result)
        self.assertAlmostEqual(mean, 0, places=7)
        
        # Verify standard deviation is approximately 1
        variance = sum((x - mean) ** 2 for x in result) / len(result)
        std_dev = variance ** 0.5
        self.assertAlmostEqual(std_dev, 1, places=7)

    def test_standardize_preserves_order(self):
        """Test that standardization preserves the order of values"""
        data = [1, 2, 3, 4, 5]
        result = standardize(data)
        
        # Verify that the order is preserved
        for i in range(len(result) - 1):
            self.assertLess(result[i], result[i + 1])

if __name__ == '__main__':
    unittest.main() 