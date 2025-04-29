.. _luigi-contrib-standardize:

StandardizeData
---------------
.. autoclass:: luigi.contrib.standardize.StandardizeData
    :members:
    :noindex:

The ``StandardizeData`` task standardizes numerical data from an input file to have a mean of 0 and standard deviation of 1. This is useful for normalizing data before machine learning or statistical analysis.

Parameters:
    - ``input_path``: Path to the input file containing numerical data (one value per line)
    - ``output_path``: Path where the standardized data will be saved

Example usage:
    .. code-block:: python

        from luigi.contrib.standardize import StandardizeData

        task = StandardizeData(
            input_path="raw_data.txt",
            output_path="standardized_data.txt"
        )

The task will:
1. Read numerical values from the input file
2. Standardize the values to have mean 0 and standard deviation 1
3. Write the standardized values to the output file

The standardization is performed using the formula:
    .. math::
        z = \frac{x - \mu}{\sigma}

where:
    - :math:`z` is the standardized value
    - :math:`x` is the original value
    - :math:`\mu` is the mean of the data
    - :math:`\sigma` is the standard deviation of the data

Special cases:
    - Empty input files will produce empty output files
    - Single value inputs will be standardized to 0
    - Identical values will all be standardized to 0 