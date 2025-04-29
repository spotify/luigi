from typing import List

def standardize(data: List[float]) -> List[float]:
    """
    Standardizes a list of numerical data to have mean 0 and standard deviation 1.
    """
    if not data:
        return []
    mean = sum(data) / len(data)
    std = (sum((x - mean) ** 2 for x in data) / len(data)) ** 0.5
    if std == 0:
        return [0 for _ in data]
    return [(x - mean) / std for x in data] 