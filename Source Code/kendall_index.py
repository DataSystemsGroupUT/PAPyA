import numpy as np

class kendallIndex:
    def __init__(self, var1, var2):
        self.var1 = var1
        self.var2 = var2
    
    def normalised_kendall_tau_distance(self):
        n = len(self.var1)
        assert len(self.var2) == n, "Both lists have to be of equal length"
        i, j = np.meshgrid(np.arange(n), np.arange(n))
        a = np.argsort(self.var1)
        b = np.argsort(self.var2)
        ndisordered = np.logical_or(np.logical_and(a[i] < a[j], b[i] > b[j]), np.logical_and(a[i] > a[j], b[i] < b[j])).sum()
        return ndisordered / (n * (n - 1)) 