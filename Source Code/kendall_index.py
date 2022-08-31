from operator import index, indexOf
import numpy as np


class kendallIndex:
    def __init__(self, var1: list, var2: list):
        self.var1 = var1
        self.var2 = var2

    def normalised_kendall_tau_distance(self):
        n = len(self.var1)
        assert len(self.var2) == n, "Both lists have to be of equal length"
        i, j = np.meshgrid(np.arange(n), np.arange(n))
        a = np.argsort(self.var1)
        b = np.argsort(self.var2)
        ndisordered = np.logical_or(np.logical_and(
            a[i] < a[j], b[i] > b[j]), np.logical_and(a[i] > a[j], b[i] < b[j])).sum()
        return ndisordered / (n * (n - 1))

    # ON DEVELOPMENT

    # def parr(self, input):
    #     return self.pairs(list(input))

    # def pairs(self, input):
    #     array = input[0]
    #     result = []
    #     for i in range(len(array)-1):
    #         for j in range(len(array)):
    #             j = i+1
    #             result.append(i)
    #             result.append(j)
    #     return result

    # def union(self, a, b):
    #     x = []
    #     check = lambda e,cb: x.append(e) if e not in x else cb = None if type(cb)

    # def check(self, e, cb):
    #     if cb and type(cb) == 'function':
    #         cb = None
    #     try:
    #         indexOf(self.x, e)
    #     except ValueError:
    #         self.x.append(e)

    # def my_rank(self):
    #     r = self.var1[0]
    #     rb = self.var2[0]
    #     pairs = self.parr(self.union(r, rb))
    #     print(pairs)
    #     count = 0
    #     c = len(pairs)

    #     def indexOf(r, e):
    #         if e in r:
    #             r.index(e)
    #         if e not in r:
    #             c

    #     for j in range(c):
    #         pair = pairs[j]
    #         if indexOf(r, pair[0]) == indexOf(r, pair[1]):
    #             if indexOf(rb, pair[0]) == indexOf(rb, pair[1]):
    #                 count = count
    #             else:
    #                 count = count + 1
    #         elif indexOf(r, pair[0]) > indexOf(r, pair[1]):
    #             if indexOf(rb, pair[0]) <= indexOf(rb, pair[1]):
    #                 count = count+1
    #             else:
    #                 count = count
    #         elif indexOf(r, pair[0]) < indexOf(r, pair[1]):
    #             if indexOf(rb, pair[0]) >= indexOf(rb, pair[1]):
    #                 count = count+1
    #             else:
    #                 count = count

    #     return count
