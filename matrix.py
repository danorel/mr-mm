from abc import ABC

from mrjob.job import MRJob


class MRMatrixMultiplication(MRJob, ABC):
    def mapper(self, _, line):
        
        yield "lines", 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRMatrixMultiplication.run()
