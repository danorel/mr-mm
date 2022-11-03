from abc import ABC

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRVectorScalarMultiplication(MRJob, ABC):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_pair_representation,
                reducer=self.reducer_pair_dot_product
            ),
            MRStep(
                mapper=self.mapper_vector_convolution,
                reducer=self.reducer_vector_dot_product
            ),
        ]

    @staticmethod
    def mapper_pair_representation(_, next_line):
        raw_vector = next_line.split(",")
        for position, raw_scalar in enumerate(raw_vector):
            yield position, raw_scalar

    @staticmethod
    def reducer_pair_dot_product(pos, pair_scalars):
        pair_dot_product = 1
        for scalar in pair_scalars:
            pair_dot_product *= float(scalar)
        yield pos, pair_dot_product

    @staticmethod
    def mapper_vector_convolution(_, pair_dot_product):
        yield "vector", pair_dot_product

    @staticmethod
    def reducer_vector_dot_product(vector, pair_dot_products):
        yield vector, sum(pair_dot_products)


if __name__ == '__main__':
    MRVectorScalarMultiplication.run()
