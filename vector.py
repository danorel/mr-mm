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
                mapper=self.mapper_vector_representation,
                reducer=self.reducer_vector_dot_product
            ),
        ]

    @staticmethod
    def mapper_pair_representation(_, line):
        values = line.split(",")
        for i, value in enumerate(values):
            yield i, value

    @staticmethod
    def reducer_pair_dot_product(key, values):
        pair_dot_product = 1
        for value in values:
            pair_dot_product *= float(value)
        yield key, pair_dot_product

    @staticmethod
    def mapper_vector_representation(_, pair_dot_product):
        yield "result", pair_dot_product

    @staticmethod
    def reducer_vector_dot_product(key, pair_dot_product):
        yield key, sum(pair_dot_product)


if __name__ == '__main__':
    MRVectorScalarMultiplication.run()
