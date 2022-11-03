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
        for index, value in enumerate(values):
            yield index, value

    @staticmethod
    def reducer_pair_dot_product(index, values):
        pair_dot_product = 1
        for value in values:
            pair_dot_product *= float(value)
        yield index, pair_dot_product

    @staticmethod
    def mapper_vector_representation(_, pair_dot_product):
        yield "result", pair_dot_product

    @staticmethod
    def reducer_vector_dot_product(result, pair_dot_product):
        yield result, sum(pair_dot_product)


if __name__ == '__main__':
    MRVectorScalarMultiplication.run()
