from abc import ABC

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRMatrixMultiplication(MRJob, ABC):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_matrix_representation,
                reducer=self.reducer_matrix_row_col_dot_product
            ),
            MRStep(
                mapper=self.mapper_row_convolution,
                reducer=self.reducer_row_convolution
            ),
            MRStep(
                mapper=self.mapper_matrix_convolution,
                reducer=self.reducer_matrix_convolution
            )
        ]

    @staticmethod
    def mapper_matrix_representation(_, next_line):
        raw_number, raw_repr = next_line.split(":")
        raw_rows = raw_repr.split("|")
        if raw_number == "1":
            for r, raw_row in enumerate(raw_rows):
                row = raw_row.split(",")
                for c in range(len(row)):
                    yield f"{r}:{c}", row
        else:
            for c in range(len(raw_rows)):
                column = [raw_row.split(",")[c] for raw_row in raw_rows]
                for r in range(len(column)):
                    yield f"{r}:{c}", column

    @staticmethod
    def reducer_matrix_row_col_dot_product(rc, vectors):
        r_vector = next(vectors)
        c_vector = next(vectors)
        dot_product = []
        for r_scalar, c_scalar in zip(r_vector, c_vector):
            dot_product.append(int(r_scalar) * int(c_scalar))
        rc_scalar = sum(dot_product)
        yield rc, rc_scalar

    @staticmethod
    def mapper_row_convolution(rc, scalar):
        yield rc[0], str(scalar)

    @staticmethod
    def reducer_row_convolution(r, scalars):
        yield r, ','.join(scalars)

    @staticmethod
    def mapper_matrix_convolution(_, r_scalars):
        yield "matrix", r_scalars

    @staticmethod
    def reducer_matrix_convolution(matrix, r_scalars):
        yield matrix, '|'.join(r_scalars)


if __name__ == '__main__':
    MRMatrixMultiplication.run()
