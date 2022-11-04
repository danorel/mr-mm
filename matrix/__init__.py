import re

from abc import ABC
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRMatrixMultiplication(MRJob, ABC):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_matrix_representation,
                reducer=self.reducer_skip
            ),
            MRStep(
                mapper=self.mapper_matrix_row_col_populate,
                reducer=self.reducer_matrix_row_col_dot_product
            )
        ]

    @staticmethod
    def mapper_matrix_representation(_, next_line):
        # Extract rows from input with specific format (https://www.wolframalpha.com/input?i=matrix+multiplication+calculator)
        num_matrix, repr_matrix = next_line.split(":")
        repr_matrix = repr_matrix[1:-1]
        repr_rows = re.findall('{(.+?)}', repr_matrix)
        # Find rows scalars in each line
        rows = []
        for repr_row in repr_rows:
            row = repr_row.split(",")
            rows.append(row)
        n_of_rows = len(rows)
        n_of_cols = len(rows[0])
        # While working with matrix:
        # 1: yield its rows
        # 2: yield its cols
        if num_matrix == "1":
            for r, row in enumerate(rows):
                yield f"{num_matrix}:{n_of_rows}:{r}", row
        else:
            for c in range(n_of_cols):
                col = [row[c] for row in rows]
                yield f"{num_matrix}:{n_of_cols}:{c}", col

    @staticmethod
    def reducer_skip(rc, vectors):
        yield rc, next(vectors)

    @staticmethod
    def mapper_matrix_row_col_populate(rc, vector):
        num_matrix, num_size, num_position = rc.split(":")
        if num_matrix == "1":
            for num_copy in range(int(num_size)):
                yield f"{num_copy}:{num_position}", vector
        else:
            for num_copy in range(int(num_size)):
                yield f"{num_position}:{num_copy}", vector

    @staticmethod
    def reducer_matrix_row_col_dot_product(rc, vectors):
        dot_product = []
        for r_scalar, c_scalar in zip(*list(vectors)):
            dot_product.append(int(r_scalar) * int(c_scalar))
        rc_scalar = sum(dot_product)
        yield rc, rc_scalar


if __name__ == '__main__':
    MRMatrixMultiplication.run()
