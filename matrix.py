from abc import ABC

from mrjob.job import MRJob
from mrjob.step import MRStep


class MRMatrixMultiplication(MRJob, ABC):
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_matrix_row_col_split,
                reducer=self.reducer_matrix_row_col_filter
            ),
            MRStep(
                mapper=self.mapper_matrix_row_col_join,
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
    def mapper_matrix_row_col_split(_, line):
        number, representation = line.split(":")
        rows = representation.split("|")
        for i, row in enumerate(rows):
            yield f"matrix-{number}:row-{i}", row.split(",")
        for j in range(len(rows)):
            column = [row.split(",")[j] for row in rows]
            yield f"matrix-{number}:col-{j}", column

    @staticmethod
    def reducer_matrix_row_col_filter(position, values):
        if "matrix-1:row" in position or "matrix-2:col" in position:
            yield position, next(values)

    @staticmethod
    def mapper_matrix_row_col_join(position, values):
        if "matrix-2:col" in position:
            index_is_column = position[13]
            for index_for_row in range(len(values)):
                yield f"row-{index_for_row}:column-{index_is_column}", values
        else:
            index_is_row = position[13]
            for index_for_col in range(len(values)):
                yield f"row-{index_is_row}:column-{index_for_col}", values

    @staticmethod
    def reducer_matrix_row_col_dot_product(location, values):
        row = next(values)
        column = next(values)
        dot_product = []
        for r_value, c_value in zip(row, column):
            dot_product.append(int(r_value) * int(c_value))
        yield location, sum(dot_product)

    @staticmethod
    def mapper_row_convolution(position, value):
        row_number = position[4]
        row_column_value = str(value)
        yield row_number, row_column_value

    @staticmethod
    def reducer_row_convolution(row_number, row_column_values):
        row_column_values = ','.join(row_column_values)
        yield row_number, row_column_values

    @staticmethod
    def mapper_matrix_convolution(_, row_values):
        yield "matrix", row_values

    @staticmethod
    def reducer_matrix_convolution(matrix, row_values):
        yield matrix, '|'.join(row_values)


if __name__ == '__main__':
    MRMatrixMultiplication.run()
