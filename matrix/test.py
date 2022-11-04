import unittest

from __init__ import MRMatrixMultiplication


class TestMRMatrixMultiplication(unittest.TestCase):

    def test_mapper_matrix_representation(self):
        c = MRMatrixMultiplication()
        matrix_representation = c.mapper_matrix_representation(
            None,
            "1:{{1,0,0,0,0},{0,1,0,0,0},{0,0,1,0,0},{0,0,0,1,0},{0,0,0,0,1}}"
        )
        it = iter(matrix_representation)
        for rc, column in it:
            self.assertTrue(rc, True)


if __name__ == '__main__':
    unittest.main()
