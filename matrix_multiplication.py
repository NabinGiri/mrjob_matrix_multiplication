"""program to calculate product of two matrix using mrjob"""

"""The input format of matrix used here is as below:
i j value_u
i j+1 value_u+1
i+1 j value_u+2
i+1 j+1 value_u+3

sample matrix input format:
0 0 1
0 1 2
0 2 3
1 0 4
1 1 5
"""

from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env
from mrjob.step import MRStep


class matrix_multiply(MRJob):

    def configure_args(self):
        super(matrix_multiply, self).configure_args()
        self.add_passthru_arg("--A-matrix", default="A", dest="Matrix_A")

    def decide_matrix(self):
        """returns 1 if matrix is A else returns 2 for matrix B"""
        filename = jobconf_from_env("map.input.file")
        if self.options.Matrix_A in filename:
            return 1
        else:
            return 2

    def mapping_values(self, _, line):
        matrix = self.decide_matrix()
        a, b, v = line.split()
        v = float(v)
        if matrix == 1:
            i = int(a)
            j = int(b)
            yield j, (0, i, v)
        else:
            j = int(a)
            k = int(b)
            yield j, (1, k, v)

    def multiply_values(self, j, values):
        values_of_matrix_A = []
        values_of_matrix_B = []
        for val in values:
            if val[0] == 0:
                values_of_matrix_A.append(val)
            elif val[0] == 1:
                values_of_matrix_B.append(val)

        for (m, i, val1) in values_of_matrix_A:
            for (m, k, val2) in values_of_matrix_B:
                yield (i, k), val1 * val2

    def name(self, k, v):
        yield k, v

    def sum_of_values(self, k, values):
        yield k, sum(values)

    def steps(self):
        return [MRStep(mapper=self.mapping_values, reducer=self.multiply_values),
                MRStep(mapper=self.name, reducer=self.sum_of_values)]


if __name__ == "__main__":
    matrix_multiply.run()
