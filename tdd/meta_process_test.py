import unittest
import os
import sys
from meta.run.meta_process import *


BASE_DIR = os.path.join('meta', 'tdd', 'meta_process')


class test_ProcessTest(unittest.TestCase):
    def test_get_data(self):
        filepath = os.path.join(BASE_DIR, 'long_field.csv')
        data = get_data(filepath)
        field_size = sys.getsizeof(data[0]['author'])
        self.assertEqual(field_size, 137622)


if __name__ == '__main__':
    unittest.main()