import unittest
from meta.scripts.cleaner import * 

class test_cleaner(unittest.TestCase):
    def test_clen_hyphen(self):
        broken_strings = ['100­101', '100−101', '100–101', '100–101', '100—101', '100⁃101', '100−101']
        fixed_strings = list()
        for string in broken_strings:
            fixed_string = clean_hyphen(string)
            fixed_strings.append(fixed_string)
        expected_output = ['100-101', '100-101', '100-101', '100-101', '100-101', '100-101', '100-101']
        self.assertEqual(fixed_strings, expected_output)


if __name__ == '__main__':
    unittest.main()