# script which provides tests for arrayexpress_files.py

import unittest
import sys
import os
import json

sys.path.insert(0, '../src')

# import functions to be tested
from arrayexpress_files import new_accessions

class Test_arrayexpress_files(unittest.TestCase):
    """Test the functions from the arrayexpress_files script."""

    def test_new_accessions_1(self):
        """Is the list of new accessions correctly generated for one json file?"""
        
        # reference accessions
        ref_acc = ['E-GEOD-42314', 'E-GEOD-51468', 'E-GEOD-55234', 'E-GEOD-50778', 'E-GEOD-30583']

        # generate list of accessions with function to be tested
        test_acc = new_accessions()

        self.assertEqual(ref_acc, test_acc)

    def test_new_accessions_2(self):
        """Is the list of new accessions correctly generated for two json files?"""

        # go to directory containing reference json files
        os.chdir('test_new_accessions_2')

        # reference accessions
        ref_acc = ['E-GEOD-42314']

        # generate list of new accessions with function to be tested
        test_acc = new_accessions()

        # go back to original directory
        os.chdir('..')

        self.assertEqual(ref_acc, test_acc)

if __name__ == '__main__':
    unittest.main()
