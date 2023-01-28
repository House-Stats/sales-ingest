import unittest
from typing import List
from pickle import loads
from confluent_kafka import Consumer

from ingest import Ingest


class testIngester(unittest.TestCase):
    def test_postcode(self):
        ingester = Ingest()
        postcodes = [
            ("CH2 1DE", ["CH2", "CH", "CH2 1"]), 
            ("CH3 7AD", ["CH3", "CH", "CH3 7"]),
            ("F12 5GK", ["F12", "F", "F12 5"]),
            ("X2 8FE", ["X2", "X", "X2 8"])]
        for postcode in postcodes:
            self.assertEqual(ingester.extract_parts(postcode[0]), postcode[1])

if __name__ == '__main__':
    unittest.main()