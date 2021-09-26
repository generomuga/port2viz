from main import *
import unittest
import numpy as np

class TestMain(unittest.TestCase):

    def test_set_locode_url(self):
        self.assertEqual(set_locode_url(
            base_url='https://service.unece.org/trade/locode/',
            country_code='af',
            extension='.htm'
        ), 'https://service.unece.org/trade/locode/af.htm')

    def test_is_failed_mapping(self):
        self.assertEqual(is_failed_mapping(
            function='1----',
            country_name='Philippines',
            port_name='Surigao',
            coordinates='1400N 23000E'
        ), 0)

    def test_convert_lat_lon(self):
        self.assertEqual(convert_lat_lon('3945N 02001E'), (39.45, 20.01))

    def test_get_formatted_addr(self):
        self.assertEqual(get_formatted_addr(39.45,20.01), 'Nomos Kerkyras')
    