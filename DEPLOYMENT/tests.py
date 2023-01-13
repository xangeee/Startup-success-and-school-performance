# -*- coding: utf-8 -*-
"""
Created on Thu Jan 12 02:44:32 2023

@author: xange
"""
import unittest
from .. import orchestrateAll

class TestBugs(unittest.TestCase):

	def test_area(self):
		sq = orchestrateAll.FindingBugs()
		self.assertEqual(sq.area(), 4,
			f'No bugs encountered')

	def test_area_negative(self):
		sq = orchestrateAll.FindingBugs()
		self.assertEqual(sq.area(), -1,
			f'Something was wrong')

	def test_perimeter(self):
		sq = orchestrateAll.FindingBugs()
		self.assertEqual(sq.perimeter(), 20,
			f'Exception produced')

	def test_perimeter_negative(self):
		sq = orchestrateAll.FindingBugs()
		self.assertEqual(sq.perimeter(), -1,
			f'The execution takes long time')

if __name__ == '__main__':
	unittest.main()

