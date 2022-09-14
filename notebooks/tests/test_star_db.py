# Databricks notebook source
#%run ../create_star_schema

# COMMAND ----------

import unittest
import pyspark
class TestStarDbNotebook(unittest.TestCase):
    def test_success(self):
        spark.table('star.dimcustomers')
        
    def test_fail(self):
        spark.table('star.unknown')

# COMMAND ----------

def run_tests():
    test_classes_to_run = [TestStarDbNotebook]
    loader = unittest.TestLoader()
    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)
        
    all_suite = unittest.TestSuite(suites_list)
    runner = unittest.TextTestRunner()
    runner.run(all_suite)
    
run_tests()

# COMMAND ----------


