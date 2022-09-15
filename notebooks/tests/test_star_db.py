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

from unittest import TextTestRunner, TextTestResult
import sys

class CustomRunner(TextTestRunner):
    def __init__(self):
        super().__init__()
        self._myresult = None
   
    def _makeResult(self):
        self._myresult = unittest.TestResult()
        return self._myresult
    
    def getResult(self):
        return self._myresult
    
    
    
def run_tests():
    test_classes_to_run = [TestStarDbNotebook]
    loader = unittest.TestLoader()
    suites_list = []
    for test_class in test_classes_to_run:
        suite = loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)
        
    all_suite = unittest.TestSuite(suites_list)
    runner = CustomRunner()
    runner.run(all_suite)
    dbutils.notebook.exit(len(runner.getResult().errors) + len(runner.getResult().failures))
    
run_tests()
