import unittest

class MyTestCase(unittest.TestCase):

    def test_something(self):
        from dbacademy_gems import dbgems

        self.assertEqual(dbgems.sc, "missing")
        self.assertEqual(dbgems.spark, "missing")
        self.assertEqual(dbgems.dbutils, "missing")

        self.assertIsNotNone(dbgems.dbgems_module)

        self.assertEqual(dbgems.dbgems_module.sc, "missing")
        self.assertEqual(dbgems.dbgems_module.spark, "missing")
        self.assertEqual(dbgems.dbgems_module.dbutils, "missing")

        self.assertIs(dbgems.dbgems_module.sc, dbgems.sc)
        self.assertIs(dbgems.dbgems_module.spark, dbgems.spark)
        self.assertIs(dbgems.dbgems_module.dbutils, dbgems.dbutils)


if __name__ == '__main__':
    unittest.main()
