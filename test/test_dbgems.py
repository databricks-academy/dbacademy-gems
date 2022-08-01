import unittest


class MyTestCase(unittest.TestCase):
    pass

    # def test_get_parameter(self):
    #     from dbgems import get_parameter
    #
    #     param = get_parameter("whatever", "missing")
    #     self.assertEqual("moo", param)

    # def test_get_current_spark_version(self):
    #     from dbgems import get_current_spark_version
    #     version = get_current_spark_version()
    #     self.assertEqual("moo", version)


if __name__ == '__main__':
    unittest.main()
