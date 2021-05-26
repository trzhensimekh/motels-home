import unittest
import MotelsHomeRecommendation_empty


class TestMotelHome(unittest.TestCase):

    @unittest.expectedFailure
    def test_process_data_without_input(self):
        MotelsHomeRecommendation_empty.process_data()


if __name__ == '__main__':
    unittest.main()