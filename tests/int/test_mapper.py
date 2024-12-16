import unittest
import warnings

import ray
from click.testing import CliRunner
from tiff_mapper.api import cli
from os import path


class MyTestCase(unittest.TestCase):
    def test_mapper(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            ray.init(local_mode=False)

            test_result_path = f'{path.sep.join(__file__.split(path.sep)[:-2])}/test_results'
            test_parquet_path = f'file://{test_result_path}/germany_pre_event.parquet'
            test_input = f'/Users/michaelleedahl/ogc_testbed_20/sn8/Germany_Training_Public/PRE-event'

            runner = CliRunner()

            # noinspection PyTypeChecker
            result = runner.invoke(cli, ['mapper', test_input, test_parquet_path])

            self.assertEqual(0, result.exit_code)
            print(result.output)


if __name__ == '__main__':
    unittest.main()
