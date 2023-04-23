"""
test_etl_job.py
~~~~~~~~~~~~~~~
Este módulo contém testes de unidade para as etapas de transformação do ETL
trabalho definido em etl_job.py. 
"""
import unittest

import json

from pyspark.sql.functions import mean

from dependencies.spark import start_spark
from jobs.etl_job import transform_data


class SparkETLTests(unittest.TestCase):
    """TConjunto de teste para o recurso de transformação de dados do etl_job.py
    """

    def setUp(self):
        """Inicia o Spark, defina a configuração e o caminho para testar os dados
        """
        self.config = json.loads("""{"steps_per_floor": 21}""")
        self.spark, *_ = start_spark()
        self.test_data_path = 'tests/test_data/'

    def tearDown(self):
        """Para o Spark
        """
        self.spark.stop()

    def test_transform_data(self):
        """Test data transformer.
        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        Usando pequenos porções de dados de entrada e dados de saída esperados, nós
        teste a etapa de transformação para garantir que está funcionando como
        esperado.
        """
        # assemble
        input_data = (
            self.spark
            .read
            .parquet(self.test_data_path + 'employees'))

        expected_data = (
            self.spark
            .read
            .parquet(self.test_data_path + 'employees_report'))

        expected_cols = len(expected_data.columns)
        expected_rows = expected_data.count()
        expected_avg_steps = (
            expected_data
            .agg(mean('steps_to_desk').alias('avg_steps_to_desk'))
            .collect()[0]
            ['avg_steps_to_desk'])

        # act
        data_transformed = transform_data(input_data, 21)

        cols = len(expected_data.columns)
        rows = expected_data.count()
        avg_steps = (
            expected_data
            .agg(mean('steps_to_desk').alias('avg_steps_to_desk'))
            .collect()[0]
            ['avg_steps_to_desk'])

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertEqual(expected_avg_steps, avg_steps)
        self.assertTrue([col in expected_data.columns
                         for col in data_transformed.columns])


if __name__ == '__main__':
    unittest.main()