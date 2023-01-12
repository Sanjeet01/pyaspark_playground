import unittest

from pyspark.shell import spark
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

from src.data_jobs import ingest


class TestIngest(unittest.TestCase):
    file_path = "../data/happiness_index_data/2015_world_happiness.csv"
    file_format = "csv"
    csv_schema = StructType([StructField("Country", StringType(), True),
                             StructField("Region", StringType(), True),
                             StructField("Happiness Rank", IntegerType(), True),
                             StructField("Happiness Score", DoubleType(), True),
                             StructField("Standard Error", DoubleType(), True),
                             StructField("Economy (GDP per Capita)", DoubleType(), True),
                             StructField("Family", DoubleType(), True),
                             StructField("Health (Life Expectancy)", DoubleType(), True),
                             StructField("Freedom", DoubleType(), True),
                             StructField("Trust (Government Corruption)", DoubleType(), True),
                             StructField("Generosity", DoubleType(), True),
                             StructField("Dystopia Residual", DoubleType(), True),
                             ])

    def test_read_data_csv(self, path: str = file_path, file_format: str = file_format,
                           schema: StructType = csv_schema):
        actual_df = ingest.read_data(path, file_format, schema)
        expected_df = spark.read.csv(path, header=True)
        # print(f"count of elements in df is {actual_df.count()}, {expected_df.count()} \n")
        assert actual_df.count() == expected_df.count()

    def test_csv_data_schema(self,  schema=csv_schema, path=file_path, file_format=file_format):
        actual_df = ingest.read_data(path, file_format, schema)
        assert actual_df.schema == schema

    def test_compare_data_csv(self, path=file_path, file_format=file_format, schema=csv_schema):
        actual_df = ingest.read_data(path, file_format, schema).filter(col("Happiness Rank") <= 2)
        data = [("Switzerland", "Western Europe", 1, 7.587, 0.03411, 1.39651, 1.34951, 0.94143, 0.66557, 0.41978,
                 0.29678, 2.51738),
                ("Iceland", "Western Europe", 2, 7.561, 0.04884, 1.30232, 1.40223, 0.94784, 0.62877, 0.14145,
                 0.4363, 2.70201)]
        expected_df = spark.createDataFrame(data, schema)
        assert sorted(actual_df.collect()) == sorted(expected_df.collect())

    def test_dataframe(self):
        data1 = [(171, 76), (151, 96)]
        data2 = [(172, 75), (95, 234)]
        schema = StructType([StructField("a", IntegerType(), True),
                             StructField("b", IntegerType(), True)])
        df1 = spark.createDataFrame(data1, schema)
        df2 = spark.createDataFrame(data1, schema)
        assert sorted(df1.collect()) == sorted(df2.collect())


if __name__ == '__main__':
    unittest.main()
