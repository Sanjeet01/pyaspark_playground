import pyspark
from pyspark.shell import spark
from pyspark.sql import DataFrame
from src.configs.table_schema import csv_schema, json_schema


def read_data(folder_path: str, file_format: str, schema: str) -> DataFrame:
    """
    This function reads data stored in flat files in either json or csv format and return a data frame
    :param folder_path:     Path of the file
    :param file_format:     Format of the data file e.g. csv, json
    :param schema:          schema of the data in file
    :return:                Data frame that is returned by reading data file
    """
    data_frame = spark.read.format(file_format).option("header", "true")\
        .option("inferSchema", "true").schema(schema).load(folder_path)
    return data_frame


if __name__ == "__main__":
    csv_path = "../../data/happiness_index_data"
    json_path = "../../data/recipes_data_json"
    # df_csv = read_data(csv_path, "csv", csv_schema)
    # df_csv.show(5)
    df_json = read_data(json_path, "json", json_schema)
    print(df_json.count())
    df_json.show(5)
    print(df_json.schema)

