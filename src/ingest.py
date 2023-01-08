import pyspark
from pyspark.shell import spark
from pyspark.sql import DataFrame


def read_data(path: str, file_format: str) -> DataFrame:
    """
    This function reads data stored in flat files in either json or csv format and return a data frame
    :param path:            Path of the file
    :param file_format:     Format of the data file (csv, json)
    :return:                Data frame that is returned by reading data file
    """
    df: DataFrame = None
    if file_format == 'csv':
        df = spark.read.csv(path, header=True, inferSchema=True)
    elif file_format == 'json':
        df = spark.read.json(path, inferSchema="true", header="true")
    return df


# if __name__ == "__main__":
#     df = read_data("../data/happiness_index_data/2015_world_happiness.csv", "csv")
#     df.show()

