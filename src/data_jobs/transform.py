from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, lower, corr, when


def filter_data(df: DataFrame, number: int):
    return df.filter(col("Happiness Rank") <= number)


def rename_columns(df: DataFrame):
    """
    This function is intended to change the column name
    :param df:      Input Data Frame
    :return:        returns a dataframe after column name changed
    """
    # TODO
    # Fix this function to change the column and not the data.

    new_columns = [lower(regexp_replace(col(column), " ", "_")).alias(column) for column in df.columns]
    df = df.select(*new_columns)
    return df


def correlation_transform(df: DataFrame):
    return df.withColumn(col("c_happiness_life_exp"), corr(col("Happiness Rank"), col("Health (Life Expectancy)")))\
        .withColumn(col("c_happiness_generosity"), corr(col("Happiness Rank"), col("Generosity")))\
        .withColumn(col("c_happiness_corruption"), corr(col("Happiness Rank"), col("Trust (Government Corruption)")))\
        .withColumn(col("c_happiness_freedom"), corr(col("Happiness Rank"), col("Freedom")))\
        .withColumn(col("c_happiness_economy"), corr(col("Happiness Rank"), col("Economy (GDP per Capita)")))


def generate_annotation(df: DataFrame):
    return df.withColumn("annotations", when(col("Happiness Rank") <= 5, "Doing Well")
                        .when(col("Happiness Rank") <= 10, "Still Good")
                        .when(col("Happiness Rank") <= 50, "Could be Better")
                        .otherwise("What are you Doing"))




