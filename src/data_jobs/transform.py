from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, lower, corr, when, lit


def filter_data(df: DataFrame, number: int):
    return df.filter(col("Happiness Rank") <= number)


def rename_columns(df: DataFrame):
    """
    This function is intended to change the column name
    :param df:      Input Data Frame
    :return:        returns a dataframe after column name changed
    """
    for name in df.schema.names:
        df = df.withColumnRenamed(name, (name.replace(' ', '_').replace('(', '').replace(')', '').lower()))
    return df


def correlation_transform(df: DataFrame):
    return df.withColumn("c_happiness_life_exp", lit(round(df.corr("happiness_rank", "health_life_expectancy"), 2)))\
        .withColumn("c_happiness_generosity", lit(round(df.corr("happiness_rank", "generosity"), 2)))\
        .withColumn("c_happiness_corruption", lit(round(df.corr("happiness_rank", "trust_government_corruption"),2)))\
        .withColumn("c_happiness_freedom", lit(round(df.corr("happiness_rank", "freedom"), 2)))\
        .withColumn("c_happiness_economy", lit(round(df.corr("happiness_rank", "economy_gdp_per_capita"), 2)))


def generate_annotation(df: DataFrame):
    return df.withColumn("annotations", when(col("happiness_rank") <= 5, "Doing Well")
                         .when(col("happiness_rank") <= 10, "Still Good")
                         .when(col("happiness_rank") <= 50, "Could be Better")
                         .otherwise("What are you Doing"))




