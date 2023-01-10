"""
# etl_config for project pyspark_playground
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

aa_data_job = {
    "paths": {
        "input_data_path": "data/source_data",
        "output_data_path": "my/dummy/path",
        "landing_path": "data/source_data/aa_data/api_landing_data",
        "superman_path": "data/source_data/aa_data/superman"
    },
    "job_parameter": {"aa_data_job": {"steps_per_floor": 21}},
}

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
                         ]),

json_schema = StructType([StructField('cookTime', StringType(), True),
                          StructField('datePublished', StringType(), True),
                          StructField('description', StringType(), True),
                          StructField('image', StringType(), True),
                          StructField('ingredients', StringType(), True),
                          StructField('name', StringType(), True),
                          StructField('prepTime', StringType(), True),
                          StructField('recipeYield', StringType(), True),
                          StructField('url', StringType(), True)
                          ])


