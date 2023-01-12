from src.configs.table_schema import csv_schema, json_schema
from src.data_jobs import ingest
from src.data_jobs.transform import rename_columns, generate_annotation, correlation_transform

if __name__ == "__main__":
    csv_path = "../../data/happiness_index_data"
    json_path = "../../data/recipes_data_json"
    df = ingest.read_data(csv_path, "csv", csv_schema)
    df2 = rename_columns(df)
    df3 = correlation_transform(df2)
    df4 = generate_annotation(df3)
    df4.show()
