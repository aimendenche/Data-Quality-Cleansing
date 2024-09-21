import json
from Cleans import sparkminio
from quality import Quality, check_column_name_format, calculate_rule_validity
minio_endpoint = "192.168.0.18:9000"
minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"
bucket_name = "nifi"

spark_minio = sparkminio(minio_endpoint, minio_access_key, minio_secret_key, bucket_name)


specific_file_path = "landing/s1/struct.json"
df = spark_minio.process_file(specific_file_path)

quality_checker = Quality(spark_minio.spark)
quality_report = quality_checker.check_quality(df)



for column_info in quality_report:
    print(column_info)


with open("validate.json", "r") as json_file:
    rules = json.load(json_file)["rules"]

for rule in rules:
    if rule["TYPE"] == "FORMAT":
        matching_columns = check_column_name_format(rule, df)

        for col_name, match_result in matching_columns:
            if match_result is True:
                print(f"Column: {col_name}, Match: True (Matches 'DATE' format)")
            else:
                print(f"Column: {col_name}, Match: {match_result}")

    elif rule["TYPE"] == "CONTENT":
        column_validity = calculate_rule_validity(rule, df)

        for col_name, validity_rate in column_validity.items():
            print(f"Column: {col_name}, Validity rate for rule '{rule['NAME']}': {validity_rate}")

            print(df.schema)