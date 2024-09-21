import re
from datetime import datetime
from pyspark.sql.functions import col, explode
from pyspark.sql.types import NumericType, StringType, StructType, ArrayType, StructField


class Quality:
    def __init__(self, spark_session):
        self.spark = spark_session

    def transform_schema(self, schema):
        def transform_fn(name):
            return re.sub(r'\W', '_', name.upper())

        new_fields = []
        for field in schema.fields:
            if isinstance(field.dataType, StructType):
                new_field = StructField(transform_fn(field.name),
                                        self.transform_schema(field.dataType),
                                        nullable=field.nullable)
            elif isinstance(field.dataType, ArrayType):
                new_element_type = field.dataType.elementType
                if isinstance(new_element_type, StructType):
                    new_element_type = self.transform_schema(new_element_type)
                new_field = StructField(transform_fn(field.name),
                                        ArrayType(new_element_type, containsNull=field.dataType.containsNull),
                                        nullable=field.nullable)
            else:
                new_field = StructField(transform_fn(field.name), field.dataType, nullable=field.nullable)
            new_fields.append(new_field)
        return StructType(new_fields)

    def get_data_type(self, data_type):
        if isinstance(data_type, StringType):
            return "String"
        elif isinstance(data_type, NumericType):
            return "Numeric"
        elif isinstance(data_type, StructType):
            return "Struct"
        elif isinstance(data_type, ArrayType):
            return f"Array<{self.get_data_type(data_type.elementType)}>"
        else:
            return str(data_type)

    def check_array(self, array_df, array_data_type, parent_column_name=""):
        array_info = []

        if array_df.count() > 0:
            null_ratio = array_df.where(col(parent_column_name).isNull()).count() / array_df.count()
        else:
            null_ratio = 0.0

        element_data_type = array_data_type.elementType
        element_col_name = "element"


        array_df = array_df.select(explode(parent_column_name).alias(element_col_name))

        if isinstance(element_data_type, StructType):
            nested_struct_info = self.check_struct(array_df, element_data_type, parent_column_name=element_col_name)
            array_info.extend(nested_struct_info)
        elif isinstance(element_data_type, ArrayType):
            nested_array_info = self.check_array(array_df, element_data_type, parent_column_name=element_col_name)
            array_info.extend(nested_array_info)
        else:

            column_info = {
                "Column": parent_column_name,
                "Nullability": null_ratio,
                "Type": self.get_data_type(element_data_type)
            }


            column_df = array_df.select(col(element_col_name))

            if isinstance(element_data_type, StringType):
                column_info["Length"] = column_df.selectExpr(f"MAX(LENGTH({element_col_name})) as max_length").collect()[0]["max_length"]
            elif isinstance(element_data_type, NumericType):
                column_info["Min"] = column_df.selectExpr(f"MIN({element_col_name}) as min_value").collect()[0]["min_value"]
                column_info["Max"] = column_df.selectExpr(f"MAX({element_col_name}) as max_value").collect()[0]["max_value"]
                column_info["Mean"] = column_df.selectExpr(f"AVG(CAST({element_col_name} AS DOUBLE)) as mean_value").collect()[0]["mean_value"]

            array_info.append(column_info)

        return array_info

    def check_struct(self, struct_df, struct_data_type, parent_column_name=""):
        struct_info = []

        for field in struct_data_type.fields:
            field_name = field.name

            field_name_escaped = f"`{field_name}`"


            if parent_column_name:

                full_column_name = f"{parent_column_name}.{field_name_escaped}"
            else:
                full_column_name = field_name_escaped


            if isinstance(field.dataType, StructType):
                nested_struct_info = self.check_struct(struct_df, field.dataType, parent_column_name=full_column_name)
                struct_info.extend(nested_struct_info)
            elif isinstance(field.dataType, ArrayType):
                nested_array_info = self.check_array(struct_df, field.dataType, parent_column_name=full_column_name)
                struct_info.extend(nested_array_info)
            else:

                column_info = {
                    "Column": full_column_name.strip('`'),
                    "Nullability": struct_df.where(col(full_column_name).isNull()).count() / struct_df.count(),
                    "Type": self.get_data_type(field.dataType)
                }

                column_df = struct_df.select(col(full_column_name))

                struct_info.append(column_info)

        return struct_info

    def check_quality(self, df):
        transformed_schema = self.transform_schema(df.schema)


        quality_report = []
        for field in df.schema.fields:
            field_name = field.name
            null_count = df.where(col(field_name).isNull()).count()
            total_rows = df.count()
            null_ratio = null_count / total_rows

            if isinstance(field.dataType, StructType):
                struct_info = self.check_struct(df, field.dataType, parent_column_name=field_name)
                quality_report.extend(struct_info)
            elif isinstance(field.dataType, ArrayType):
                nested_array_info = self.check_array(df, field.dataType, parent_column_name=field_name)
                quality_report.extend(nested_array_info)
            else:
                column_info = {
                    "Column": field_name,
                    "Nullability": null_ratio,
                    "Type": self.get_data_type(field.dataType)
                }

                column_df = df.select(col(field_name))
                if isinstance(field.dataType, StringType):
                    column_info["Length"] = \
                    column_df.selectExpr(f"MAX(LENGTH({field_name})) as max_length").collect()[0]["max_length"]
                elif isinstance(field.dataType, NumericType):
                    column_info["Min"] = column_df.selectExpr(f"MIN({field_name}) as min_value").collect()[0][
                        "min_value"]
                    column_info["Max"] = column_df.selectExpr(f"MAX({field_name}) as max_value").collect()[0][
                        "max_value"]
                    column_info["Mean"] = \
                    column_df.selectExpr(f"AVG(CAST({field_name} AS DOUBLE)) as mean_value").collect()[0]["mean_value"]

                quality_report.append(column_info)

        return quality_report


def check_column_name_format(rule, df):
    field_pattern = re.compile(rule["FIELD"])
    format_type = rule.get("FORMAT_TYPE", "STRING")
    format_pattern = None

    if format_type == "DATE":
        format_pattern = re.compile(r"\b(?:date|datetime)\b", re.IGNORECASE)
    elif format_type == "STRING":
        format_pattern = re.compile(r".*")

    matching_columns = []

    for col_name in df.columns:
        if re.match(field_pattern, col_name):
            if format_pattern and format_pattern.match(col_name):
                matching_columns.append((col_name, True))
            else:
                matching_columns.append((col_name, f"Does not match '{format_type}' format"))

    return matching_columns

def calculate_rule_validity(rule, df):
    field_pattern = re.compile(rule["FIELD"])
    match_pattern = re.compile(rule["MATCH"])
    format_type = rule.get("FORMAT_TYPE", "STRING")

    matching_columns = [col_name for col_name in df.columns if field_pattern.match(col_name)]

    column_validity = {}

    for col_name in matching_columns:
        col_values = df.select(col_name).rdd.flatMap(lambda x: x).collect()
        valid_count = 0
        total_count = 0

        for value in col_values:
            if isinstance(value, str) and format_type == "STRING":
                if match_pattern.match(value):
                    valid_count += 1
            elif isinstance(value, datetime) and format_type == "DATE":

                pass

            total_count += 1

        if total_count > 0:
            validity_rate = valid_count / total_count
            column_validity[col_name] = validity_rate

    return column_validity
