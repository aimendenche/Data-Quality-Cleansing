# Data Quality and Transformation Pipeline with Apache Spark and MinIO

## Project Overview

This project implements a pipeline for processing, cleaning, and validating JSON data using Apache Spark, S3 MinIO, and Python. The pipeline reads data from S3 MinIO, processes it using Spark, and runs data quality checks before storing the transformed data back in S3 MinIO. Additionally, the project provides detailed data quality reports on the processed data.

### Features:
- **Data Transformation**: Reads and transforms JSON data from MinIO.
- **Data Quality**: Performs checks on data quality, including null values, data types, and column format.
- **Metadata Management**: Updates metadata with processing details.

## Prerequisites

Before running the project, ensure you have the following tools and libraries installed:

- **Python**: >=3.8
- **Apache Spark**: >=3.3.2
- **MinIO**: >=2023
- **Pandas**: Install using `pip install pandas`
- **PySpark**: Install using `pip install pyspark`
- **MinIO Python Client**: Install using `pip install minio`

Make sure you have configured Apache Spark to include Hadoop AWS JARs for MinIO integration. You will need the following JARs:
- `hadoop-aws-3.3.2.jar`
- `aws-java-sdk-bundle-1.11.1026.jar`

### Installing Dependencies:

```bash
pip install pyspark minio pandas
```

## Project Setup

1. Clone the Repository:

```bash
git clone https://github.com/aimendenche/Data-Quality-Cleansing.git
cd Data-Quality-Cleansing
```

2. Start MinIO and Create a Bucket:

You can start your MinIO instance and create a bucket called `nifi`.

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/nifi
```

3. Update Credentials:

Update your MinIO credentials and endpoint in the scripts (`cleans.py`, `connect-spark-minio.py`) if necessary. For example:

```python
minio_endpoint = "192.168.0.18:9000"
minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"
bucket_name = "nifi"
```

## Execution

### Step 1: Clean and Transform Data

Run the script to process the data from MinIO, perform transformations, and store the result as Parquet files in MinIO:

```bash
spark-submit --jars /path/to/hadoop-aws-3.3.2.jar,/path/to/aws-java-sdk-bundle-1.11.1026.jar cleans.py
```

This script will:
- Load JSON files and metadata from MinIO.
- Transform the column names (uppercase, replace spaces with underscores).
- Write the transformed data back to MinIO in Parquet format.
- Update the metadata (e.g., record counts, application ID).

Example Data Structure in MinIO:
- Input Data: `landing/s1/test.json`
- Metadata: `landing/s1/metadata.json`
- Output Data: `staging/s1/{app_id}/*.parquet`

---

### Step 2: Connect Spark to MinIO

You can test Spark's connection to MinIO by running this script:

```bash
python connect-spark-minio.py
```

This script reads data from MinIO and prints it as a DataFrame.

Example Output:

```bash
+------------+-----------+--------------+
|column1     |column2     |column3       |
+------------+-----------+--------------+
|value1      |value2      |value3        |
+------------+-----------+--------------+
```

---

### Step 3: Run Data Quality Checks

To check the quality of the processed data, run the quality checking script:

```bash
python quality.py
```

This will produce a detailed report showing nullability, data types, and any schema validation issues.

Example Results:

```bash
{'Column': 'ACTIVITE_PRINCIPALE', 'Nullability': 0.0, 'Type': 'String', 'Length': 6}
{'Column': 'ANNEE_CATEGORIE_ENTREPRISE', 'Nullability': 0.0, 'Type': 'String', 'Length': 4}
{'Column': 'ANNEE_TRANCHE_EFFECTIF_SALARIE', 'Nullability': 0.1406, 'Type': 'String', 'Length': 4}
{'Column': 'COMPLEMENTS.collectivite_territoriale.code', 'Nullability': 0.7843, 'Type': 'String'}
...
```

You will see the schema of the processed data, nullability reports for each field, and any specific validation failures, such as incorrect data formats.

### Schema Example:

Here is a part of the schema that is checked:

```bash
root
 |-- ACTIVITE_PRINCIPALE: string (nullable = true)
 |-- ANNEE_CATEGORIE_ENTREPRISE: string (nullable = true)
 |-- COMPLEMENTS: struct (nullable = true)
 |    |-- collectivite_territoriale: struct (nullable = true)
 |    |    |-- code: string (nullable = true)
 |    |    |-- code_insee: string (nullable = true)
 |    |    |-- elus: array (nullable = true)
 |    |-- convention_collective_renseignee: boolean (nullable = true)
...
```

---

### Validation Rule Example:

If you have rules defined in `validate.json`, such as date validation, they will be applied to ensure the data matches the expected format.

Example Rule in `validate.json`:
```json
{
  "TYPE": "FORMAT",
  "FIELD": "DATE_CREATION",
  "FORMAT_TYPE": "DATE",
  "MATCH": "\d{4}-\d{2}-\d{2}"
}
```

Example Validation Result:
```bash
Rule: DATE_VALIDATION
Field: DATE_CREATION
Validation Result: Failed
```

---

### Example Output:

```bash
+-------------------------+-----------+---------+
| Column                  | Nullability | Type    |
+-------------------------+-----------+---------+
| ACTIVITE_PRINCIPALE      | 0.0        | String  |
| ANNEE_CATEGORIE_ENTREPRISE | 0.0      | String  |
| ANNEE_TRANCHE_EFFECTIF_SALARIE | 0.1406 | String|
...
```

---

## Uploading the Project to GitHub

1. Initialize the GitHub repository:
   ```bash
   git init
   git add .
   git commit -m "Initial commit"
   ```

2. Push to your GitHub repository:
   ```bash
   git remote add origin https://github.com/aimendenche/Data-Quality-Cleansing.git
   git push -u origin main
   ```

---

## Conclusion

This project provides a complete pipeline for data transformation and quality validation using Apache Spark and MinIO. You can expand upon this by adding more quality checks, transforming more complex data types, or integrating with other cloud storage systems.

If you have any issues or questions, feel free to open an issue on the GitHub repository.
